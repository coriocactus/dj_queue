import logging
import multiprocessing
import os
import time
from collections import Counter

import pytest

from dj_queue.models import ClaimedExecution, ReadyExecution

logger = logging.getLogger("dj_queue.stress")


pytestmark = [
  pytest.mark.django_db(transaction=True),
  pytest.mark.postgres,
  pytest.mark.skipif(
    os.environ.get("STRESS") != "1",
    reason="run separately with STRESS=1",
  ),
]


def _queue_tasks(database_alias="default"):
  return {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {
        "mode": "fork",
        "database_alias": database_alias,
        "workers": [{"queues": "*", "threads": 4, "processes": 4, "polling_interval": 0.01}],
        "dispatchers": [],
        "scheduler": None,
        "process_heartbeat_interval": 0,
        "process_alive_threshold": 5,
        "preserve_finished_jobs": True,
        "clear_finished_jobs_after": None,
        "listen_notify": False,
      },
    }
  }


def _terminal_job_count():
  from django.db.models import Q

  from dj_queue.models import Job

  return Job.objects.filter(
    Q(finished_at__isnull=False) | Q(failed_execution__isnull=False)
  ).count()


def _run_drain_runner(runner_class_path, kwargs, task_count, databases, tasks_settings):
  import django
  from django.apps import apps
  from django.conf import settings

  settings.DATABASES = databases
  settings.TASKS = tasks_settings

  if apps.ready is False:
    django.setup()

  from django.db import connections
  from django.utils.module_loading import import_string

  from dj_queue.models import ClaimedExecution, ReadyExecution

  connections.close_all()
  runner_class = import_string(runner_class_path)
  runner = runner_class(**kwargs)
  try:
    runner.start()
    while True:
      runner.poll_once()
      if (
        _terminal_job_count() == task_count
        and ReadyExecution.objects.exists() is False
        and ClaimedExecution.objects.exists() is False
      ):
        break
      time.sleep(runner.polling_interval)
  finally:
    runner.stop(timeout=5)
    connections.close_all()


def _join_children(children, timeout):
  deadline = time.monotonic() + timeout
  while time.monotonic() < deadline:
    if all(child.is_alive() is False for child in children):
      return
    if children:
      time.sleep(0.01)
  assert all(child.is_alive() is False for child in children)


def _wait_for_fork_drain(task_count, *, timeout, children_by_pid):
  deadline = time.monotonic() + timeout
  next_log_at = time.monotonic()
  while time.monotonic() < deadline:
    finished = _terminal_job_count()
    ready = ReadyExecution.objects.count()
    claimed = ClaimedExecution.objects.count()
    if finished == task_count and ready == 0 and claimed == 0:
      logger.info(
        "fork volume complete finished=%s/%s ready=%s claimed=%s live_children=%s",
        finished,
        task_count,
        ready,
        claimed,
        sum(child.is_alive() for child in children_by_pid.values()),
      )
      return
    now = time.monotonic()
    if now >= next_log_at:
      logger.info(
        "fork volume progress finished=%s/%s ready=%s claimed=%s live_children=%s",
        finished,
        task_count,
        ready,
        claimed,
        sum(child.is_alive() for child in children_by_pid.values()),
      )
      next_log_at = now + 1
    time.sleep(0.1)
  assert (
    _terminal_job_count() == task_count
    and ReadyExecution.objects.count() == 0
    and ClaimedExecution.objects.count() == 0
  )


def _terminate_children(children):
  for child in children:
    if child.is_alive():
      child.terminate()
  _join_children(children, timeout=5)


def test_10k_tasks_fork_mode_no_duplicates(tmp_path, queue_test_settings):
  from django.conf import settings
  from django.tasks import TaskResultStatus
  from django.utils import timezone

  from dj_queue.models import FailedExecution, Job, Process
  from dj_queue.runtime.supervisor import ForkSupervisor
  from tests.tasks import record_once

  task_count = 10_000
  tasks_settings = _queue_tasks(database_alias="default")
  queue_test_settings(tasks=tasks_settings)
  logger.info("fork volume start task_count=%s", task_count)
  databases = settings.DATABASES
  output_dir = tmp_path / "fork-records"
  output_dir.mkdir()

  payloads = [f"task-{index}" for index in range(task_count)]
  now = timezone.now()
  jobs = [
    Job(
      task_path=record_once.module_path,
      queue_name=record_once.queue_name,
      priority=record_once.priority,
      payload={"args": [str(output_dir), value], "kwargs": {}},
      backend_alias="default",
      created_at=now,
      updated_at=now,
    )
    for value in payloads
  ]
  Job.objects.bulk_create(jobs, batch_size=1000)
  ReadyExecution.objects.bulk_create(
    [
      ReadyExecution(
        job=job,
        backend_alias=job.backend_alias,
        queue_name=job.queue_name,
        priority=job.priority,
        created_at=now,
      )
      for job in jobs
    ],
    batch_size=1000,
  )

  spawn = multiprocessing.get_context("spawn")
  children_by_pid = {}

  def launcher(spec):
    runner_class_path = f"{spec['runner_class'].__module__}.{spec['runner_class'].__qualname__}"
    child = spawn.Process(
      target=_run_drain_runner,
      args=(runner_class_path, spec["kwargs"], task_count, databases, tasks_settings),
    )
    child.start()
    children_by_pid[child.pid] = child
    return child.pid

  def killer(pid, _signal):
    child = children_by_pid.get(pid)
    if child is None or child.is_alive() is False:
      raise ProcessLookupError(pid)
    child.terminate()
    child.join(timeout=1)

  supervisor = ForkSupervisor.from_backend_config(
    backend_alias="default",
    tasks_settings=tasks_settings,
    standalone=False,
    launcher=launcher,
    killer=killer,
  )
  children = ()

  try:
    supervisor.start()
    children = tuple(children_by_pid[pid] for pid in supervisor.children)
    logger.info("fork volume supervisor started children=%s", len(children))

    _wait_for_fork_drain(task_count, timeout=180, children_by_pid=children_by_pid)

    _join_children(children, timeout=10)
    supervisor.stop()

    assert all(child.exitcode == 0 for child in children)
    assert Process.objects.count() == 0
    assert Job.objects.count() == task_count
    assert FailedExecution.objects.count() == 0
    assert ReadyExecution.objects.count() == 0
    assert ClaimedExecution.objects.count() == 0
    assert Job.objects.filter(finished_at__isnull=False).count() == task_count
    assert Counter(Job.objects.values_list("return_value", flat=True)) == Counter(payloads)
    assert Counter(path.stem for path in output_dir.iterdir()) == Counter(payloads)

    sample_ids = [str(jobs[index].id) for index in (0, task_count // 2, task_count - 1)]
    fetched = [record_once.get_backend().get_result(result_id) for result_id in sample_ids]
    assert [result.status for result in fetched] == [TaskResultStatus.SUCCESSFUL] * 3
  finally:
    try:
      logger.info("fork volume stop")
      supervisor.stop()
    finally:
      _terminate_children(children)
