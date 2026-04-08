from pathlib import Path
import multiprocessing
import time

import pytest


pytestmark = [
  pytest.mark.django_db(transaction=True),
  pytest.mark.postgres,
]


def _queue_tasks(database_alias="default"):
  return {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {
        "mode": "fork",
        "database_alias": database_alias,
        "workers": [{"queues": "*", "threads": 1, "processes": 1, "polling_interval": 0.01}],
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


def _run_polling_runner(runner_class_path, kwargs, databases, tasks_settings, stop_path):
  import django
  from django.apps import apps
  from django.conf import settings

  settings.DATABASES = databases
  settings.TASKS = tasks_settings

  if apps.ready is False:
    django.setup()

  from django.db import connections
  from django.utils.module_loading import import_string

  connections.close_all()
  runner_class = import_string(runner_class_path)
  runner = runner_class(**kwargs)
  try:
    runner.start()
    while Path(stop_path).exists() is False:
      runner.poll_once()
      time.sleep(runner.polling_interval)
  finally:
    runner.stop(timeout=5)


def _join_children(children, timeout):
  deadline = time.monotonic() + timeout
  while time.monotonic() < deadline:
    if all(child.is_alive() is False for child in children):
      return
    if children:
      time.sleep(0.01)
  assert all(child.is_alive() is False for child in children)


def _terminate_children(children):
  for child in children:
    if child.is_alive():
      child.terminate()
  for child in children:
    child.join(timeout=1)


def test_kill_worker_mid_execution_recovery(tmp_path, queue_test_settings):
  from django.conf import settings

  from dj_queue.exceptions import ProcessExitError
  from dj_queue.models import ClaimedExecution, FailedExecution, Job, Process, ReadyExecution
  from dj_queue.operations.jobs import retry_failed_job
  from dj_queue.runtime.supervisor import ForkSupervisor
  from tests.contrib.test_server_integrations import wait_until
  from tests.tasks import signal_and_wait

  tasks_settings = _queue_tasks(database_alias="default")
  queue_test_settings(tasks=tasks_settings)
  databases = settings.DATABASES
  control_dir = tmp_path / "recovery"
  control_dir.mkdir()
  stop_path = control_dir / "stop"
  job = signal_and_wait.enqueue(str(control_dir), "recovered")

  spawn = multiprocessing.get_context("spawn")
  children_by_pid = {}
  reported_exits = set()

  def launcher(spec):
    runner_class_path = f"{spec['runner_class'].__module__}.{spec['runner_class'].__qualname__}"
    child = spawn.Process(
      target=_run_polling_runner,
      args=(runner_class_path, spec["kwargs"], databases, tasks_settings, str(stop_path)),
    )
    child.start()
    children_by_pid[child.pid] = child
    return child.pid

  def waitpid(_pid, _flags):
    for pid, child in children_by_pid.items():
      child.join(timeout=0)
      if child.exitcode is None or pid in reported_exits:
        continue
      reported_exits.add(pid)
      return pid, child.exitcode
    return 0, 0

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
    waitpid=waitpid,
    killer=killer,
  )

  try:
    supervisor.start()
    first_pid = next(iter(supervisor.children))
    first_child = children_by_pid[first_pid]

    wait_until(lambda: (control_dir / "started").exists())
    wait_until(
      lambda: (
        ClaimedExecution.objects.filter(job_id=job.id, process__pid=first_pid).exists() is True
      )
    )

    first_child.terminate()
    first_child.join(timeout=1)
    wait_until(lambda: first_child.exitcode is not None)

    replacement_pid = supervisor.check_children()
    failed_execution = FailedExecution.objects.get(job_id=job.id)

    assert replacement_pid != first_pid
    assert failed_execution.exception_class == (
      f"{ProcessExitError.__module__}.{ProcessExitError.__qualname__}"
    )
    assert Process.objects.filter(pid=first_pid).exists() is False
    assert replacement_pid in supervisor.children
    assert ReadyExecution.objects.filter(job_id=job.id).exists() is False

    retry_failed_job(job.id)
    (control_dir / "release").write_text("release")

    wait_until(
      lambda: (
        Job.objects.filter(pk=job.id, finished_at__isnull=False, return_value="recovered").exists()
        is True
      ),
      timeout=5,
    )

    assert FailedExecution.objects.filter(job_id=job.id).exists() is False
    assert ClaimedExecution.objects.filter(job_id=job.id).exists() is False
    assert ReadyExecution.objects.filter(job_id=job.id).exists() is False

    stop_path.write_text("stop")
    _join_children((children_by_pid[replacement_pid],), timeout=5)
    supervisor.stop()

    assert Process.objects.count() == 0
  finally:
    stop_path.write_text("stop")
    try:
      supervisor.stop()
    finally:
      _terminate_children(tuple(children_by_pid.values()))
