import logging
import os
import time
from collections import Counter

import pytest
from django.utils import timezone

from dj_queue.models import ClaimedExecution, Job, Process, ReadyExecution
from dj_queue.runtime.supervisor import AsyncSupervisor
from tests.tasks import echo

logger = logging.getLogger("dj_queue.stress")

pytestmark = [
  pytest.mark.django_db(transaction=True),
  pytest.mark.skipif(
    os.environ.get("STRESS") != "1",
    reason="run separately with STRESS=1",
  ),
  pytest.mark.skipif(
    os.environ.get("DB_BACKEND", "sqlite") == "sqlite",
    reason="requires a shared test database across worker threads",
  ),
]


def _queue_tasks(database_alias="default"):
  return {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {
        "mode": "async",
        "database_alias": database_alias,
        "workers": [
          {"queues": "*", "threads": 4, "processes": 1, "polling_interval": 0.01} for _ in range(2)
        ],
        "dispatchers": [],
        "scheduler": None,
        "process_heartbeat_interval": 0,
        "process_alive_threshold": 5,
        "shutdown_timeout": 60,
        "preserve_finished_jobs": True,
        "clear_finished_jobs_after": None,
      },
    }
  }


def _seed_ready_jobs(task_count):
  payloads = [f"task-{index}" for index in range(task_count)]
  now = timezone.now()
  jobs = [
    Job(
      task_path=echo.module_path,
      queue_name=echo.queue_name,
      priority=echo.priority,
      payload={"args": [value], "kwargs": {}},
      backend_alias="default",
      created_at=now,
      updated_at=now,
    )
    for value in payloads
  ]
  Job.objects.bulk_create(jobs, batch_size=500)
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
    batch_size=500,
  )
  return payloads


def _wait_for_drain(task_count, *, timeout):
  deadline = time.monotonic() + timeout
  while time.monotonic() < deadline:
    finished = Job.objects.filter(finished_at__isnull=False).count()
    ready = ReadyExecution.objects.count()
    claimed = ClaimedExecution.objects.count()
    if finished == task_count and ready == 0 and claimed == 0:
      return
    time.sleep(0.05)
  assert Job.objects.filter(finished_at__isnull=False).count() == task_count
  assert ReadyExecution.objects.count() == 0
  assert ClaimedExecution.objects.count() == 0


def test_async_worker_drain_smoke_on_non_sqlite_backends(queue_test_settings):
  task_count = 1_000
  queue_test_settings(tasks=_queue_tasks(database_alias="default"))
  payloads = _seed_ready_jobs(task_count)
  supervisor = AsyncSupervisor.from_backend_config(backend_alias="default", standalone=False)
  logger.info("non-sqlite async drain start task_count=%s", task_count)

  supervisor.start()
  try:
    _wait_for_drain(task_count, timeout=120)

    assert Process.objects.filter(kind="Worker").count() == 2
    assert Job.objects.count() == task_count
    assert ReadyExecution.objects.count() == 0
    assert ClaimedExecution.objects.count() == 0
    assert Counter(Job.objects.values_list("return_value", flat=True)) == Counter(payloads)
  finally:
    logger.info("non-sqlite async drain stop")
    supervisor.stop()
