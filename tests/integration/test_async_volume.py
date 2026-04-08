from collections import Counter
import os

import pytest
from django.tasks import TaskResultStatus
from django.utils import timezone

from dj_queue.models import ClaimedExecution, Job, Process, ReadyExecution
from dj_queue.runtime.supervisor import AsyncSupervisor
from tests.contrib.test_server_integrations import wait_until
from tests.tasks import echo


pytestmark = [
  pytest.mark.django_db(transaction=True),
  pytest.mark.postgres,
  pytest.mark.skipif(
    os.environ.get("RUN_STRESS_ASYNC_VOLUME") != "1",
    reason="run separately with RUN_STRESS_ASYNC_VOLUME=1",
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
          {"queues": "*", "threads": 8, "processes": 1, "polling_interval": 0.01} for _ in range(4)
        ],
        "dispatchers": [],
        "scheduler": None,
        "process_heartbeat_interval": 0,
        "process_alive_threshold": 5,
        "preserve_finished_jobs": True,
        "clear_finished_jobs_after": None,
      },
    }
  }


def test_10k_tasks_async_mode_no_duplicates(queue_test_settings):
  task_count = 10_000
  queue_test_settings(tasks=_queue_tasks(database_alias="default"))

  payloads = [f"task-{index}" for index in range(task_count)]
  now = timezone.now()
  jobs = [
    Job(
      task_path=echo.module_path,
      queue_name=echo.queue_name,
      priority=echo.priority,
      payload={"args": [value], "kwargs": {}},
      backend_name="default",
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
        queue_name=job.queue_name,
        priority=job.priority,
        created_at=now,
      )
      for job in jobs
    ],
    batch_size=1000,
  )

  supervisor = AsyncSupervisor.from_backend_config(backend_alias="default", standalone=False)
  supervisor.start()

  try:
    wait_until(
      lambda: Job.objects.filter(finished_at__isnull=False).count() == task_count,
      timeout=120,
    )

    assert Process.objects.filter(kind="Worker").count() == 4
    assert Job.objects.count() == task_count
    assert ReadyExecution.objects.count() == 0
    assert ClaimedExecution.objects.count() == 0
    assert Job.objects.filter(finished_at__isnull=False).count() == task_count
    assert Counter(Job.objects.values_list("return_value", flat=True)) == Counter(payloads)

    sample_ids = [str(jobs[index].id) for index in (0, task_count // 2, task_count - 1)]
    fetched = [echo.get_backend().get_result(result_id) for result_id in sample_ids]
    assert [result.status for result in fetched] == [TaskResultStatus.SUCCESSFUL] * 3
  finally:
    supervisor.stop()
