from concurrent.futures import ThreadPoolExecutor
import os
import time

import pytest
from django.core.management import call_command
from django.db import OperationalError, connections
from django.utils import timezone

from dj_queue.models import ClaimedExecution, Job, ReadyExecution
from dj_queue.operations.jobs import claim_ready_jobs
from tests.tasks import echo


pytestmark = [
  pytest.mark.django_db(transaction=True),
  pytest.mark.filterwarnings(
    r"ignore:Overriding setting DATABASES can lead to unexpected behavior\.:UserWarning"
  ),
  pytest.mark.skipif(
    os.environ.get("DB_BACKEND", "sqlite") != "sqlite",
    reason="file-backed SQLite contention coverage runs only under DB_BACKEND=sqlite",
  ),
]


def _sqlite_databases(tmp_path):
  return {
    "default": {
      "ENGINE": "django.db.backends.sqlite3",
      "NAME": str(tmp_path / "queue.sqlite3"),
    }
  }


def _queue_tasks():
  return {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {"database_alias": "default", "use_skip_locked": False},
    }
  }


def _seed_ready_jobs(task_count):
  now = timezone.now()
  jobs = [
    Job(
      task_path=echo.module_path,
      queue_name=echo.queue_name,
      priority=echo.priority,
      payload={"args": [index], "kwargs": {}},
      backend_alias="default",
      created_at=now,
      updated_at=now,
    )
    for index in range(task_count)
  ]
  Job.objects.bulk_create(jobs)
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
    ]
  )


def _claim_until_empty():
  claimed_ids = []
  connections.close_all()
  try:
    while True:
      try:
        claimed_jobs = claim_ready_jobs(limit=1, use_skip_locked=False)
      except OperationalError as exc:
        if "database is locked" not in str(exc).lower():
          raise
        time.sleep(0.01)
        continue
      if claimed_jobs:
        claimed_ids.append(claimed_jobs[0].job.id)
        continue
      if ReadyExecution.objects.count() == 0:
        return claimed_ids
      time.sleep(0.01)
  finally:
    connections.close_all()


def test_file_backed_sqlite_concurrent_claims_do_not_duplicate_or_drop_jobs(
  tmp_path,
  django_db_blocker,
  queue_test_settings,
):
  task_count = 20
  queue_test_settings(databases=_sqlite_databases(tmp_path), tasks=_queue_tasks())

  with django_db_blocker.unblock():
    call_command("migrate", "dj_queue", database="default", interactive=False, verbosity=0)
    _seed_ready_jobs(task_count)

    with ThreadPoolExecutor(max_workers=2) as executor:
      claimed_ids = [
        job_id
        for worker_ids in executor.map(lambda _index: _claim_until_empty(), range(2))
        for job_id in worker_ids
      ]

    assert len(claimed_ids) == task_count
    assert len(set(claimed_ids)) == task_count
    assert ReadyExecution.objects.count() == 0
    assert ClaimedExecution.objects.count() == task_count
