import os
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from django.db import connections
from django.utils import timezone

from dj_queue.models import Job, RecurringExecution, RecurringTask
from dj_queue.operations import recurring
from dj_queue.operations.jobs import claim_ready_jobs, execute_claimed_job

pytestmark = [
  pytest.mark.django_db(transaction=True),
  pytest.mark.skipif(
    os.environ.get("DB_BACKEND", "sqlite") == "sqlite",
    reason="requires a shared test database across threads",
  ),
]


def test_recurring_publication_hides_job_and_locks_out_other_schedulers(settings, monkeypatch):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {"preserve_finished_jobs": False},
    }
  }
  recurring_task = RecurringTask.objects.create(
    backend_alias="default",
    key="atomic-publication",
    task_path="tests.tasks.echo",
    payload={"args": ["once"], "kwargs": {}},
    schedule="* * * * *",
  )
  run_at = timezone.now().replace(second=0, microsecond=0)
  attaching = threading.Event()
  allow_attachment = threading.Event()
  original_attach = recurring._attach_reserved_recurring_job

  def paused_attach(*args, **kwargs):
    attaching.set()
    assert allow_attachment.wait(10)
    return original_attach(*args, **kwargs)

  def publish():
    try:
      return recurring.fire_recurring_task(recurring_task, run_at)
    finally:
      connections.close_all()

  monkeypatch.setattr(recurring, "_attach_reserved_recurring_job", paused_attach)

  with ThreadPoolExecutor(max_workers=2) as executor:
    future = executor.submit(publish)
    try:
      assert attaching.wait(10)
      early_claims = claim_ready_jobs(limit=1)
      competing_execution = executor.submit(publish).result(timeout=5)
    finally:
      allow_attachment.set()
    execution = future.result(timeout=10)

  assert early_claims == []
  assert competing_execution is None
  assert RecurringExecution.objects.get(pk=execution.pk).job_id == execution.job_id
  claimed_job = claim_ready_jobs(limit=1)[0]
  assert claimed_job.job.id == execution.job_id
  execute_claimed_job(claimed_job)

  assert Job.objects.exists() is False
  assert recurring.fire_recurring_task(recurring_task, run_at) is None
  assert claim_ready_jobs(limit=1) == []
