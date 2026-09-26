import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from unittest.mock import Mock

import pytest
from django.db import connections
from django.utils import timezone

from dj_queue.models import BlockedExecution, ClaimedExecution, Job, ReadyExecution, Semaphore
from dj_queue.operations import concurrency
from dj_queue.operations.claiming import claim_ready_jobs
from dj_queue.operations.execution import execute_claimed_job
from tests.tasks import limited


@pytest.mark.django_db(transaction=True)
@pytest.mark.skipif(
  os.environ.get("DB_BACKEND", "sqlite") == "sqlite",
  reason="requires row locks and a shared test database across threads",
)
def test_competing_maintenance_batches_share_one_slot(monkeypatch):
  expired_at = timezone.now() - timedelta(seconds=10)
  jobs = [
    Job.objects.create(
      task_path=limited.module_path,
      queue_name="default",
      backend_alias="default",
      payload={"args": [1], "kwargs": {"value": f"job-{index}"}},
      concurrency_key="account:1",
      concurrency_limit=1,
      concurrency_duration=60,
      concurrency_on_conflict="block",
    )
    for index in range(2)
  ]
  for index, job in enumerate(jobs):
    BlockedExecution.objects.create(
      job=job,
      backend_alias=job.backend_alias,
      queue_name=job.queue_name,
      priority=job.priority,
      concurrency_key=job.concurrency_key,
      expires_at=expired_at + timedelta(seconds=index),
    )

  acquired = threading.Event()
  competing = threading.Event()
  release = threading.Event()
  acquire = concurrency.semaphore_acquire
  notify = Mock()

  def held_acquire(*args, **kwargs):
    if acquired.is_set():
      competing.set()
      return acquire(*args, **kwargs)
    result = acquire(*args, **kwargs)
    acquired.set()
    assert release.wait(timeout=5)
    return result

  def promote():
    try:
      return concurrency.promote_expired_blocked_jobs(batch_size=1, use_skip_locked=True)
    finally:
      connections.close_all()

  monkeypatch.setattr(concurrency, "semaphore_acquire", held_acquire)
  monkeypatch.setattr(concurrency, "notify_ready_queues_on_commit", notify)
  with ThreadPoolExecutor(max_workers=2) as pool:
    first = pool.submit(promote)
    try:
      assert acquired.wait(timeout=5)
      second = pool.submit(promote)
      assert competing.wait(timeout=5)
      assert not second.done()
    finally:
      release.set()
    assert [job.id for job in first.result(timeout=5)] == [jobs[0].id]
    assert second.result(timeout=5) == []

  assert list(ReadyExecution.objects.values_list("job_id", flat=True)) == [jobs[0].id]
  assert not ClaimedExecution.objects.exists()
  waiter = BlockedExecution.objects.get(job=jobs[1])
  assert waiter.expires_at > timezone.now()
  semaphore = Semaphore.objects.get(key="account:1")
  assert (semaphore.value, semaphore.active_count) == (0, 1)
  notify.assert_called_once()

  for _ in jobs:
    (claimed,) = claim_ready_jobs(limit=1)
    execute_claimed_job(claimed)
  assert Job.objects.filter(finished_at__isnull=False).count() == len(jobs)
  assert not BlockedExecution.objects.exists()
  assert not ReadyExecution.objects.exists()
  assert not ClaimedExecution.objects.exists()
  semaphore.refresh_from_db()
  assert (semaphore.value, semaphore.active_count) == (1, 0)
