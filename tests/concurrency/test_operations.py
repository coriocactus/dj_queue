import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from django.db import connections
from django.tasks import TaskResultStatus
from django.utils import timezone

from dj_queue.models import (
  BlockedExecution,
  FailedExecution,
  Job,
  Pause,
  ReadyExecution,
  Semaphore,
)
from dj_queue.operations.concurrency import (
  cleanup_expired_semaphores,
  promote_expired_blocked_jobs,
  semaphore_acquire,
  semaphore_release,
)
from dj_queue.operations.jobs import (
  claim_ready_jobs,
  complete_claimed_job,
  discard_ready_jobs,
  fail_claimed_job,
)
from tests.tasks import echo, limited, other_queue


def make_job(task=echo, **overrides):
  payload = {
    "args": list(overrides.pop("args", [])),
    "kwargs": dict(overrides.pop("kwargs", {})),
  }
  payload.update(overrides.pop("payload", {}))

  return Job.objects.create(
    task_path=overrides.pop("task_path", task.module_path),
    queue_name=overrides.pop("queue_name", task.queue_name),
    priority=overrides.pop("priority", task.priority),
    payload=payload,
    backend_name=overrides.pop("backend_name", task.backend),
    scheduled_at=overrides.pop("scheduled_at", None),
    concurrency_key=overrides.pop("concurrency_key", None),
    finished_at=overrides.pop("finished_at", None),
    return_value=overrides.pop("return_value", None),
    **overrides,
  )


@pytest.mark.django_db
def test_semaphore_acquire_release_cycle():
  assert semaphore_acquire("account:1", limit=1, duration_seconds=60) is True
  assert semaphore_acquire("account:1", limit=1, duration_seconds=60) is False
  assert semaphore_release("account:1", duration_seconds=60) is True
  assert semaphore_acquire("account:1", limit=1, duration_seconds=60) is True


@pytest.mark.django_db
def test_semaphore_signal_caps_at_limit():
  semaphore_acquire("account:1", limit=2, duration_seconds=60)
  semaphore_release("account:1", duration_seconds=60)
  semaphore_release("account:1", duration_seconds=60)

  semaphore = Semaphore.objects.get(key="account:1")

  assert semaphore.value == semaphore.limit == 2


@pytest.mark.skipif(
  os.environ.get("DB_BACKEND", "sqlite") == "sqlite",
  reason="requires a shared test database across threads",
)
@pytest.mark.django_db(transaction=True)
def test_concurrent_acquire_allows_exactly_limit_successes():
  limit = 2
  attempts = 5
  barrier = threading.Barrier(attempts)

  def acquire_once():
    try:
      barrier.wait()
      return semaphore_acquire("account:concurrent", limit=limit, duration_seconds=60)
    finally:
      connections.close_all()

  with ThreadPoolExecutor(max_workers=attempts) as executor:
    results = list(executor.map(lambda _: acquire_once(), range(attempts)))

  assert results.count(True) == limit
  assert results.count(False) == attempts - limit
  assert Semaphore.objects.get(key="account:concurrent").value == 0


@pytest.mark.django_db
def test_enqueue_with_concurrency_slot_available_goes_ready():
  result = limited.enqueue(1, value="first")

  job = ReadyExecution.objects.get(job_id=result.id).job
  semaphore = Semaphore.objects.get(key="account:1")

  assert result.status.name == "READY"
  assert job.concurrency_key == "account:1"
  assert semaphore.value == 0
  assert semaphore.limit == 1


@pytest.mark.django_db
def test_enqueue_with_concurrency_limit_reached_goes_blocked():
  limited.enqueue(1, value="first")
  second = limited.enqueue(1, value="second")
  job = Job.objects.get(pk=second.id)

  assert ReadyExecution.objects.count() == 1
  assert job.blocked is True
  assert job.concurrency_key == "account:1"
  assert job.blocked_execution.concurrency_key == "account:1"
  assert limited.get_backend().get_result(second.id).status == TaskResultStatus.READY


@pytest.mark.django_db
def test_successful_completion_unblocks_next_waiter():
  first = limited.enqueue(1, value="first")
  second = limited.enqueue(1, value="second")

  claimed_jobs = claim_ready_jobs(limit=1)
  complete_claimed_job(first.id, "done")

  assert [str(job.id) for job in claimed_jobs] == [first.id]
  assert ReadyExecution.objects.filter(job_id=second.id).exists() is True
  assert Semaphore.objects.get(key="account:1").value == 0


@pytest.mark.django_db
def test_failed_completion_still_unblocks_next_waiter():
  first = limited.enqueue(1, value="first")
  second = limited.enqueue(1, value="second")

  claim_ready_jobs(limit=1)
  fail_claimed_job(first.id, ValueError("boom"), traceback_text="traceback")

  assert FailedExecution.objects.filter(job_id=first.id).exists() is True
  assert ReadyExecution.objects.filter(job_id=second.id).exists() is True
  assert Semaphore.objects.get(key="account:1").value == 0


@pytest.mark.django_db
def test_dispatcher_promotes_expired_blocked_jobs():
  job = make_job(task=limited, args=[1], kwargs={"value": "later"}, concurrency_key="account:1")
  BlockedExecution.objects.create(
    job=job,
    queue_name=job.queue_name,
    priority=job.priority,
    concurrency_key=job.concurrency_key,
    expires_at=timezone.now() - timedelta(seconds=1),
  )

  promoted = promote_expired_blocked_jobs(batch_size=10)

  assert [promoted_job.id for promoted_job in promoted] == [job.id]
  assert BlockedExecution.objects.filter(job=job).exists() is False
  assert ReadyExecution.objects.filter(job=job).exists() is True
  assert Semaphore.objects.get(key="account:1").value == 0


@pytest.mark.django_db
def test_discarding_ready_job_releases_waiter():
  first = limited.enqueue(1, value="first")
  second = limited.enqueue(1, value="second")

  deleted = discard_ready_jobs(job_ids=[first.id], batch_size=1)

  assert deleted == 1
  assert ReadyExecution.objects.filter(job_id=second.id).exists() is True
  assert Semaphore.objects.get(key="account:1").value == 0


@pytest.mark.django_db
def test_queue_pause_blocks_claiming_not_enqueue():
  Pause.objects.create(queue_name="other")
  other_queue.enqueue("paused")

  claimed_jobs = claim_ready_jobs(limit=1, queues=("other",))

  assert ReadyExecution.objects.filter(queue_name="other").count() == 1
  assert claimed_jobs == []


@pytest.mark.django_db
def test_queue_resume_restores_claiming():
  pause = Pause.objects.create(queue_name="other")
  result = other_queue.enqueue("paused")

  assert claim_ready_jobs(limit=1, queues=("other",)) == []

  pause.delete()
  claimed_jobs = claim_ready_jobs(limit=1, queues=("other",))

  assert [str(job.id) for job in claimed_jobs] == [result.id]


@pytest.mark.django_db
def test_queue_selector_exact_prefix_and_star_ordering():
  alpha = echo.using(queue_name="alpha").enqueue("alpha")
  mail = echo.using(queue_name="mailers").enqueue("mail")
  default = echo.enqueue("default")

  claimed_jobs = claim_ready_jobs(limit=3, queues=("alpha", "mail*", "*"))

  assert [str(job.id) for job in claimed_jobs] == [alpha.id, mail.id, default.id]


@pytest.mark.django_db
def test_cleanup_expired_semaphores():
  Semaphore.objects.create(
    key="expired",
    value=0,
    limit=1,
    expires_at=timezone.now() - timedelta(seconds=1),
  )
  Semaphore.objects.create(
    key="fresh",
    value=0,
    limit=1,
    expires_at=timezone.now() + timedelta(minutes=1),
  )

  deleted = cleanup_expired_semaphores()

  assert deleted == 1
  assert Semaphore.objects.filter(key="expired").exists() is False
  assert Semaphore.objects.filter(key="fresh").exists() is True
