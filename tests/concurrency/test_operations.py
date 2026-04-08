from datetime import timedelta

import pytest
from django.tasks import TaskResultStatus
from django.utils import timezone

from dj_queue.models import FailedExecution, Job, Pause, ReadyExecution, Semaphore
from dj_queue.operations.concurrency import (
  cleanup_expired_semaphores,
  semaphore_acquire,
  semaphore_release,
)
from dj_queue.operations.jobs import (
  claim_ready_jobs,
  complete_claimed_job,
  discard_ready_jobs,
  fail_claimed_job,
)
from tests.tasks import limited, other_queue


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
