from datetime import timedelta

import pytest
from django.tasks import TaskResultStatus
from django.utils import timezone

from dj_queue.models import (
  BlockedExecution,
  ClaimedExecution,
  FailedExecution,
  Job,
  ReadyExecution,
  ScheduledExecution,
  Semaphore,
)
from dj_queue.operations.cleanup import clear_failed_jobs, clear_finished_jobs
from dj_queue.operations.concurrency import promote_expired_blocked_jobs, unblock_next_blocked_job
from dj_queue.operations.jobs import (
  EnqueueError,
  discard_blocked_jobs,
  discard_ready_jobs,
  promote_scheduled_jobs,
  retry_failed_job,
)
from tests.tasks import limited_discard

pytestmark = pytest.mark.django_db(transaction=True)


def make_job(**overrides):
  payload = {
    "args": list(overrides.pop("args", [])),
    "kwargs": dict(overrides.pop("kwargs", {})),
  }
  payload.update(overrides.pop("payload", {}))

  return Job.objects.create(
    task_path=overrides.pop("task_path", "tests.tasks.echo"),
    queue_name=overrides.pop("queue_name", "default"),
    priority=overrides.pop("priority", 0),
    payload=payload,
    backend_alias=overrides.pop("backend_alias", "default"),
    scheduled_at=overrides.pop("scheduled_at", None),
    concurrency_key=overrides.pop("concurrency_key", None),
    finished_at=overrides.pop("finished_at", None),
    return_value=overrides.pop("return_value", None),
    **overrides,
  )


def test_retry_failed_future_scheduled_job_returns_to_scheduled_state():
  future = timezone.now() + timedelta(minutes=5)
  job = make_job(task_path="tests.tasks.echo", scheduled_at=future)
  FailedExecution.objects.create(
    job=job,
    exception_class="builtins.ValueError",
    message="boom",
    traceback="traceback",
  )

  retry_failed_job(job.id)

  job.refresh_from_db()
  assert FailedExecution.objects.filter(job=job).exists() is False
  assert job.scheduled is True
  assert ReadyExecution.objects.filter(job=job).exists() is False


def test_discard_blocked_job_does_not_change_semaphore_value():
  Semaphore.objects.create(
    key="account:1",
    value=0,
    active_count=1,
    limit=1,
    expires_at=timezone.now() + timedelta(minutes=1),
  )
  job = make_job(task_path="tests.tasks.limited", concurrency_key="account:1")
  BlockedExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
    concurrency_key=job.concurrency_key,
    expires_at=timezone.now() + timedelta(minutes=1),
  )

  deleted = discard_blocked_jobs(job_ids=[job.id], batch_size=1)

  assert deleted == 1
  assert Semaphore.objects.get(key="account:1").value == 0


def test_discard_ready_job_rejects_conflicting_execution_state():
  job = make_job()
  ReadyExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
  )
  FailedExecution.objects.create(
    job=job,
    exception_class="builtins.ValueError",
    message="boom",
    traceback="traceback",
  )

  with pytest.raises(EnqueueError, match="already has an execution-state row"):
    discard_ready_jobs(job_ids=[job.id], batch_size=1)

  assert Job.objects.filter(pk=job.pk).exists() is True
  assert ReadyExecution.objects.filter(job=job).exists() is True
  assert FailedExecution.objects.filter(job=job).exists() is True


def test_discard_ready_job_rejects_mismatched_state_backend_alias():
  job = make_job(backend_alias="secondary")
  ReadyExecution.objects.create(
    job=job,
    backend_alias="default",
    queue_name=job.queue_name,
    priority=job.priority,
  )

  with pytest.raises(EnqueueError, match="belongs to backend 'secondary'"):
    discard_ready_jobs(job_ids=[job.id], batch_size=1, backend_alias="default")

  assert Job.objects.filter(pk=job.pk).exists() is True
  assert ReadyExecution.objects.filter(job=job).exists() is True


def test_clear_failed_jobs_rejects_conflicting_execution_state():
  job = make_job()
  FailedExecution.objects.create(
    job=job,
    exception_class="builtins.ValueError",
    message="boom",
    traceback="traceback",
  )
  ClaimedExecution.objects.create(job=job)

  with pytest.raises(EnqueueError, match="already has an execution-state row"):
    clear_failed_jobs(older_than=0, now=timezone.now() + timedelta(seconds=1))

  assert Job.objects.filter(pk=job.pk).exists() is True
  assert FailedExecution.objects.filter(job=job).exists() is True
  assert ClaimedExecution.objects.filter(job=job).exists() is True


def test_clear_finished_jobs_rejects_conflicting_execution_state():
  job = make_job(finished_at=timezone.now() - timedelta(minutes=1))
  ReadyExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
  )

  with pytest.raises(EnqueueError, match="already has an execution-state row"):
    clear_finished_jobs(older_than=0, now=timezone.now())

  assert Job.objects.filter(pk=job.pk).exists() is True
  assert ReadyExecution.objects.filter(job=job).exists() is True


def test_promote_scheduled_jobs_rejects_mismatched_state_backend_alias():
  job = make_job(
    backend_alias="secondary",
    scheduled_at=timezone.now() - timedelta(seconds=1),
  )
  ScheduledExecution.objects.create(
    job=job,
    backend_alias="default",
    queue_name=job.queue_name,
    priority=job.priority,
    scheduled_at=job.scheduled_at,
  )

  with pytest.raises(EnqueueError, match="belongs to backend 'secondary'"):
    promote_scheduled_jobs(batch_size=1, backend_alias="default")

  assert ScheduledExecution.objects.filter(job=job).exists() is True
  assert ReadyExecution.objects.filter(job=job).exists() is False


def test_unblock_next_blocked_job_rejects_mismatched_state_backend_alias():
  job = make_job(backend_alias="secondary", concurrency_key="account:1")
  BlockedExecution.objects.create(
    job=job,
    backend_alias="default",
    queue_name=job.queue_name,
    priority=job.priority,
    concurrency_key=job.concurrency_key,
    expires_at=timezone.now() + timedelta(minutes=1),
  )

  with pytest.raises(EnqueueError, match="belongs to backend 'secondary'"):
    unblock_next_blocked_job("account:1", limit=1, duration_seconds=60, backend_alias="default")

  assert BlockedExecution.objects.filter(job=job).exists() is True
  assert ReadyExecution.objects.filter(job=job).exists() is False


def test_promote_expired_blocked_jobs_rejects_mismatched_state_backend_alias():
  job = make_job(backend_alias="secondary", concurrency_key="account:1")
  BlockedExecution.objects.create(
    job=job,
    backend_alias="default",
    queue_name=job.queue_name,
    priority=job.priority,
    concurrency_key=job.concurrency_key,
    expires_at=timezone.now() - timedelta(seconds=1),
  )

  with pytest.raises(EnqueueError, match="belongs to backend 'secondary'"):
    promote_expired_blocked_jobs(batch_size=1, backend_alias="default")

  assert BlockedExecution.objects.filter(job=job).exists() is True
  assert ReadyExecution.objects.filter(job=job).exists() is False


def test_discard_on_conflict_creates_no_execution_rows():
  limited_discard.enqueue(1, value="first")

  result = limited_discard.enqueue(1, value="second")
  job = Job.objects.get(pk=result.id)

  assert ReadyExecution.objects.filter(job=job).exists() is False
  assert BlockedExecution.objects.filter(job=job).exists() is False
  assert FailedExecution.objects.filter(job=job).exists() is False
  assert job.claimed is False


def test_discard_on_conflict_result_is_terminal():
  limited_discard.enqueue(1, value="first")

  result = limited_discard.enqueue(1, value="second")
  job = Job.objects.get(pk=result.id)
  fetched = limited_discard.get_backend().get_result(result.id)

  assert fetched.status == TaskResultStatus.SUCCESSFUL
  assert job.finished_at is not None
  assert job.return_value is None
