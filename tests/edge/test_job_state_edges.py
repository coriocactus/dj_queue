from datetime import timedelta

import pytest
from django.tasks import TaskResultStatus
from django.utils import timezone

from dj_queue.models import BlockedExecution, FailedExecution, Job, ReadyExecution, Semaphore
from dj_queue.operations.jobs import discard_blocked_jobs, retry_failed_job
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
