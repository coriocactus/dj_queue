from datetime import timedelta
from uuid import uuid4

import pytest
from django.db import IntegrityError, transaction
from django.utils import timezone

from dj_queue.models import Job, RecurringExecution, RecurringTask


def make_job(**overrides):
  payload = {
    "args": [],
    "kwargs": {},
  }
  payload.update(overrides.pop("payload", {}))

  return Job.objects.create(
    task_path=overrides.pop("task_path", "tests.tasks.example"),
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


@pytest.mark.django_db
def test_recurring_execution_allows_null_job_during_reservation():
  task = RecurringTask.objects.create(
    backend_alias="default",
    key="every-minute",
    task_path="tests.tasks.example",
    schedule="* * * * *",
  )
  run_at = timezone.now().replace(second=0, microsecond=0) + timedelta(minutes=1)

  execution = RecurringExecution.objects.create(
    backend_alias="default",
    task_key=task.key,
    run_at=run_at,
  )

  assert execution.job is None
  assert execution.intended_job_id is not None

  job = make_job(id=execution.intended_job_id, task_path=task.task_path)
  execution.job = job
  execution.save(update_fields=["job"])

  assert RecurringExecution.objects.get(pk=execution.pk).job == job


@pytest.mark.django_db
def test_recurring_execution_job_matches_intended_job():
  execution = RecurringExecution.objects.create(
    backend_alias="default",
    task_key="every-minute",
    run_at=timezone.now(),
  )
  other_job = make_job(id=uuid4())

  execution.job = other_job
  with pytest.raises(IntegrityError), transaction.atomic():
    execution.save(update_fields=["job"])


@pytest.mark.django_db
def test_recurring_task_key_unique():
  RecurringTask.objects.create(
    backend_alias="default",
    key="every-minute",
    task_path="tests.tasks.example",
    schedule="* * * * *",
  )

  with pytest.raises(IntegrityError), transaction.atomic():
    RecurringTask.objects.create(
      backend_alias="default",
      key="every-minute",
      task_path="tests.tasks.other",
      schedule="*/5 * * * *",
    )


@pytest.mark.django_db
def test_recurring_execution_task_key_run_at_unique():
  run_at = timezone.now().replace(second=0, microsecond=0) + timedelta(minutes=1)
  RecurringExecution.objects.create(
    backend_alias="default",
    task_key="every-minute",
    run_at=run_at,
  )

  with pytest.raises(IntegrityError), transaction.atomic():
    RecurringExecution.objects.create(
      backend_alias="default",
      task_key="every-minute",
      run_at=run_at,
    )


@pytest.mark.django_db
def test_recurring_task_model_save_does_not_validate_schedule():
  task = RecurringTask.objects.create(
    backend_alias="default",
    key="bad-cron",
    task_path="tests.tasks.example",
    schedule="tomorrow",
  )

  assert task.schedule == "tomorrow"
