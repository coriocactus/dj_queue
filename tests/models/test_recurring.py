from datetime import timedelta

import pytest
from django.core.exceptions import ValidationError
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
    backend_name=overrides.pop("backend_name", "default"),
    scheduled_at=overrides.pop("scheduled_at", None),
    concurrency_key=overrides.pop("concurrency_key", None),
    finished_at=overrides.pop("finished_at", None),
    return_value=overrides.pop("return_value", None),
    **overrides,
  )


@pytest.mark.django_db
def test_recurring_execution_allows_null_job_during_reservation():
  task = RecurringTask.objects.create(
    key="every-minute",
    task_path="tests.tasks.example",
    schedule="* * * * *",
  )
  run_at = timezone.now().replace(second=0, microsecond=0) + timedelta(minutes=1)

  execution = RecurringExecution.objects.create(task_key=task.key, run_at=run_at)

  assert execution.job is None

  job = make_job(task_path=task.task_path)
  execution.job = job
  execution.save(update_fields=["job"])

  assert RecurringExecution.objects.get(pk=execution.pk).job == job


@pytest.mark.django_db
def test_recurring_task_key_unique():
  RecurringTask.objects.create(
    key="every-minute",
    task_path="tests.tasks.example",
    schedule="* * * * *",
  )

  with pytest.raises(ValidationError, match="Key"):
    RecurringTask.objects.create(
      key="every-minute",
      task_path="tests.tasks.other",
      schedule="*/5 * * * *",
    )


@pytest.mark.django_db
def test_recurring_execution_task_key_run_at_unique():
  run_at = timezone.now().replace(second=0, microsecond=0) + timedelta(minutes=1)
  RecurringExecution.objects.create(task_key="every-minute", run_at=run_at)

  with pytest.raises(IntegrityError), transaction.atomic():
    RecurringExecution.objects.create(task_key="every-minute", run_at=run_at)


@pytest.mark.django_db
def test_recurring_task_schedule_validation():
  with pytest.raises(ValidationError, match="schedule"):
    RecurringTask.objects.create(
      key="bad-cron",
      task_path="tests.tasks.example",
      schedule="tomorrow",
    )
