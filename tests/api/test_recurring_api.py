import pytest
from django.core.exceptions import ValidationError
from django.utils import timezone

from dj_queue.api import schedule_recurring_task, unschedule_recurring_task
from dj_queue.models import RecurringTask

pytestmark = pytest.mark.django_db(transaction=True)


def test_schedule_recurring_task_creates_dynamic_row():
  task = schedule_recurring_task(
    key="dynamic-task",
    task_path="tests.tasks.echo",
    schedule="* * * * *",
    args=("hello",),
    kwargs={"value": "world"},
    queue_name="maintenance",
    priority=5,
    description="dynamic task",
  )

  assert task.key == "dynamic-task"
  assert task.backend_alias == "default"
  assert task.static is False
  assert task.payload == {"args": ["hello"], "kwargs": {"value": "world"}}
  assert task.queue_name == "maintenance"
  assert task.priority == 5
  assert task.description == "dynamic task"


def test_unschedule_recurring_task_removes_dynamic_row():
  schedule_recurring_task(
    key="dynamic-task",
    task_path="tests.tasks.echo",
    schedule="* * * * *",
  )

  deleted = unschedule_recurring_task("dynamic-task")

  assert deleted == 1
  assert (
    RecurringTask.objects.filter(backend_alias="default", key="dynamic-task").exists() is False
  )


def test_recurring_tasks_are_backend_scoped(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {"database_alias": "default"},
    },
    "secondary": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {"database_alias": "default"},
    },
  }
  schedule_recurring_task(
    key="dynamic-task",
    task_path="tests.tasks.echo",
    schedule="* * * * *",
    backend_alias="default",
  )
  schedule_recurring_task(
    key="dynamic-task",
    task_path="tests.tasks.echo",
    schedule="0 * * * *",
    backend_alias="secondary",
  )

  assert RecurringTask.objects.filter(backend_alias="default", key="dynamic-task").exists() is True
  assert (
    RecurringTask.objects.filter(backend_alias="secondary", key="dynamic-task").exists() is True
  )

  deleted = unschedule_recurring_task("dynamic-task", backend_alias="default")

  assert deleted == 1
  assert (
    RecurringTask.objects.filter(backend_alias="default", key="dynamic-task").exists() is False
  )
  assert (
    RecurringTask.objects.filter(backend_alias="secondary", key="dynamic-task").exists() is True
  )


def test_schedule_recurring_task_resets_persisted_next_run_when_schedule_changes():
  recurring_task = schedule_recurring_task(
    key="dynamic-task",
    task_path="tests.tasks.echo",
    schedule="* * * * *",
  )
  RecurringTask.objects.filter(pk=recurring_task.pk).update(next_run_at=timezone.now())

  updated_task = schedule_recurring_task(
    key="dynamic-task",
    task_path="tests.tasks.echo",
    schedule="*/5 * * * *",
  )

  assert updated_task.next_run_at is None


def test_invalid_cron_is_rejected():
  with pytest.raises(ValidationError):
    schedule_recurring_task(
      key="invalid-task",
      task_path="tests.tasks.echo",
      schedule="not a cron",
    )


def test_missing_recurring_task_path_is_rejected_without_persisting():
  with pytest.raises(ImportError):
    schedule_recurring_task(
      key="missing-task",
      task_path="tests.tasks.missing_recurring_task",
      schedule="* * * * *",
    )

  assert RecurringTask.objects.filter(key="missing-task").exists() is False


def test_schedule_recurring_task_validates_once(monkeypatch):
  calls = []
  original_full_clean = RecurringTask.full_clean

  def counted_full_clean(self, *args, **kwargs):
    calls.append(self.key)
    return original_full_clean(self, *args, **kwargs)

  monkeypatch.setattr(RecurringTask, "full_clean", counted_full_clean)

  schedule_recurring_task(
    key="dynamic-task",
    task_path="tests.tasks.echo",
    schedule="* * * * *",
  )

  assert calls == ["dynamic-task"]
