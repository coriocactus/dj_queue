import pytest
from django.core.exceptions import ValidationError

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
  assert RecurringTask.objects.filter(key="dynamic-task").exists() is False


def test_invalid_cron_is_rejected():
  with pytest.raises(ValidationError):
    schedule_recurring_task(
      key="invalid-task",
      task_path="tests.tasks.echo",
      schedule="not a cron",
    )


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
