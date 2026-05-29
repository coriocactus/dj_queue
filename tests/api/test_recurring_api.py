import pytest
from django.utils import timezone

from dj_queue.api import schedule_recurring_task, unschedule_recurring_task
from dj_queue.exceptions import EnqueueError
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


def test_schedule_recurring_task_rejects_overwriting_static_task():
  RecurringTask.objects.create(
    backend_alias="default",
    key="shared-task",
    task_path="tests.tasks.echo",
    payload={"args": [], "kwargs": {}},
    schedule="* * * * *",
    queue_name="default",
    priority=0,
    static=True,
  )

  with pytest.raises(EnqueueError, match="already managed statically"):
    schedule_recurring_task(
      key="shared-task",
      task_path="tests.tasks.echo",
      schedule="*/5 * * * *",
    )


def test_schedule_recurring_task_rejects_configured_static_task(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {
        "recurring": {
          "shared-task": {
            "task_path": "tests.tasks.echo",
            "schedule": "* * * * *",
          }
        }
      },
    }
  }

  with pytest.raises(EnqueueError, match="already managed statically"):
    schedule_recurring_task(
      key="shared-task",
      task_path="tests.tasks.echo",
      schedule="*/5 * * * *",
    )


def test_invalid_cron_is_rejected():
  with pytest.raises(EnqueueError, match="schedule must be a valid cron expression"):
    schedule_recurring_task(
      key="invalid-task",
      task_path="tests.tasks.echo",
      schedule="not a cron",
    )


def test_schedule_recurring_task_rejects_invalid_priority():
  with pytest.raises(EnqueueError, match="priority must be an integer from -100 to 100"):
    schedule_recurring_task(
      key="invalid-priority",
      task_path="tests.tasks.echo",
      schedule="* * * * *",
      priority=101,
    )

  assert RecurringTask.objects.filter(key="invalid-priority").exists() is False


def test_missing_recurring_task_path_is_rejected_without_persisting():
  with pytest.raises(EnqueueError, match="task_path must be importable"):
    schedule_recurring_task(
      key="missing-task",
      task_path="tests.tasks.missing_recurring_task",
      schedule="* * * * *",
    )

  assert RecurringTask.objects.filter(key="missing-task").exists() is False


@pytest.mark.parametrize(
  ("option_name", "value", "message"),
  (
    ("key", "", "key must be a non-empty string"),
    ("key", 1, "key must be a non-empty string"),
    ("task_path", "", "task_path must be a non-empty string"),
    ("task_path", 1, "task_path must be a non-empty string"),
    ("schedule", "", "schedule must be a non-empty string"),
    ("schedule", 1, "schedule must be a non-empty string"),
    ("queue_name", "", "queue_name must be a non-empty string"),
    ("queue_name", 1, "queue_name must be a non-empty string"),
    ("description", 1, "description must be a string"),
  ),
)
def test_schedule_recurring_task_rejects_invalid_string_options(option_name, value, message):
  options = {
    "key": "dynamic-task",
    "task_path": "tests.tasks.echo",
    "schedule": "* * * * *",
    "queue_name": "default",
    "description": "",
  }
  options[option_name] = value

  with pytest.raises(EnqueueError, match=message):
    schedule_recurring_task(**options)

  assert RecurringTask.objects.filter(key="dynamic-task").exists() is False


def test_schedule_recurring_task_rejects_non_task_path_without_persisting():
  with pytest.raises(EnqueueError, match="task_path must reference a Django task"):
    schedule_recurring_task(
      key="not-a-task",
      task_path="dj_queue.config.load_backend_config",
      schedule="* * * * *",
    )

  assert RecurringTask.objects.filter(key="not-a-task").exists() is False


def test_schedule_recurring_task_does_not_use_model_full_clean(monkeypatch):
  def fail_full_clean(self, *args, **kwargs):
    pytest.fail("recurring operation validation should not use model full_clean")

  monkeypatch.setattr(RecurringTask, "full_clean", fail_full_clean)

  schedule_recurring_task(
    key="dynamic-task",
    task_path="tests.tasks.echo",
    schedule="* * * * *",
  )
