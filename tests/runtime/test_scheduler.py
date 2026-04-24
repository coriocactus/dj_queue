import time
from datetime import datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from django.utils import timezone

from dj_queue.api import schedule_recurring_task, unschedule_recurring_task
from dj_queue.models import FailedExecution, Job, Process, RecurringExecution, RecurringTask
from dj_queue.runtime.scheduler import Scheduler
from tests.tasks import echo

pytestmark = pytest.mark.django_db(transaction=True)


def scheduler_tasks_settings(
  *,
  recurring=None,
  dynamic_tasks_enabled=False,
  clear_finished_jobs_after=None,
  clear_failed_jobs_after=None,
  clear_recurring_executions_after=None,
  preserve_finished_jobs=True,
):
  return {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {
        "workers": [{"queues": "*", "threads": 1, "processes": 1, "polling_interval": 0.1}],
        "dispatchers": [],
        "scheduler": {
          "dynamic_tasks_enabled": dynamic_tasks_enabled,
          "polling_interval": 5,
        },
        "recurring": recurring or {},
        "clear_finished_jobs_after": clear_finished_jobs_after,
        "clear_failed_jobs_after": clear_failed_jobs_after,
        "clear_recurring_executions_after": clear_recurring_executions_after,
        "preserve_finished_jobs": preserve_finished_jobs,
      },
    }
  }


def build_scheduler(*, tasks_settings, name=None):
  return Scheduler.from_backend_config(
    backend_alias="default",
    tasks_settings=tasks_settings,
    name=name or f"scheduler-{uuid4()}",
    pid=34567,
    hostname="localhost",
  )


def fixed_now():
  return datetime(2026, 4, 8, 12, 0, 1, tzinfo=timezone.get_current_timezone())


def make_finished_job(*, finished_at, return_value, task=echo):
  return Job.objects.create(
    task_path=task.module_path,
    queue_name=task.queue_name,
    priority=task.priority,
    payload={"args": [], "kwargs": {}},
    backend_alias=task.backend,
    finished_at=finished_at,
    return_value=return_value,
  )


def test_scheduler_only_starts_when_it_has_work():
  scheduler = build_scheduler(
    tasks_settings=scheduler_tasks_settings(
      recurring={},
      dynamic_tasks_enabled=False,
      clear_finished_jobs_after=None,
      preserve_finished_jobs=False,
    )
  )

  assert scheduler is None


def test_scheduler_uses_configured_polling_interval():
  scheduler = build_scheduler(
    tasks_settings=scheduler_tasks_settings(
      recurring={
        "static-task": {
          "task_path": "tests.tasks.echo",
          "schedule": "* * * * *",
        }
      }
    )
  )

  assert scheduler.polling_interval == 5


@pytest.mark.parametrize("polling_interval", (None, 0, -1, "fast"))
def test_scheduler_uses_safe_polling_interval_when_config_is_missing_or_invalid(polling_interval):
  if polling_interval is None:
    scheduler_config = SimpleNamespace()
  else:
    scheduler_config = SimpleNamespace(polling_interval=polling_interval)

  scheduler = Scheduler(
    SimpleNamespace(
      scheduler=scheduler_config,
      recurring={},
      preserve_finished_jobs=False,
      clear_finished_jobs_after=None,
      clear_failed_jobs_after=None,
      clear_recurring_executions_after=None,
    ),
    backend_alias="default",
    name=f"scheduler-{uuid4()}",
    pid=34567,
    hostname="localhost",
  )

  assert scheduler.polling_interval == 1.0


def test_scheduler_persists_static_tasks():
  scheduler = build_scheduler(
    tasks_settings=scheduler_tasks_settings(
      recurring={
        "static-task": {
          "task_path": "tests.tasks.echo",
          "schedule": "* * * * *",
          "args": ["hello"],
          "queue_name": "maintenance",
        }
      }
    )
  )

  scheduler.sync_static_tasks()

  task = RecurringTask.objects.get(backend_alias="default", key="static-task")
  assert task.static is True
  assert task.payload == {"args": ["hello"], "kwargs": {}}
  assert task.queue_name == "maintenance"

  scheduler = build_scheduler(
    tasks_settings=scheduler_tasks_settings(
      recurring={
        "replacement-task": {
          "task_path": "tests.tasks.echo",
          "schedule": "*/5 * * * *",
        }
      }
    )
  )
  scheduler.sync_static_tasks()

  assert RecurringTask.objects.filter(backend_alias="default", key="static-task").exists() is False
  assert (
    RecurringTask.objects.filter(
      backend_alias="default",
      key="replacement-task",
      static=True,
    ).exists()
    is True
  )


def test_scheduler_static_sync_is_idempotent_when_unchanged():
  scheduler = build_scheduler(
    tasks_settings=scheduler_tasks_settings(
      recurring={
        "static-task": {
          "task_path": "tests.tasks.echo",
          "schedule": "* * * * *",
          "args": ["hello"],
          "queue_name": "maintenance",
        }
      }
    )
  )

  scheduler.sync_static_tasks()
  first_updated_at = RecurringTask.objects.get(
    backend_alias="default", key="static-task"
  ).updated_at

  time.sleep(0.01)
  scheduler.sync_static_tasks()

  assert (
    RecurringTask.objects.get(backend_alias="default", key="static-task").updated_at
    == first_updated_at
  )


def test_scheduler_fires_static_task():
  now = fixed_now()
  scheduler = build_scheduler(
    tasks_settings=scheduler_tasks_settings(
      recurring={
        "static-task": {
          "task_path": "tests.tasks.echo",
          "schedule": "* * * * *",
          "args": ["hello"],
        }
      }
    )
  )
  scheduler.start()

  fired_jobs = scheduler.poll_once(now=now)

  assert [job.payload for job in fired_jobs] == [{"args": ["hello"], "kwargs": {}}]
  assert RecurringExecution.objects.filter(backend_alias="default").count() == 1
  scheduler.stop()


def test_scheduler_dedup_across_instances():
  now = fixed_now()
  tasks_settings = scheduler_tasks_settings(
    recurring={
      "static-task": {
        "task_path": "tests.tasks.echo",
        "schedule": "* * * * *",
      }
    }
  )
  scheduler_one = build_scheduler(tasks_settings=tasks_settings, name="scheduler-one")
  scheduler_two = build_scheduler(tasks_settings=tasks_settings, name="scheduler-two")
  scheduler_one.start()
  scheduler_two.start()

  scheduler_one.poll_once(now=now)
  scheduler_two.poll_once(now=now)

  assert Job.objects.count() == 1
  assert RecurringExecution.objects.filter(backend_alias="default").count() == 1
  scheduler_one.stop()
  scheduler_two.stop()


def test_scheduler_picks_up_new_dynamic_task():
  now = fixed_now()
  scheduler = build_scheduler(tasks_settings=scheduler_tasks_settings(dynamic_tasks_enabled=True))
  scheduler.start()
  schedule_recurring_task(
    key="dynamic-task",
    task_path="tests.tasks.echo",
    schedule="* * * * *",
    args=("dynamic",),
  )

  fired_jobs = scheduler.poll_once(now=now)

  assert [job.payload for job in fired_jobs] == [{"args": ["dynamic"], "kwargs": {}}]
  scheduler.stop()


def test_scheduler_cancels_removed_dynamic_task():
  now = fixed_now()
  scheduler = build_scheduler(tasks_settings=scheduler_tasks_settings(dynamic_tasks_enabled=True))
  scheduler.start()
  schedule_recurring_task(
    key="dynamic-task",
    task_path="tests.tasks.echo",
    schedule="* * * * *",
  )
  unschedule_recurring_task("dynamic-task")

  fired_jobs = scheduler.poll_once(now=now)

  assert fired_jobs == []
  assert Job.objects.count() == 0
  scheduler.stop()


def test_scheduler_reschedules_changed_dynamic_task():
  now = fixed_now()
  scheduler = build_scheduler(tasks_settings=scheduler_tasks_settings(dynamic_tasks_enabled=True))
  scheduler.start()
  schedule_recurring_task(
    key="dynamic-task",
    task_path="tests.tasks.echo",
    schedule="0 0 1 1 *",
  )
  schedule_recurring_task(
    key="dynamic-task",
    task_path="tests.tasks.echo",
    schedule="* * * * *",
    args=("updated",),
  )

  fired_jobs = scheduler.poll_once(now=now)

  assert [job.payload for job in fired_jobs] == [{"args": ["updated"], "kwargs": {}}]
  assert (
    RecurringTask.objects.get(backend_alias="default", key="dynamic-task").schedule == "* * * * *"
  )
  scheduler.stop()


def test_internal_cleanup_runs_without_persisted_internal_recurring_task():
  scheduler = build_scheduler(
    tasks_settings=scheduler_tasks_settings(
      clear_finished_jobs_after=60, preserve_finished_jobs=True
    )
  )
  scheduler.start()

  scheduler.poll_once(now=fixed_now())

  assert RecurringTask.objects.filter(backend_alias="default").count() == 0
  scheduler.stop()


def test_internal_cleanup_deletes_only_old_finished_jobs():
  now = fixed_now()
  old_job = make_finished_job(
    finished_at=now - timedelta(minutes=10),
    return_value="old",
  )
  recent_job = make_finished_job(
    finished_at=now - timedelta(seconds=10),
    return_value="recent",
  )
  scheduler = build_scheduler(
    tasks_settings=scheduler_tasks_settings(
      clear_finished_jobs_after=60, preserve_finished_jobs=True
    )
  )
  scheduler.start()

  scheduler.poll_once(now=now)

  assert Job.objects.filter(pk=old_job.pk).exists() is False
  assert Job.objects.filter(pk=recent_job.pk).exists() is True
  scheduler.stop()


def test_internal_cleanup_deletes_old_failed_jobs_when_configured():
  now = fixed_now()
  old_job = Job.objects.create(
    task_path=echo.module_path,
    queue_name=echo.queue_name,
    priority=echo.priority,
    payload={"args": [], "kwargs": {}},
    backend_alias=echo.backend,
  )
  recent_job = Job.objects.create(
    task_path=echo.module_path,
    queue_name=echo.queue_name,
    priority=echo.priority,
    payload={"args": [], "kwargs": {}},
    backend_alias=echo.backend,
  )
  old_failed = FailedExecution.objects.create(
    job=old_job,
    exception_class="ValueError",
    message="old",
    traceback="old",
  )
  recent_failed = FailedExecution.objects.create(
    job=recent_job,
    exception_class="ValueError",
    message="recent",
    traceback="recent",
  )
  FailedExecution.objects.filter(pk=old_failed.pk).update(created_at=now - timedelta(minutes=10))
  FailedExecution.objects.filter(pk=recent_failed.pk).update(
    created_at=now - timedelta(seconds=10)
  )
  scheduler = build_scheduler(
    tasks_settings=scheduler_tasks_settings(
      clear_failed_jobs_after=60,
      preserve_finished_jobs=False,
    )
  )
  scheduler.start()

  scheduler.poll_once(now=now)

  assert Job.objects.filter(pk=old_job.pk).exists() is False
  assert Job.objects.filter(pk=recent_job.pk).exists() is True
  scheduler.stop()


def test_internal_cleanup_deletes_old_recurring_executions_when_configured():
  now = fixed_now()
  old_execution = RecurringExecution.objects.create(
    backend_alias="default",
    task_key="nightly",
    run_at=now - timedelta(minutes=10),
  )
  recent_execution = RecurringExecution.objects.create(
    backend_alias="default",
    task_key="nightly",
    run_at=now - timedelta(seconds=10),
  )
  scheduler = build_scheduler(
    tasks_settings=scheduler_tasks_settings(
      clear_recurring_executions_after=60,
      preserve_finished_jobs=False,
    )
  )
  scheduler.start()

  scheduler.poll_once(now=now)

  assert RecurringExecution.objects.filter(pk=old_execution.pk).exists() is False
  assert RecurringExecution.objects.filter(pk=recent_execution.pk).exists() is True
  scheduler.stop()


def test_scheduler_stop_cancels_timers_and_deregisters():
  scheduler = build_scheduler(
    tasks_settings=scheduler_tasks_settings(dynamic_tasks_enabled=True), name="scheduler-stop"
  )
  process = scheduler.start()

  scheduler.stop()

  assert Process.objects.filter(pk=process.pk).exists() is False
