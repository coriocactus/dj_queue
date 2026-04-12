from datetime import datetime
from uuid import uuid4

import pytest
from django.utils import timezone

from dj_queue.api import unschedule_recurring_task
from dj_queue.models import Job, RecurringExecution, RecurringTask
from dj_queue.runtime.notify import (
  NoopWakeupBackend,
  NotifyWakeupBackend,
  READY_PAYLOAD,
  build_wakeup_backend,
  notify_ready_queues,
)
from dj_queue.runtime.scheduler import Scheduler, _latest_run_at


pytestmark = pytest.mark.django_db(transaction=True)


def fixed_now():
  return datetime(2026, 4, 8, 12, 0, 1, tzinfo=timezone.get_current_timezone())


def scheduler_tasks_settings(*, recurring=None, dynamic_tasks_enabled=False, listen_notify=True):
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
        "listen_notify": listen_notify,
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


def test_static_recurring_task_cannot_be_unscheduled_via_dynamic_api():
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
  scheduler.sync_static_tasks()

  deleted = unschedule_recurring_task("static-task")

  assert deleted == 0
  assert RecurringTask.objects.filter(key="static-task", static=True).exists() is True


def test_recurring_reservation_without_job_backfill_does_not_double_fire(monkeypatch):
  now = fixed_now()
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
  scheduler.sync_static_tasks()
  recurring_task = RecurringTask.objects.get(key="static-task")
  run_at = _latest_run_at(recurring_task.schedule, now)
  RecurringExecution.objects.create(task_key=recurring_task.key, run_at=run_at, job=None)

  monkeypatch.setattr(
    "dj_queue.operations.recurring.enqueue_job",
    lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not enqueue twice")),
  )

  fired_jobs = scheduler.poll_once(now=now)

  assert fired_jobs == []
  assert Job.objects.count() == 0
  assert RecurringExecution.objects.filter(task_key="static-task", run_at=run_at).count() == 1


def test_listen_notify_ignored_on_non_postgres_backends(settings):
  settings.TASKS = {
    **settings.TASKS,
    "default": {
      **settings.TASKS["default"],
      "OPTIONS": {
        **settings.TASKS["default"]["OPTIONS"],
        "listen_notify": False,
      },
    },
  }

  backend = build_wakeup_backend(backend_alias="default", queues=("*",), wake_up=lambda: None)

  assert isinstance(backend, NoopWakeupBackend)


def test_notify_connection_uses_django_backend_connection_params(monkeypatch):
  connect_calls = []
  executed = []

  class FakeCursor:
    def __enter__(self):
      return self

    def __exit__(self, exc_type, exc, tb):
      return False

    def execute(self, sql):
      executed.append(sql)

  class FakeConnection:
    def __init__(self):
      self.autocommit = False

    def cursor(self):
      return FakeCursor()

    def close(self):
      return None

  class FakeDatabase:
    @staticmethod
    def connect(**params):
      connect_calls.append(params)
      return FakeConnection()

  class FakeWrapper:
    Database = FakeDatabase

    def ensure_connection(self):
      return None

    def get_connection_params(self):
      return {"dbname": "queue", "sslmode": "require", "service": "primary"}

  monkeypatch.setattr("dj_queue.runtime.notify.get_database_alias", lambda backend_alias: "queue")
  monkeypatch.setattr("dj_queue.runtime.notify.connections", {"queue": FakeWrapper()})

  backend = NotifyWakeupBackend(backend_alias="default", wake_up=lambda: None)

  connection = backend._open_connection()

  assert connect_calls == [{"dbname": "queue", "sslmode": "require", "service": "primary"}]
  assert connection.autocommit is True
  assert executed == ["LISTEN dj_queue_ready"]


@pytest.mark.postgres
def test_notify_watcher_shutdown_is_clean():
  backend = build_wakeup_backend(backend_alias="default", queues=("*",), wake_up=lambda: None)
  assert isinstance(backend, NotifyWakeupBackend)

  backend.start()
  watcher = backend._watcher
  backend.stop()

  assert watcher is not None
  assert watcher.is_alive() is False


def test_notify_ready_queues_sends_one_generic_wakeup(monkeypatch):
  sent = []

  monkeypatch.setattr("dj_queue.runtime.notify.supports_listen_notify", lambda alias: True)
  monkeypatch.setattr(
    "dj_queue.runtime.notify.get_database_alias", lambda backend_alias: "default"
  )
  monkeypatch.setattr(
    "dj_queue.runtime.notify._notify",
    lambda channel, payload, *, backend_alias: sent.append((channel, payload, backend_alias)),
  )

  notify_ready_queues(("alpha", "alpha", "beta"), backend_alias="default")

  assert sent == [("dj_queue_ready", READY_PAYLOAD, "default")]
