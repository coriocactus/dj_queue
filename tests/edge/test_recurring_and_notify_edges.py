import json
from datetime import datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from django.db import transaction
from django.db.models.query import QuerySet
from django.utils import timezone

from dj_queue.api import unschedule_recurring_task
from dj_queue.cron import latest_cron_run
from dj_queue.models import Job, RecurringExecution, RecurringTask
from dj_queue.runtime.notify import (
  NoopWakeupBackend,
  NotifyWakeupBackend,
  build_wakeup_backend,
  notify_ready_queues,
)
from dj_queue.runtime.scheduler import Scheduler
from dj_queue.wakeup import notify_ready_queues_on_commit
from tests.tasks import echo


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
  assert (
    RecurringTask.objects.filter(
      backend_alias="default",
      key="static-task",
      static=True,
    ).exists()
    is True
  )


def test_ready_notification_waits_for_outer_transaction_commit(monkeypatch):
  notified = []

  def capture(queue_names, *, backend_alias="default"):
    notified.append((tuple(queue_names), backend_alias))

  monkeypatch.setattr("dj_queue.wakeup.supports_listen_notify", lambda alias: True)
  monkeypatch.setattr("dj_queue.runtime.notify.notify_ready_queues", capture)

  with transaction.atomic():
    echo.enqueue("deferred")
    assert notified == []

  assert notified == [(("default",), "default")]


def test_ready_notification_skips_on_commit_when_listen_notify_disabled(monkeypatch):
  on_commit_calls = []

  monkeypatch.setattr(
    "dj_queue.wakeup.load_backend_config",
    lambda backend_alias: SimpleNamespace(listen_notify=False, database_alias="default"),
  )
  monkeypatch.setattr("dj_queue.wakeup.supports_listen_notify", lambda alias: True)
  monkeypatch.setattr(
    "dj_queue.wakeup.transaction.on_commit",
    lambda func, *, using: on_commit_calls.append((func, using)),
  )

  notify_ready_queues_on_commit(("default",), backend_alias="default")

  assert on_commit_calls == []


def test_ready_notification_skips_on_commit_without_notify_support(monkeypatch):
  on_commit_calls = []

  monkeypatch.setattr(
    "dj_queue.wakeup.load_backend_config",
    lambda backend_alias: SimpleNamespace(listen_notify=True, database_alias="default"),
  )
  monkeypatch.setattr("dj_queue.wakeup.supports_listen_notify", lambda alias: False)
  monkeypatch.setattr(
    "dj_queue.wakeup.transaction.on_commit",
    lambda func, *, using: on_commit_calls.append((func, using)),
  )

  notify_ready_queues_on_commit(("default",), backend_alias="default")

  assert on_commit_calls == []


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
  recurring_task = RecurringTask.objects.get(backend_alias="default", key="static-task")
  run_at = latest_cron_run(recurring_task.schedule, now)
  RecurringExecution.objects.create(
    backend_alias="default",
    task_key=recurring_task.key,
    run_at=run_at,
    job=None,
  )

  monkeypatch.setattr(
    "dj_queue.operations.recurring.enqueue_job",
    lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not enqueue twice")),
  )

  fired_jobs = scheduler.poll_once(now=now)

  assert fired_jobs == []
  assert Job.objects.count() == 0
  assert (
    RecurringExecution.objects.filter(
      backend_alias="default",
      task_key="static-task",
      run_at=run_at,
    ).count()
    == 1
  )


def test_recurring_reservation_does_not_insert_when_already_reserved(monkeypatch):
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
  recurring_task = RecurringTask.objects.get(backend_alias="default", key="static-task")
  run_at = latest_cron_run(recurring_task.schedule, now)
  RecurringExecution.objects.create(
    backend_alias="default",
    task_key=recurring_task.key,
    run_at=run_at,
    job=None,
  )

  original_create = QuerySet.create

  def reject_duplicate_insert(queryset, *args, **kwargs):
    if queryset.model is RecurringExecution:
      raise AssertionError("existing recurring execution should be read, not inserted")
    return original_create(queryset, *args, **kwargs)

  monkeypatch.setattr(QuerySet, "create", reject_duplicate_insert)

  fired_jobs = scheduler.poll_once(now=now)

  assert fired_jobs == []
  assert Job.objects.count() == 0


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


def test_notify_stop_keeps_live_watcher_visible_until_it_exits():
  closed = []

  class FakeConnection:
    def close(self):
      closed.append(True)

  class LiveWatcher:
    def __init__(self):
      self.join_timeout = None

    def join(self, timeout=None):
      self.join_timeout = timeout

    def is_alive(self):
      return True

  backend = NotifyWakeupBackend(backend_alias="default", wake_up=lambda: None)
  watcher = LiveWatcher()
  backend._connection = FakeConnection()
  backend._watcher = watcher

  backend.stop(timeout=0.01)

  assert closed == [True]
  assert watcher.join_timeout == 0.01
  assert backend._watcher is watcher


def test_notify_watcher_suppresses_connection_close_errors_during_stop(monkeypatch):
  errors = []

  class ClosingConnection:
    def notifies(self, *, timeout, stop_after):
      def notifications():
        raise RuntimeError("connection closed")
        yield

      return notifications()

  monkeypatch.setattr(
    "dj_queue.runtime.notify.handle_thread_error",
    lambda error, **kwargs: errors.append((error, kwargs)),
  )
  backend = NotifyWakeupBackend(backend_alias="default", wake_up=lambda: None)
  backend._connection = ClosingConnection()
  backend._stop_event.set()

  backend._watch()

  assert errors == []


def test_notify_watcher_reconnects_after_connection_error(monkeypatch):
  errors = []
  wakes = []
  events = []
  closed = []

  class BrokenConnection:
    def notifies(self, *, timeout, stop_after):
      raise RuntimeError("notify disconnected")

    def close(self):
      closed.append("broken")

  class RestoredConnection:
    def notifies(self, *, timeout, stop_after):
      backend._stop_event.set()
      return [SimpleNamespace(payload=json.dumps(["alpha"]))]

  monkeypatch.setattr(
    "dj_queue.runtime.notify.handle_thread_error",
    lambda error, **kwargs: errors.append((error, kwargs)),
  )
  monkeypatch.setattr(
    "dj_queue.runtime.notify.log_event",
    lambda event, **kwargs: events.append((event, kwargs)),
  )
  backend = NotifyWakeupBackend(
    backend_alias="default",
    queues=("alpha",),
    wake_up=lambda: wakes.append("wake"),
    reconnect_base_delay=0,
  )
  backend._connection = BrokenConnection()
  monkeypatch.setattr(backend, "_open_connection", lambda: RestoredConnection())

  backend._watch()

  assert [(str(error), kwargs["context"]) for error, kwargs in errors] == [
    ("notify disconnected", "worker.notify")
  ]
  assert closed == ["broken"]
  assert wakes == ["wake"]
  assert backend.failed is False
  assert events == [("notify.restored", {"backend_alias": "default"})]


def test_notify_wakeup_backend_start_clears_prior_stop_event(monkeypatch):
  backend = NotifyWakeupBackend(backend_alias="default", wake_up=lambda: None)
  backend._stop_event.set()
  monkeypatch.setattr(backend, "_open_connection", lambda: object())
  monkeypatch.setattr(backend, "_start_watcher", lambda: None)

  backend.start()

  assert backend._stop_event.is_set() is False


def test_notify_ready_queues_sends_one_queue_payload(monkeypatch):
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

  assert len(sent) == 1
  channel, payload, backend_alias = sent[0]
  assert channel == "dj_queue_ready"
  assert json.loads(payload) == ["alpha", "beta"]
  assert backend_alias == "default"


def test_notify_ready_queues_reports_send_failures(monkeypatch):
  errors = []

  class BrokenConnection:
    def cursor(self):
      raise RuntimeError("notify failed")

  monkeypatch.setattr("dj_queue.runtime.notify.supports_listen_notify", lambda alias: True)
  monkeypatch.setattr(
    "dj_queue.runtime.notify.get_database_alias", lambda backend_alias: "default"
  )
  monkeypatch.setattr("dj_queue.runtime.notify.connections", {"default": BrokenConnection()})
  monkeypatch.setattr(
    "dj_queue.runtime.notify.handle_thread_error",
    lambda error, **kwargs: errors.append((error, kwargs)),
  )

  notify_ready_queues(("default",), backend_alias="default")

  assert [
    (str(error), kwargs["context"], kwargs["backend_alias"]) for error, kwargs in errors
  ] == [("notify failed", "producer.notify", "default")]


def test_notify_wakeup_backend_ignores_non_matching_queue_payload():
  wakes = []
  backend = NotifyWakeupBackend(
    backend_alias="default",
    queues=("alpha", "mail*"),
    wake_up=lambda: wakes.append("wake"),
  )

  class FakeConnection:
    def notifies(self, *, timeout, stop_after):
      backend._stop_event.set()
      return [SimpleNamespace(payload=json.dumps(["beta"]))]

  backend._connection = FakeConnection()
  backend._watch()

  assert wakes == []


def test_notify_wakeup_backend_wakes_matching_queue_payload():
  wakes = []
  backend = NotifyWakeupBackend(
    backend_alias="default",
    queues=("alpha", "mail*"),
    wake_up=lambda: wakes.append("wake"),
  )

  class FakeConnection:
    def notifies(self, *, timeout, stop_after):
      backend._stop_event.set()
      return [SimpleNamespace(payload=json.dumps(["mailers"]))]

  backend._connection = FakeConnection()
  backend._watch()

  assert wakes == ["wake"]
