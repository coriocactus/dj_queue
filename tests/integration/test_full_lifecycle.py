from datetime import datetime, timedelta
from uuid import uuid4

import pytest
from django.utils import timezone

from dj_queue.api import schedule_recurring_task
from dj_queue.config import DispatcherConfig, WorkerConfig
from dj_queue.models import Job, RecurringExecution
from dj_queue.runtime.dispatcher import Dispatcher
from dj_queue.runtime.scheduler import Scheduler
from dj_queue.runtime.worker import Worker
from tests.tasks import echo, limited, other_queue


pytestmark = pytest.mark.django_db(transaction=True)


class InlinePool:
  def __init__(self, max_workers):
    self.idle_capacity = max_workers

  def submit(self, fn, *args, **kwargs):
    from concurrent.futures import Future

    future = Future()
    try:
      future.set_result(fn(*args, **kwargs))
    except Exception as exc:
      future.set_exception(exc)
    return future

  def shutdown(self, timeout, *, on_drained=None):
    if on_drained is not None:
      on_drained()
    return True


class FakeSleeper:
  def wake_up(self):
    return None


class FakeWakeupBackend:
  def start(self):
    return None

  def stop(self):
    return None


def fixed_now():
  return datetime(2026, 4, 8, 12, 0, 1, tzinfo=timezone.get_current_timezone())


def make_worker(*, config=None, name="worker-1"):
  if config is None:
    config = WorkerConfig(queues=("*",), threads=1, processes=1, polling_interval=0.1)
  return Worker(
    config,
    backend_alias="default",
    name=name,
    pid=12345,
    hostname="localhost",
    sleeper=FakeSleeper(),
    pool=InlinePool(1),
    wakeup_backend=FakeWakeupBackend(),
  )


def make_dispatcher(name="dispatcher-1"):
  return Dispatcher(
    DispatcherConfig(
      batch_size=10,
      polling_interval=1,
      concurrency_maintenance=True,
      concurrency_maintenance_interval=600,
    ),
    backend_alias="default",
    name=name,
    pid=23456,
    hostname="localhost",
  )


def make_scheduler(tasks_settings, name="scheduler-1"):
  return Scheduler.from_backend_config(
    backend_alias="default",
    tasks_settings=tasks_settings,
    name=name,
    pid=34567,
    hostname="localhost",
  )


def lifecycle_tasks_settings():
  return {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {
        "workers": [{"queues": "*", "threads": 1, "processes": 1, "polling_interval": 0.1}],
        "dispatchers": [
          {
            "batch_size": 10,
            "polling_interval": 1,
            "concurrency_maintenance": True,
            "concurrency_maintenance_interval": 600,
          }
        ],
        "scheduler": {
          "dynamic_tasks_enabled": True,
          "polling_interval": 5,
        },
        "recurring": {},
      },
    }
  }


def test_full_lifecycle_mixed_queues_scheduled_blocked_and_recurring(monkeypatch):
  now = fixed_now()
  future_at = timezone.now() + timedelta(minutes=1)
  tasks_settings = lifecycle_tasks_settings()
  worker = make_worker(
    config=WorkerConfig(
      queues=("other", "default"),
      threads=1,
      processes=1,
      polling_interval=0.1,
    ),
    name=f"worker-{uuid4()}",
  )
  dispatcher = make_dispatcher(name=f"dispatcher-{uuid4()}")
  scheduler = make_scheduler(tasks_settings, name=f"scheduler-{uuid4()}")
  assert scheduler is not None

  ready = echo.using(priority=0).enqueue("ready-now")
  future = echo.using(priority=15, run_after=future_at).enqueue("scheduled-later")
  other = other_queue.using(priority=20).enqueue("ready-other")
  first_limited = limited.using(priority=10).enqueue(1, value="first")
  blocked = limited.using(priority=5).enqueue(1, value="blocked-second")
  schedule_recurring_task(
    key="dynamic-task",
    task_path="tests.tasks.echo",
    schedule="* * * * *",
    args=("from-recurring",),
    queue_name="other",
    priority=30,
  )

  worker.start()
  dispatcher.start()
  scheduler.start()

  try:
    future_job = Job.objects.get(pk=future.id)
    blocked_job = Job.objects.get(pk=blocked.id)
    assert future_job.scheduled is True
    assert blocked_job.blocked is True

    recurring_jobs = scheduler.poll_once(now=now)
    recurring_job = recurring_jobs[0]
    assert recurring_job.ready is True

    worker.poll_once()
    recurring_job.refresh_from_db()
    ready_job = Job.objects.get(pk=ready.id)
    assert recurring_job.return_value == "from-recurring"
    assert ready_job.return_value is None

    worker.poll_once()
    other_job = Job.objects.get(pk=other.id)
    assert other_job.return_value == "ready-other"

    worker.poll_once()
    first_limited_job = Job.objects.get(pk=first_limited.id)
    blocked_job.refresh_from_db()
    assert first_limited_job.return_value == "first"
    assert blocked_job.ready is True

    promoted_at = future_at + timedelta(seconds=1)
    with monkeypatch.context() as mp:
      mp.setattr("dj_queue.operations.jobs.timezone.now", lambda: promoted_at)
      dispatcher.poll_once()

    future_job.refresh_from_db()
    assert future_job.ready is True

    worker.poll_once()
    worker.poll_once()
    worker.poll_once()

    ready_job.refresh_from_db()
    future_job.refresh_from_db()
    blocked_job.refresh_from_db()
    recurring_job.refresh_from_db()

    assert ready_job.return_value == "ready-now"
    assert future_job.return_value == "scheduled-later"
    assert blocked_job.return_value == "blocked-second"
    assert recurring_job.return_value == "from-recurring"
    assert RecurringExecution.objects.filter(job=recurring_job).exists() is True
  finally:
    worker.stop()
    dispatcher.stop()
    scheduler.stop()
