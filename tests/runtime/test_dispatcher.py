from datetime import timedelta
from uuid import uuid4

import pytest
from django.utils import timezone

from dj_queue.config import DispatcherConfig
from dj_queue.models import (
  BlockedExecution,
  Job,
  Process,
  ReadyExecution,
  ScheduledExecution,
  Semaphore,
)
from dj_queue.runtime.dispatcher import Dispatcher
from tests.tasks import echo, limited

pytestmark = pytest.mark.django_db(transaction=True)


def make_scheduled_job(task=echo, *, scheduled_at=None, **overrides):
  if scheduled_at is None:
    scheduled_at = timezone.now() - timedelta(seconds=1)

  payload = {
    "args": list(overrides.pop("args", [])),
    "kwargs": dict(overrides.pop("kwargs", {})),
  }
  payload.update(overrides.pop("payload", {}))

  job = Job.objects.create(
    task_path=overrides.pop("task_path", task.module_path),
    queue_name=overrides.pop("queue_name", task.queue_name),
    priority=overrides.pop("priority", task.priority),
    payload=payload,
    backend_alias=overrides.pop("backend_alias", task.backend),
    scheduled_at=overrides.pop("job_scheduled_at", scheduled_at),
    concurrency_key=overrides.pop("concurrency_key", None),
    finished_at=overrides.pop("finished_at", None),
    return_value=overrides.pop("return_value", None),
    **overrides,
  )
  ScheduledExecution.objects.create(
    job=job,
    queue_name=job.queue_name,
    priority=job.priority,
    scheduled_at=scheduled_at,
  )
  return job


def make_dispatcher(config=None, **overrides):
  if config is None:
    config = DispatcherConfig(
      batch_size=500,
      polling_interval=1,
      concurrency_maintenance=True,
      concurrency_maintenance_interval=600,
    )
  return Dispatcher(
    config,
    backend_alias=overrides.pop("backend_alias", "default"),
    name=overrides.pop("name", f"dispatcher-{uuid4()}"),
    pid=overrides.pop("pid", 23456),
    hostname=overrides.pop("hostname", "localhost"),
  )


def test_dispatcher_registers_process_with_metadata():
  config = DispatcherConfig(
    batch_size=25,
    polling_interval=0.5,
    concurrency_maintenance=True,
    concurrency_maintenance_interval=30,
  )
  dispatcher = make_dispatcher(config=config, name="dispatcher-1", pid=201, hostname="host")

  process = dispatcher.start()

  assert process.backend_alias == "default"
  assert process.kind == "Dispatcher"
  assert process.name == "dispatcher-1"
  assert process.metadata == {
    "batch_size": 25,
    "polling_interval": 0.5,
    "concurrency_maintenance": True,
    "concurrency_maintenance_interval": 30,
  }

  dispatcher.stop()


def test_dispatcher_promotes_due_jobs():
  first = make_scheduled_job(args=["first"])
  second = make_scheduled_job(args=["second"])
  dispatcher = make_dispatcher(
    config=DispatcherConfig(
      batch_size=10,
      polling_interval=1,
      concurrency_maintenance=False,
      concurrency_maintenance_interval=600,
    )
  )
  dispatcher.start()

  promoted_jobs = dispatcher.poll_once()

  assert [job.id for job in promoted_jobs] == [first.id, second.id]
  assert ReadyExecution.objects.filter(job_id__in=[first.id, second.id]).count() == 2
  assert ScheduledExecution.objects.count() == 0
  dispatcher.stop()


def test_dispatcher_leaves_future_jobs_scheduled():
  future_job = make_scheduled_job(
    scheduled_at=timezone.now() + timedelta(minutes=5), args=["future"]
  )
  dispatcher = make_dispatcher(
    config=DispatcherConfig(
      batch_size=10,
      polling_interval=1,
      concurrency_maintenance=False,
      concurrency_maintenance_interval=600,
    )
  )
  dispatcher.start()

  promoted_jobs = dispatcher.poll_once()

  assert promoted_jobs == []
  assert ScheduledExecution.objects.filter(job=future_job).exists() is True
  assert ReadyExecution.objects.filter(job=future_job).exists() is False
  dispatcher.stop()


def test_dispatcher_respects_batch_size():
  jobs = [make_scheduled_job(args=[index]) for index in range(3)]
  dispatcher = make_dispatcher(
    config=DispatcherConfig(
      batch_size=2,
      polling_interval=1,
      concurrency_maintenance=False,
      concurrency_maintenance_interval=600,
    )
  )
  dispatcher.start()

  promoted_jobs = dispatcher.poll_once()

  assert [job.id for job in promoted_jobs] == [jobs[0].id, jobs[1].id]
  assert ReadyExecution.objects.count() == 2
  assert ScheduledExecution.objects.count() == 1
  dispatcher.stop()


def test_dispatcher_uses_concurrency_path_for_limited_jobs():
  limited.enqueue(1, value="first")
  scheduled_job = make_scheduled_job(
    task=limited,
    args=[1],
    kwargs={"value": "second"},
    concurrency_key="account:1",
  )
  dispatcher = make_dispatcher(
    config=DispatcherConfig(
      batch_size=10,
      polling_interval=1,
      concurrency_maintenance=False,
      concurrency_maintenance_interval=600,
    )
  )
  dispatcher.start()

  dispatcher.poll_once()

  scheduled_job.refresh_from_db()
  assert ReadyExecution.objects.filter(job=scheduled_job).exists() is False
  assert BlockedExecution.objects.filter(job=scheduled_job).exists() is True
  dispatcher.stop()


def test_dispatcher_cleans_expired_semaphores():
  Semaphore.objects.create(
    key="expired",
    value=0,
    limit=1,
    expires_at=timezone.now() - timedelta(seconds=1),
  )
  dispatcher = make_dispatcher()
  dispatcher.start()

  dispatcher.poll_once()

  assert Semaphore.objects.filter(key="expired").exists() is False
  dispatcher.stop()


def test_dispatcher_notifies_workers_when_rows_become_ready(monkeypatch):
  notified = []
  make_scheduled_job(args=["notify"])
  dispatcher = make_dispatcher(
    config=DispatcherConfig(
      batch_size=10,
      polling_interval=1,
      concurrency_maintenance=False,
      concurrency_maintenance_interval=600,
    )
  )
  dispatcher.start()

  def capture(queue_names, *, backend_alias="default"):
    notified.append((tuple(queue_names), backend_alias))

  monkeypatch.setattr("dj_queue.runtime.notify.notify_ready_queues", capture)

  dispatcher.poll_once()

  assert notified == [(("default",), "default")]
  dispatcher.stop()


def test_dispatcher_deregisters_on_stop():
  dispatcher = make_dispatcher(name="dispatcher-stop")
  process = dispatcher.start()

  dispatcher.stop()

  assert Process.objects.filter(pk=process.pk).exists() is False
