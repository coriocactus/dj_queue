from datetime import timedelta
import time
from uuid import uuid4

import pytest
from django.utils import timezone

from dj_queue.config import load_backend_config
from dj_queue.exceptions import ProcessMissingError, ProcessPrunedError
from dj_queue.models import ClaimedExecution, FailedExecution, Job, Process
from dj_queue.runtime.supervisor import AsyncSupervisor, Supervisor
from tests.tasks import limited

pytestmark = pytest.mark.django_db(transaction=True)


def make_job(**overrides):
  payload = {
    "args": list(overrides.pop("args", [])),
    "kwargs": dict(overrides.pop("kwargs", {})),
  }
  payload.update(overrides.pop("payload", {}))

  return Job.objects.create(
    task_path=overrides.pop("task_path", "tests.tasks.echo"),
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


def make_process(**overrides):
  return Process.objects.create(
    kind=overrides.pop("kind", "Worker"),
    pid=overrides.pop("pid", 12345),
    hostname=overrides.pop("hostname", "localhost"),
    name=overrides.pop("name", f"worker-{uuid4()}"),
    metadata=overrides.pop("metadata", {}),
    last_heartbeat_at=overrides.pop("last_heartbeat_at", timezone.now()),
    **overrides,
  )


def make_supervisor(name=None):
  return Supervisor(
    load_backend_config(),
    name=name or f"supervisor-{uuid4()}",
    pid=54321,
    hostname="localhost",
    heartbeat_interval=0.01,
  )


def async_tasks_settings(*, workers=None, dispatchers=None, recurring=None):
  return {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {
        "mode": "async",
        "workers": workers
        or [{"queues": "*", "threads": 1, "processes": 1, "polling_interval": 0.1}],
        "dispatchers": dispatchers
        or [{"batch_size": 10, "polling_interval": 1, "concurrency_maintenance": False}],
        "scheduler": {"dynamic_tasks_enabled": False, "polling_interval": 5},
        "recurring": recurring or {},
        "preserve_finished_jobs": False,
        "clear_finished_jobs_after": None,
      },
    }
  }


def build_async_supervisor(*, tasks_settings, standalone=True, name=None):
  return AsyncSupervisor.from_backend_config(
    backend_alias="default",
    tasks_settings=tasks_settings,
    name=name or f"async-supervisor-{uuid4()}",
    pid=65432,
    hostname="localhost",
    standalone=standalone,
  )


def wait_until(predicate, timeout=1):
  deadline = time.monotonic() + timeout
  while time.monotonic() < deadline:
    if predicate():
      return
    time.sleep(0.01)
  assert predicate()


def test_startup_orphan_cleanup_fails_leftover_claimed_jobs():
  process = make_process()
  job = make_job(
    task_path="tests.tasks.limited",
    args=[1],
    kwargs={"value": "first"},
    concurrency_key="account:1",
  )
  ClaimedExecution.objects.create(job=job, process=process)
  process.delete()
  waiting_job = limited.enqueue(1, value="second")
  supervisor = make_supervisor()

  supervisor.start()

  failed_execution = FailedExecution.objects.get(job=job)
  assert failed_execution.exception_class == (
    f"{ProcessMissingError.__module__}.{ProcessMissingError.__qualname__}"
  )
  assert ClaimedExecution.objects.filter(job=job).exists() is False
  assert Job.objects.filter(pk=waiting_job.id, ready_execution__isnull=False).exists() is True
  supervisor.stop()


def test_prune_stale_process_rows_fails_their_claimed_jobs():
  stale_process = make_process(
    name="stale-worker",
    last_heartbeat_at=timezone.now() - timedelta(minutes=10),
  )
  fresh_process = make_process(name="fresh-worker")
  stale_job = make_job(task_path="tests.tasks.echo")
  fresh_job = make_job(task_path="tests.tasks.echo")
  ClaimedExecution.objects.create(job=stale_job, process=stale_process)
  ClaimedExecution.objects.create(job=fresh_job, process=fresh_process)
  supervisor = make_supervisor()
  supervisor.start()

  pruned = supervisor.prune_stale_process_rows(now=timezone.now())

  assert [process.name for process in pruned] == ["stale-worker"]
  assert FailedExecution.objects.get(job=stale_job).exception_class == (
    f"{ProcessPrunedError.__module__}.{ProcessPrunedError.__qualname__}"
  )
  assert Process.objects.filter(name="stale-worker").exists() is False
  assert ClaimedExecution.objects.filter(job=fresh_job, process=fresh_process).exists() is True
  supervisor.stop()


def test_async_supervisor_starts_configured_runners_in_one_pid():
  supervisor = build_async_supervisor(
    tasks_settings=async_tasks_settings(
      recurring={
        "static-task": {
          "task_path": "tests.tasks.echo",
          "schedule": "* * * * *",
        }
      }
    )
  )

  process = supervisor.start()

  wait_until(lambda: Process.objects.filter(supervisor=process).count() == 3)
  children = list(Process.objects.filter(supervisor=process).order_by("kind", "name"))

  assert {child.kind for child in children} == {"Worker", "Dispatcher", "Scheduler"}
  assert {child.pid for child in children} == {process.pid}
  assert len({child.name for child in children}) == 3
  supervisor.stop()


def test_async_mode_ignores_processes_greater_than_one():
  with pytest.warns(UserWarning, match="ignores worker processes > 1"):
    supervisor = build_async_supervisor(
      tasks_settings=async_tasks_settings(
        workers=[{"queues": "*", "threads": 1, "processes": 3, "polling_interval": 0.1}]
      )
    )

  process = supervisor.start()

  wait_until(lambda: Process.objects.filter(supervisor=process, kind="Worker").count() == 1)
  assert Process.objects.filter(supervisor=process, kind="Worker").count() == 1
  supervisor.stop()


def test_standalone_supervisor_registers_self_and_children():
  supervisor = build_async_supervisor(tasks_settings=async_tasks_settings(), standalone=True)

  process = supervisor.start()

  wait_until(lambda: Process.objects.filter(supervisor=process).count() == 2)
  assert Process.objects.filter(pk=process.pk, kind="Supervisor").exists() is True
  assert Process.objects.filter(supervisor=process).count() == 2
  supervisor.stop()


def test_embedded_async_supervisor_does_not_register_signal_handlers(monkeypatch):
  supervisor = build_async_supervisor(tasks_settings=async_tasks_settings(), standalone=False)
  called = []

  monkeypatch.setattr(supervisor, "register_signal_handlers", lambda: called.append(True))
  process = supervisor.start()

  wait_until(lambda: Process.objects.filter(supervisor=process).count() == 2)
  assert called == []
  supervisor.stop()
