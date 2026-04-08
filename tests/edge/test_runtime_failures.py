from datetime import timedelta
from uuid import uuid4

import pytest
from django.utils import timezone

from dj_queue.config import load_backend_config
from dj_queue.exceptions import ProcessExitError, ProcessMissingError, ProcessPrunedError
from dj_queue.models import ClaimedExecution, FailedExecution, Job, Process, ReadyExecution
from dj_queue.runtime.supervisor import ForkSupervisor, Supervisor
from dj_queue.runtime.worker import Worker


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


def make_ready_job(**overrides):
  job = make_job(**overrides)
  ReadyExecution.objects.create(job=job, queue_name=job.queue_name, priority=job.priority)
  return job


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

  def shutdown(self, timeout):
    return True


class FakeSleeper:
  def wake_up(self):
    return None


class FakeWakeupBackend:
  def start(self):
    return None

  def stop(self):
    return None


def make_worker():
  return Worker(
    load_backend_config().workers[0],
    backend_alias="default",
    name=f"worker-{uuid4()}",
    pid=12345,
    hostname="localhost",
    sleeper=FakeSleeper(),
    pool=InlinePool(1),
    wakeup_backend=FakeWakeupBackend(),
  )


def make_supervisor(name=None):
  return Supervisor(
    load_backend_config(),
    name=name or f"supervisor-{uuid4()}",
    pid=54321,
    hostname="localhost",
    heartbeat_interval=0.01,
  )


def build_fork_supervisor(*, launcher=None, waitpid=None):
  tasks_settings = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {
        "mode": "fork",
        "workers": [{"queues": "*", "threads": 1, "processes": 1, "polling_interval": 0.1}],
        "dispatchers": [],
        "scheduler": None,
      },
    }
  }
  return ForkSupervisor.from_backend_config(
    backend_alias="default",
    tasks_settings=tasks_settings,
    name=f"fork-supervisor-{uuid4()}",
    pid=76543,
    hostname="localhost",
    launcher=launcher,
    waitpid=waitpid,
  )


def test_invalid_task_import_target_fails_as_failed_execution():
  job = make_ready_job(task_path="tests.tasks.missing")
  worker = make_worker()
  worker.start()

  worker.poll_once()

  failed_execution = FailedExecution.objects.get(job=job)
  assert failed_execution.exception_class in {"builtins.ImportError", "builtins.AttributeError"}
  assert (
    "No module named" in failed_execution.traceback
    or "has no attribute" in failed_execution.traceback
  )
  worker.stop()


def test_missing_process_row_on_startup_records_process_missing_error():
  process = make_process()
  job = make_job(task_path="tests.tasks.echo")
  ClaimedExecution.objects.create(job=job, process=process)
  process.delete()
  supervisor = make_supervisor()

  supervisor.start()

  failed_execution = FailedExecution.objects.get(job=job)
  assert failed_execution.exception_class == (
    f"{ProcessMissingError.__module__}.{ProcessMissingError.__qualname__}"
  )
  supervisor.stop()


def test_pruned_process_records_process_pruned_error():
  stale_process = make_process(last_heartbeat_at=timezone.now() - timedelta(minutes=10))
  stale_job = make_job(task_path="tests.tasks.echo")
  ClaimedExecution.objects.create(job=stale_job, process=stale_process)
  supervisor = make_supervisor()
  supervisor.start()

  supervisor.prune_stale_process_rows(now=timezone.now())

  failed_execution = FailedExecution.objects.get(job=stale_job)
  assert failed_execution.exception_class == (
    f"{ProcessPrunedError.__module__}.{ProcessPrunedError.__qualname__}"
  )
  supervisor.stop()


def test_unexpected_child_exit_records_process_exit_error():
  launched = []
  waitpid_results = [(90001, 0)]

  def launcher(spec):
    launched.append(spec)
    return 90000 + len(launched)

  def waitpid(_pid, _flags):
    if waitpid_results:
      return waitpid_results.pop(0)
    raise ChildProcessError

  supervisor = build_fork_supervisor(launcher=launcher, waitpid=waitpid)
  supervisor.start()
  child_process = make_process(pid=90001, name="worker-1")
  job = make_job(task_path="tests.tasks.echo")
  ClaimedExecution.objects.create(job=job, process=child_process)

  supervisor.check_children()

  failed_execution = FailedExecution.objects.get(job=job)
  assert failed_execution.exception_class == (
    f"{ProcessExitError.__module__}.{ProcessExitError.__qualname__}"
  )
