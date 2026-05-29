import threading
import time
from concurrent.futures import Future
from contextlib import contextmanager
from uuid import uuid4

import pytest
from django.utils import timezone

from dj_queue.config import WorkerConfig
from dj_queue.exceptions import ProcessExitError
from dj_queue.models import ClaimedExecution, FailedExecution, Job, Process, ReadyExecution
from dj_queue.operations.jobs import (
  ClaimedJob,
  claim_ready_jobs,
  complete_claimed_job,
  execute_claimed_job,
)
from dj_queue.runtime.worker import Worker
from tests.tasks import echo, fail, non_json_result, with_context

pytestmark = pytest.mark.django_db(transaction=True)


class InlinePool:
  def __init__(self, max_workers):
    self.max_workers = max_workers
    self.idle_capacity = max_workers
    self.shutdown_timeout = None

  def submit(self, fn, *args, **kwargs):
    future = Future()
    try:
      future.set_result(fn(*args, **kwargs))
    except Exception as exc:
      future.set_exception(exc)
    return future

  def shutdown(self, timeout, *, on_drained=None):
    self.shutdown_timeout = timeout
    if on_drained is not None:
      on_drained()
    return True


class FakeSleeper:
  def __init__(self):
    self.wake_count = 0

  def wake_up(self):
    self.wake_count += 1


class FakeWakeupBackend:
  def __init__(self):
    self.started = 0
    self.stopped = 0
    self.stop_timeout = None

  def start(self):
    self.started += 1

  def stop(self, *, timeout=None):
    self.stopped += 1
    self.stop_timeout = timeout


class FailingSubmitPool:
  max_workers = 1
  idle_capacity = 1

  def submit(self, *_args, **_kwargs):
    raise RuntimeError("pool closed")

  def shutdown(self, timeout, *, on_drained=None):
    if on_drained is not None:
      on_drained()
    return True


def make_ready_job(task=echo, **overrides):
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
    scheduled_at=overrides.pop("scheduled_at", None),
    concurrency_key=overrides.pop("concurrency_key", None),
    finished_at=overrides.pop("finished_at", None),
    return_value=overrides.pop("return_value", None),
    **overrides,
  )
  ReadyExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
  )
  return job


def wait_until(predicate, timeout=1):
  deadline = time.monotonic() + timeout
  while time.monotonic() < deadline:
    if predicate():
      return
    time.sleep(0.01)
  assert predicate()


def make_worker(config=None, **overrides):
  if config is None:
    config = WorkerConfig(queues=("*",), threads=1, processes=1, polling_interval=0.1)
  return Worker(
    config,
    backend_alias=overrides.pop("backend_alias", "default"),
    name=overrides.pop("name", f"worker-{uuid4()}"),
    pid=overrides.pop("pid", 12345),
    hostname=overrides.pop("hostname", "localhost"),
    sleeper=overrides.pop("sleeper", FakeSleeper()),
    pool=overrides.pop("pool", InlinePool(config.threads)),
    wakeup_backend=overrides.pop("wakeup_backend", FakeWakeupBackend()),
  )


def test_worker_future_handler_uses_app_executor_only_for_errors(monkeypatch):
  events = []

  @contextmanager
  def executor():
    events.append("enter")
    yield
    events.append("exit")

  monkeypatch.setattr("dj_queue.runtime.worker.app_executor", executor)
  monkeypatch.setattr(
    "dj_queue.runtime.worker.handle_thread_error",
    lambda error, **kwargs: events.append(str(error)),
  )
  future = Future()
  future.set_exception(RuntimeError("boom"))

  make_worker()._handle_future(future)

  assert events == ["enter", "boom", "exit"]


def test_worker_future_handler_skips_app_executor_on_success(monkeypatch):
  events = []

  @contextmanager
  def executor():
    events.append("enter")
    yield
    events.append("exit")

  monkeypatch.setattr("dj_queue.runtime.worker.app_executor", executor)
  future = Future()
  future.set_result(None)

  make_worker()._handle_future(future)

  assert events == []


def test_worker_registers_process_with_metadata():
  config = WorkerConfig(queues=("alpha", "beta*"), threads=2, processes=1, polling_interval=0.25)
  worker = make_worker(config=config, name="worker-1", pid=101, hostname="host")

  process = worker.start()

  assert process.backend_alias == "default"
  assert process.kind == "Worker"
  assert process.name == "worker-1"
  assert process.pid == 101
  assert process.hostname == "host"
  assert process.metadata == {
    "queues": ["alpha", "beta*"],
    "threads": 2,
    "polling_interval": 0.25,
  }

  worker.stop()
  assert Process.objects.filter(pk=process.pk).exists() is False


def test_worker_claims_highest_priority_first():
  low = make_ready_job(args=["low"], priority=0)
  high = make_ready_job(args=["high"], priority=10)
  worker = make_worker()
  worker.start()

  claimed_jobs = worker.poll_once()

  assert [claimed_job.job.id for claimed_job in claimed_jobs] == [high.id]
  assert ClaimedExecution.objects.filter(job=high).exists() is False
  assert ReadyExecution.objects.filter(job=low).exists() is True
  worker.stop()


def test_worker_respects_ordered_queue_list():
  alpha = make_ready_job(queue_name="alpha", priority=0)
  make_ready_job(queue_name="beta", priority=10)
  worker = make_worker(
    config=WorkerConfig(queues=("alpha", "beta"), threads=1, processes=1, polling_interval=0.1)
  )
  worker.start()

  claimed_jobs = worker.poll_once()

  assert [claimed_job.job.id for claimed_job in claimed_jobs] == [alpha.id]
  worker.stop()


def test_worker_executes_success_path():
  job = make_ready_job(args=["done"])
  worker = make_worker()
  worker.start()

  worker.poll_once()

  fresh_job = Job.objects.get(pk=job.pk)
  assert fresh_job.finished_at is not None
  assert fresh_job.return_value == "done"
  assert ClaimedExecution.objects.filter(job=job).exists() is False
  worker.stop()


def test_worker_fails_claimed_job_when_submit_fails():
  job = make_ready_job(args=["submit-fails"])
  worker = make_worker(pool=FailingSubmitPool())
  worker.start()

  submitted = worker.poll_once()

  assert submitted == []
  assert ClaimedExecution.objects.filter(job=job).exists() is False
  failed_execution = FailedExecution.objects.get(job=job)
  assert failed_execution.exception_class == (
    f"{ProcessExitError.__module__}.{ProcessExitError.__qualname__}"
  )
  worker.stop()


def test_worker_fails_claimed_job_when_execution_future_raises():
  job = make_ready_job(args=["future-fails"])
  worker = make_worker()
  worker.start()
  claimed_job = claim_ready_jobs(limit=1, process=worker.process)[0]
  future = Future()
  future.set_exception(RuntimeError("persist failed"))

  worker._handle_future(future, claimed_job)

  assert ClaimedExecution.objects.filter(job=job).exists() is False
  failed_execution = FailedExecution.objects.get(job=job)
  assert failed_execution.message == "persist failed"
  worker.stop()


def test_worker_executes_already_claimed_job_object(monkeypatch):
  job = make_ready_job(args=["claimed-object"])
  seen = []
  worker = make_worker()
  worker.start()

  def execute_job(claimed_job, *, backend_alias):
    seen.append((claimed_job, backend_alias))

  monkeypatch.setattr("dj_queue.runtime.worker.execute_claimed_job", execute_job)

  worker.poll_once()

  assert [(claimed_job.job.id, backend_alias) for claimed_job, backend_alias in seen] == [
    (job.id, "default")
  ]
  assert isinstance(seen[0][0], ClaimedJob)
  worker.stop()


def test_claim_ready_jobs_returns_claim_metadata():
  process = Process.objects.create(
    backend_alias="default",
    kind="Worker",
    pid=12345,
    hostname="localhost",
    name="worker-claim-metadata",
    metadata={},
    last_heartbeat_at=timezone.now(),
  )
  job = make_ready_job(args=["claim-metadata"])

  claimed_jobs = claim_ready_jobs(limit=1, process=process)

  assert [claimed_job.job.id for claimed_job in claimed_jobs] == [job.id]
  assert claimed_jobs[0].claimed_at is not None
  assert claimed_jobs[0].worker_ids == (process.name,)


def test_execute_claimed_job_completes_already_loaded_job_object(monkeypatch):
  job = make_ready_job(args=["complete-object"])
  process = Process.objects.create(
    backend_alias="default",
    kind="Worker",
    pid=12345,
    hostname="localhost",
    name="worker-complete-object",
    metadata={},
    last_heartbeat_at=timezone.now(),
  )
  ReadyExecution.objects.filter(job=job).delete()
  ClaimedExecution.objects.create(job=job, process=process)
  seen = []

  def complete_job(claimed_job, return_value, *, backend_alias, task=None):
    seen.append((claimed_job, return_value, backend_alias))

  monkeypatch.setattr("dj_queue.operations.jobs._complete_claimed_job", complete_job)

  execute_claimed_job(ClaimedJob(job=job, claimed_at=timezone.now(), worker_ids=(process.name,)))

  assert [
    (claimed_job.id, return_value, backend_alias)
    for claimed_job, return_value, backend_alias in seen
  ] == [(job.id, "complete-object", "default")]
  assert isinstance(seen[0][0], Job)


def test_complete_claimed_job_uses_loaded_job_without_select_related(monkeypatch):
  job = make_ready_job(args=["done"])
  process = Process.objects.create(
    backend_alias="default",
    kind="Worker",
    pid=12345,
    hostname="localhost",
    name="worker-no-reread-complete",
    metadata={},
    last_heartbeat_at=timezone.now(),
  )
  ReadyExecution.objects.filter(job=job).delete()
  ClaimedExecution.objects.create(job=job, process=process)

  def fail_select_related(self, *args, **kwargs):
    raise AssertionError("loaded-job completion should not select_related the job")

  monkeypatch.setattr("django.db.models.query.QuerySet.select_related", fail_select_related)

  complete_claimed_job(job, "done")

  fresh_job = Job.objects.get(pk=job.pk)
  assert fresh_job.return_value == "done"
  assert ClaimedExecution.objects.filter(job=job).exists() is False


def test_worker_executes_failure_path():
  job = make_ready_job(task=fail, args=["boom"])
  worker = make_worker()
  worker.start()

  worker.poll_once()

  failed_execution = FailedExecution.objects.get(job=job)
  assert failed_execution.message == "boom"
  assert "ValueError" in failed_execution.traceback
  assert ClaimedExecution.objects.filter(job=job).exists() is False
  worker.stop()


def test_worker_fails_non_json_result_instead_of_stranding_claim():
  job = make_ready_job(task=non_json_result)
  worker = make_worker()
  worker.start()

  worker.poll_once()

  failed_execution = FailedExecution.objects.get(job=job)
  assert failed_execution.message == "return value must be JSON round-trippable"
  assert ClaimedExecution.objects.filter(job=job).exists() is False
  assert Job.objects.get(pk=job.pk).finished_at is None
  worker.stop()


def test_worker_provides_task_context():
  job = make_ready_job(task=with_context, args=["ctx"])
  worker = make_worker()
  worker.start()

  worker.poll_once()

  fresh_job = Job.objects.get(pk=job.pk)
  assert fresh_job.return_value == {
    "job_id": str(job.id),
    "attempt": 1,
    "value": "ctx",
  }
  worker.stop()


def test_worker_preserves_finished_job_when_enabled():
  job = make_ready_job(args=["keep"])
  worker = make_worker()
  worker.start()

  worker.poll_once()

  assert Job.objects.filter(pk=job.pk).exists() is True
  assert Job.objects.get(pk=job.pk).finished_at is not None
  worker.stop()


def test_worker_deletes_finished_job_when_preserve_disabled(settings):
  settings.TASKS = {
    **settings.TASKS,
    "default": {
      **settings.TASKS["default"],
      "OPTIONS": {
        **settings.TASKS["default"]["OPTIONS"],
        "preserve_finished_jobs": False,
      },
    },
  }
  job = make_ready_job(args=["gone"])
  worker = make_worker()
  worker.start()

  worker.poll_once()

  assert Job.objects.filter(pk=job.pk).exists() is False
  worker.stop()


def test_worker_repolls_immediately_when_pool_frees_capacity():
  sleeper = FakeSleeper()
  worker = Worker(
    WorkerConfig(queues=("*",), threads=1, processes=1, polling_interval=0.1),
    name="worker-repoll",
    pid=12345,
    hostname="localhost",
    sleeper=sleeper,
  )
  make_ready_job(args=["wake"])
  worker.start()
  worker._execute_job = lambda job_id: time.sleep(0.05)

  worker.poll_once()
  wait_until(lambda: sleeper.wake_count > 0)

  worker.stop()


def test_worker_uses_wakeup_backend_without_changing_correctness():
  job = make_ready_job(args=["wakeup"])
  wakeup_backend = FakeWakeupBackend()
  worker = make_worker(wakeup_backend=wakeup_backend)

  worker.start()
  worker.poll_once()
  worker.stop()

  assert wakeup_backend.started == 1
  assert wakeup_backend.stopped == 1
  assert Job.objects.get(pk=job.pk).return_value == "wakeup"


def test_worker_graceful_shutdown_drains_pool():
  worker = Worker(
    WorkerConfig(queues=("*",), threads=1, processes=1, polling_interval=0.1),
    name="worker-shutdown",
    pid=12345,
    hostname="localhost",
  )
  make_ready_job(args=["slow"])
  worker.start()
  worker._execute_job = lambda job_id: time.sleep(0.05)

  worker.poll_once()
  started_at = time.monotonic()
  drained = worker.stop(timeout=1)
  elapsed = time.monotonic() - started_at

  assert drained is True
  assert elapsed >= 0.04
  assert Process.objects.filter(name="worker-shutdown").exists() is False


def test_worker_timeout_shutdown_keeps_process_until_work_finishes():
  release = threading.Event()
  worker = Worker(
    WorkerConfig(queues=("*",), threads=1, processes=1, polling_interval=0.1),
    name="worker-timeout-shutdown",
    pid=12345,
    hostname="localhost",
  )
  make_ready_job(args=["slow-timeout"])
  worker.start()
  worker._execute_job = lambda job_id: release.wait(timeout=1)

  worker.poll_once()

  drained = worker.stop(timeout=0.01)

  assert drained is False
  assert Process.objects.filter(name="worker-timeout-shutdown").exists() is True

  release.set()
  wait_until(lambda: Process.objects.filter(name="worker-timeout-shutdown").exists() is False)


def test_worker_timeout_shutdown_keeps_heartbeat_until_work_finishes():
  release = threading.Event()
  worker = Worker(
    WorkerConfig(queues=("*",), threads=1, processes=1, polling_interval=0.1),
    name="worker-timeout-heartbeat",
    pid=12345,
    hostname="localhost",
    heartbeat_interval=0.01,
  )
  make_ready_job(args=["slow-timeout-heartbeat"])
  worker.start()
  worker._execute_job = lambda job_id: release.wait(timeout=5)

  worker.poll_once()
  process_pk = worker.process.pk
  initial_heartbeat = Process.objects.get(pk=process_pk).last_heartbeat_at

  try:
    drained = worker.stop(timeout=0.01)

    assert drained is False
    wait_until(
      lambda: (
        Process.objects.filter(pk=process_pk).first() is not None
        and Process.objects.get(pk=process_pk).last_heartbeat_at > initial_heartbeat
      )
    )
  finally:
    release.set()
    wait_until(lambda: Process.objects.filter(pk=process_pk).exists() is False)


def test_worker_missing_task_path_fails_job_cleanly():
  job = make_ready_job(task_path="tests.tasks.missing")
  worker = make_worker()
  worker.start()

  worker.poll_once()

  failed_execution = FailedExecution.objects.get(job=job)
  assert (
    "No module named" in failed_execution.traceback
    or "has no attribute" in failed_execution.traceback
  )
  assert ClaimedExecution.objects.filter(job=job).exists() is False
  worker.stop()
