from datetime import timedelta
from contextlib import contextmanager
import time
import threading
import signal
from uuid import uuid4

import pytest
from django.db import connection
from django.db.utils import OperationalError
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from dj_queue.config import load_backend_config
from dj_queue.exceptions import ProcessExitError, ProcessMissingError, ProcessPrunedError
from dj_queue.models import ClaimedExecution, FailedExecution, Job, Process
from dj_queue.runtime.supervisor import AsyncSupervisor, ForkSupervisor, Supervisor
from tests.tasks import limited

pytestmark = pytest.mark.django_db(transaction=True)


def retry_after_sqlite_lock(operation, *, timeout=1):
  deadline = time.monotonic() + timeout
  while True:
    try:
      return operation()
    except OperationalError as error:
      if "locked" not in str(error).lower():
        raise
      if time.monotonic() >= deadline:
        raise
      time.sleep(0.01)


def make_job(**overrides):
  payload = {
    "args": list(overrides.pop("args", [])),
    "kwargs": dict(overrides.pop("kwargs", {})),
  }
  payload.update(overrides.pop("payload", {}))
  task_path = overrides.pop("task_path", "tests.tasks.echo")
  queue_name = overrides.pop("queue_name", "default")
  priority = overrides.pop("priority", 0)
  backend_alias = overrides.pop("backend_alias", "default")
  scheduled_at = overrides.pop("scheduled_at", None)
  concurrency_key = overrides.pop("concurrency_key", None)
  finished_at = overrides.pop("finished_at", None)
  return_value = overrides.pop("return_value", None)

  return retry_after_sqlite_lock(
    lambda: Job.objects.create(
      task_path=task_path,
      queue_name=queue_name,
      priority=priority,
      payload=payload,
      backend_alias=backend_alias,
      scheduled_at=scheduled_at,
      concurrency_key=concurrency_key,
      finished_at=finished_at,
      return_value=return_value,
      **overrides,
    )
  )


def make_process(**overrides):
  kind = overrides.pop("kind", "Worker")
  pid = overrides.pop("pid", 12345)
  hostname = overrides.pop("hostname", "localhost")
  name = overrides.pop("name", f"worker-{uuid4()}")
  metadata = overrides.pop("metadata", {})
  last_heartbeat_at = overrides.pop("last_heartbeat_at", timezone.now())

  return retry_after_sqlite_lock(
    lambda: Process.objects.create(
      backend_alias=overrides.pop("backend_alias", "default"),
      kind=kind,
      pid=pid,
      hostname=hostname,
      name=name,
      metadata=metadata,
      last_heartbeat_at=last_heartbeat_at,
      **overrides,
    )
  )


def make_claimed_execution(*, job, process):
  return retry_after_sqlite_lock(lambda: ClaimedExecution.objects.create(job=job, process=process))


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
        if workers is not None
        else [{"queues": "*", "threads": 1, "processes": 1, "polling_interval": 0.1}],
        "dispatchers": dispatchers
        if dispatchers is not None
        else [{"batch_size": 10, "polling_interval": 1, "concurrency_maintenance": False}],
        "scheduler": {"dynamic_tasks_enabled": False, "polling_interval": 5},
        "recurring": recurring or {},
        "preserve_finished_jobs": False,
        "clear_finished_jobs_after": None,
        "supervisor_pidfile": None,
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


def build_fork_supervisor(
  *,
  tasks_settings,
  standalone=True,
  name=None,
  launcher=None,
  waitpid=None,
  killer=None,
  exit_fn=None,
):
  return ForkSupervisor.from_backend_config(
    backend_alias="default",
    tasks_settings=tasks_settings,
    name=name or f"fork-supervisor-{uuid4()}",
    pid=76543,
    hostname="localhost",
    standalone=standalone,
    launcher=launcher,
    waitpid=waitpid,
    killer=killer,
    exit_fn=exit_fn,
  )


def wait_until(predicate, timeout=1):
  deadline = time.monotonic() + timeout
  while time.monotonic() < deadline:
    try:
      if predicate():
        return
    except OperationalError as error:
      if "locked" not in str(error).lower():
        raise
    time.sleep(0.01)

  try:
    assert predicate()
  except OperationalError as error:
    if "locked" not in str(error).lower():
      raise
    pytest.fail(f"timed out waiting for predicate after database lock: {error}")


def test_startup_orphan_cleanup_fails_leftover_claimed_jobs():
  process = make_process()
  job = make_job(
    task_path="tests.tasks.limited",
    args=[1],
    kwargs={"value": "first"},
    concurrency_key="account:1",
  )
  make_claimed_execution(job=job, process=process)
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


def test_supervisor_start_checks_persistent_connection_budget(monkeypatch):
  seen = []

  def record_warning(config, *, backend_alias):
    seen.append((config, backend_alias))

  monkeypatch.setattr(
    "dj_queue.runtime.supervisor.warn_if_persistent_connection_budget_is_tight",
    record_warning,
  )
  supervisor = make_supervisor(name="supervisor-connection-budget")

  supervisor.start()
  supervisor.stop()

  assert seen == [(supervisor.config, "default")]


def test_supervisor_start_failure_cleans_process_row_and_pidfile(monkeypatch, tmp_path):
  pidfile = tmp_path / "run" / "dj_queue.pid"
  tasks_settings = async_tasks_settings(recurring={})
  tasks_settings["default"]["OPTIONS"]["supervisor_pidfile"] = str(pidfile)
  supervisor = build_fork_supervisor(
    tasks_settings=tasks_settings,
    launcher=lambda spec: 91000,
    name="failing-supervisor-start",
  )

  def fail_budget_check(config, *, backend_alias):
    raise RuntimeError("budget check failed")

  monkeypatch.setattr(
    "dj_queue.runtime.supervisor.warn_if_persistent_connection_budget_is_tight",
    fail_budget_check,
  )

  with pytest.raises(RuntimeError, match="budget check failed"):
    supervisor.start()

  assert pidfile.exists() is False
  assert Process.objects.filter(name="failing-supervisor-start").exists() is False


def test_prune_stale_process_rows_fails_their_claimed_jobs():
  stale_process = make_process(
    name="stale-worker",
    last_heartbeat_at=timezone.now() - timedelta(minutes=10),
  )
  fresh_process = make_process(name="fresh-worker")
  stale_job = make_job(task_path="tests.tasks.echo")
  fresh_job = make_job(task_path="tests.tasks.echo")
  make_claimed_execution(job=stale_job, process=stale_process)
  make_claimed_execution(job=fresh_job, process=fresh_process)
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


def test_prune_stale_process_rows_fails_multiple_claimed_jobs_for_process():
  stale_process = make_process(
    name="stale-worker",
    last_heartbeat_at=timezone.now() - timedelta(minutes=10),
  )
  jobs = [make_job(task_path="tests.tasks.echo") for _index in range(3)]
  for job in jobs:
    make_claimed_execution(job=job, process=stale_process)
  supervisor = make_supervisor()
  supervisor.start()

  try:
    pruned = supervisor.prune_stale_process_rows(now=timezone.now())
  finally:
    supervisor.stop()

  assert [process.name for process in pruned] == ["stale-worker"]
  assert FailedExecution.objects.filter(job__in=jobs).count() == len(jobs)
  assert ClaimedExecution.objects.filter(job__in=jobs).exists() is False


def test_prune_stale_process_rows_query_budget_stays_process_sized():
  stale_process = make_process(
    name="stale-worker",
    last_heartbeat_at=timezone.now() - timedelta(minutes=10),
  )
  jobs = [make_job(task_path="tests.tasks.echo") for _index in range(5)]
  for job in jobs:
    make_claimed_execution(job=job, process=stale_process)
  supervisor = make_supervisor()
  supervisor.start()

  try:
    with CaptureQueriesContext(connection) as ctx:
      pruned = supervisor.prune_stale_process_rows(now=timezone.now())
  finally:
    supervisor.stop()

  assert len(ctx.captured_queries) <= 13
  assert [process.name for process in pruned] == ["stale-worker"]
  assert FailedExecution.objects.filter(job__in=jobs).count() == len(jobs)


def test_prune_stale_process_rows_skips_process_that_heartbeats_before_delete(monkeypatch):
  stale_process = make_process(
    name="stale-worker",
    last_heartbeat_at=timezone.now() - timedelta(minutes=10),
  )
  job = make_job(task_path="tests.tasks.echo")
  make_claimed_execution(job=job, process=stale_process)
  supervisor = make_supervisor()
  supervisor.start()

  from django.db.models.query import QuerySet

  original_delete = QuerySet.delete
  refreshed = {"done": False}

  def refresh_before_delete(queryset):
    if queryset.filter(pk=stale_process.pk).exists() and refreshed["done"] is False:
      refreshed["done"] = True
      Process.objects.filter(pk=stale_process.pk).update(last_heartbeat_at=timezone.now())
    return original_delete(queryset)

  monkeypatch.setattr("django.db.models.query.QuerySet.delete", refresh_before_delete)

  try:
    pruned = supervisor.prune_stale_process_rows(now=timezone.now())
  finally:
    supervisor.stop()

  assert pruned == []
  assert FailedExecution.objects.filter(job=job).exists() is False
  assert ClaimedExecution.objects.filter(job=job, process=stale_process).exists() is True


def test_supervisor_housekeeping_interval_tracks_heartbeat_when_enabled():
  supervisor = make_supervisor()

  assert supervisor.housekeeping_interval == 60


def test_supervisor_housekeeping_interval_falls_back_when_heartbeat_disabled(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {
        "process_heartbeat_interval": 0,
        "process_alive_threshold": 300,
      },
    }
  }
  supervisor = Supervisor(
    load_backend_config(),
    name=f"supervisor-{uuid4()}",
    pid=54321,
    hostname="localhost",
    heartbeat_interval=0.01,
  )

  assert supervisor.housekeeping_interval == 60


def test_supervisor_heartbeat_disabled_still_keeps_live_processes_fresh(settings):
  tasks_settings = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {
        "mode": "async",
        "workers": [{"queues": "*", "threads": 1, "processes": 1, "polling_interval": 0.01}],
        "dispatchers": [],
        "scheduler": None,
        "process_heartbeat_interval": 0,
        "process_alive_threshold": 0.05,
        "supervisor_pidfile": None,
        "preserve_finished_jobs": False,
        "clear_finished_jobs_after": None,
      },
    }
  }
  supervisor = build_async_supervisor(tasks_settings=tasks_settings, standalone=False)
  process = supervisor.start()

  try:
    wait_until(lambda: Process.objects.filter(supervisor=process, kind="Worker").count() == 1)
    worker_process = Process.objects.get(supervisor=process, kind="Worker")
    initial_heartbeat = worker_process.last_heartbeat_at

    wait_until(
      lambda: Process.objects.get(pk=worker_process.pk).last_heartbeat_at > initial_heartbeat,
      timeout=1,
    )
  finally:
    supervisor.stop()


def test_supervisor_poll_once_skips_prune_until_housekeeping_interval(monkeypatch):
  supervisor = make_supervisor()
  supervisor._last_housekeeping_at = 100
  calls = []

  monkeypatch.setattr(Supervisor, "housekeeping_interval", property(lambda self: 60))
  monkeypatch.setattr("dj_queue.runtime.supervisor.time.monotonic", lambda: 120)
  monkeypatch.setattr(
    supervisor, "prune_stale_process_rows", lambda now=None: calls.append(now) or []
  )

  assert supervisor.poll_once() == []
  assert calls == []


def test_supervisor_poll_once_prunes_when_housekeeping_interval_elapsed(monkeypatch):
  supervisor = make_supervisor()
  supervisor._last_housekeeping_at = 100
  stale_process = make_process(name="stale-worker")

  monkeypatch.setattr(Supervisor, "housekeeping_interval", property(lambda self: 60))
  monkeypatch.setattr("dj_queue.runtime.supervisor.time.monotonic", lambda: 160)
  monkeypatch.setattr(supervisor, "prune_stale_process_rows", lambda now=None: [stale_process])

  pruned = supervisor.poll_once()

  assert pruned == [stale_process]
  assert supervisor._last_housekeeping_at == 160


def test_supervisor_poll_once_keeps_running_after_housekeeping_error(monkeypatch):
  handled = []
  supervisor = make_supervisor()
  supervisor._last_housekeeping_at = 100

  monkeypatch.setattr(Supervisor, "housekeeping_interval", property(lambda self: 60))
  monkeypatch.setattr("dj_queue.runtime.supervisor.time.monotonic", lambda: 160)
  monkeypatch.setattr(
    supervisor,
    "prune_stale_process_rows",
    lambda now=None: (_ for _ in ()).throw(RuntimeError("prune failed")),
  )
  monkeypatch.setattr(
    "dj_queue.runtime.supervisor.handle_thread_error",
    lambda error, **kwargs: handled.append((str(error), kwargs["context"])),
  )

  assert supervisor.poll_once() == []
  assert supervisor._last_housekeeping_at == 160
  assert handled == [("prune failed", "supervisor.housekeeping")]


@pytest.mark.filterwarnings("error::pytest.PytestUnhandledThreadExceptionWarning")
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


def test_async_supervisor_restarts_crashed_runner_and_fails_its_claimed_jobs():
  supervisor = build_async_supervisor(
    tasks_settings=async_tasks_settings(dispatchers=[]),
  )
  process = supervisor.start()

  try:
    wait_until(lambda: Process.objects.filter(supervisor=process, kind="Worker").count() == 1)

    # grab the worker and attach a claimed job, then force a crash
    worker = supervisor.runners[0]
    original_process_pk = worker.process.pk
    job = make_job(task_path="tests.tasks.echo", queue_name="default")
    make_claimed_execution(job=job, process=worker.process)
    original_poll = worker.poll_once

    crash_count = 0

    def crashing_poll():
      nonlocal crash_count
      crash_count += 1
      if crash_count == 1:
        raise RuntimeError("simulated worker crash")
      return original_poll()

    worker.poll_once = crashing_poll

    # wait for the crash to be detected and a replacement runner to register
    wait_until(lambda: crash_count >= 1, timeout=2)
    wait_until(
      lambda: (
        supervisor.runners[0] is not worker
        and supervisor.runners[0].process is not None
        and supervisor.runners[0].process.pk != original_process_pk
        and not Process.objects.filter(pk=original_process_pk).exists()
      ),
      timeout=2,
    )

    # the old runner was replaced and the replacement process row is registered
    replacement_worker = supervisor.runners[0]
    new_worker_process = replacement_worker.process
    assert new_worker_process is not None
    assert new_worker_process.pk != original_process_pk
    assert new_worker_process.name == "worker-1"
    assert (
      Process.objects.filter(
        pk=new_worker_process.pk,
        supervisor=process,
        kind="Worker",
      ).exists()
      is True
    )
  finally:
    supervisor.stop()

  # the claimed job was failed, not left orphaned
  assert ClaimedExecution.objects.filter(job=job).exists() is False
  failed = FailedExecution.objects.get(job=job)
  assert failed.exception_class == (
    f"{ProcessExitError.__module__}.{ProcessExitError.__qualname__}"
  )

  assert Process.objects.filter(supervisor=process, kind="Worker").exists() is False


def test_async_supervisor_stop_does_not_restart_runner_after_stop_request():
  supervisor = build_async_supervisor(
    tasks_settings=async_tasks_settings(dispatchers=[]),
  )
  process = supervisor.start()

  try:
    wait_until(lambda: Process.objects.filter(supervisor=process, kind="Worker").count() == 1)

    worker = supervisor.runners[0]
    crash_handled = threading.Event()
    allow_runner_stop = threading.Event()
    original_stop = worker.stop

    def crashing_poll():
      raise RuntimeError("simulated worker crash")

    def gated_stop(*, timeout=None):
      crash_handled.set()
      allow_runner_stop.wait(timeout=1)
      return original_stop(timeout=timeout)

    worker.poll_once = crashing_poll
    worker.stop = gated_stop

    wait_until(crash_handled.is_set, timeout=2)
    supervisor.stop()
    allow_runner_stop.set()
  finally:
    allow_runner_stop.set()
    supervisor.stop()

  assert supervisor.runners == []
  assert Process.objects.filter(supervisor=process, kind="Worker").exists() is False


def test_async_supervisor_drains_crashed_runner_before_failing_leftovers():
  supervisor = build_async_supervisor(
    tasks_settings=async_tasks_settings(dispatchers=[]),
  )
  events = []

  class Runner:
    process = object()
    process_kind = "Worker"
    name = "worker-1"
    pool = type("Pool", (), {"idle_capacity": 0, "max_workers": 1})()

    def run_managed_poll_loop(self, *, host_stop_requested):
      assert host_stop_requested() is False
      events.append("poll")
      return False

    def stop(self):
      events.append("stop")
      self.process = None
      return True

  runner = Runner()

  def fail_leftovers(crashed_runner):
    assert crashed_runner is runner
    events.append("fail")
    supervisor.request_stop()

  supervisor._fail_crashed_runner_jobs = fail_leftovers

  supervisor._run_managed_runner(runner)

  assert events == ["poll", "stop", "fail"]


def test_async_supervisor_preserves_active_claims_until_drain_finishes():
  supervisor = build_async_supervisor(
    tasks_settings=async_tasks_settings(dispatchers=[]),
  )
  events = []

  class Runner:
    process = object()
    process_kind = "Worker"
    name = "worker-1"
    pool = type("Pool", (), {"idle_capacity": 0, "max_workers": 1})()

    def run_managed_poll_loop(self, *, host_stop_requested):
      assert host_stop_requested() is False
      events.append("poll")
      return False

    def stop(self):
      events.append("stop")
      return False

  runner = Runner()

  def fail_leftovers(crashed_runner):
    assert crashed_runner is runner
    events.append("fail")
    supervisor.request_stop()

  supervisor._fail_crashed_runner_jobs = fail_leftovers
  thread = threading.Thread(target=supervisor._run_managed_runner, args=(runner,))

  thread.start()
  wait_until(lambda: events == ["poll", "stop"], timeout=1)

  assert "fail" not in events

  runner.process = None
  thread.join(timeout=1)

  assert thread.is_alive() is False
  assert events == ["poll", "stop", "fail"]


def test_async_supervisor_starts_managed_runners_inside_app_executor(monkeypatch):
  supervisor = build_async_supervisor(
    tasks_settings=async_tasks_settings(dispatchers=[]),
  )
  events = []

  @contextmanager
  def executor():
    events.append("enter")
    try:
      yield
    finally:
      events.append("exit")

  class Runner:
    process_kind = "Worker"
    name = "worker-1"

    def start(self):
      events.append("start")

    def run_managed_poll_loop(self, *, host_stop_requested):
      events.append("run")
      return True

    def stop(self):
      events.append("stop")

  monkeypatch.setattr("dj_queue.runtime.supervisor.app_executor", executor)
  monkeypatch.setattr(supervisor, "_build_runners", lambda: [Runner()])

  supervisor.start_runners()
  supervisor.runner_threads[0].join(timeout=1)

  assert supervisor.runner_threads[0].is_alive() is False
  assert events[:3] == ["enter", "start", "exit"]


def test_async_supervisor_waits_for_undrained_crashed_runner_before_replacement():
  supervisor = build_async_supervisor(
    tasks_settings=async_tasks_settings(dispatchers=[]),
  )
  process = supervisor.start()

  try:
    wait_until(lambda: Process.objects.filter(supervisor=process, kind="Worker").count() == 1)

    worker = supervisor.runners[0]
    original_process_pk = worker.process.pk
    original_stop = worker.stop
    allow_runner_stop = threading.Event()

    def crashing_poll():
      raise RuntimeError("simulated worker crash")

    def gated_stop(*, timeout=None):
      drained = original_stop(timeout=0)
      if drained is False:
        allow_runner_stop.wait(timeout=1)
      return drained

    worker.poll_once = crashing_poll
    worker.stop = gated_stop

    wait_until(
      lambda: worker.process is not None and worker.process.pk == original_process_pk,
      timeout=2,
    )
    assert Process.objects.filter(pk=original_process_pk).exists() is True

    allow_runner_stop.set()
    wait_until(
      lambda: (
        supervisor.runners[0] is not worker
        and supervisor.runners[0].process is not None
        and supervisor.runners[0].process.pk != original_process_pk
        and not Process.objects.filter(pk=original_process_pk).exists()
      ),
      timeout=2,
    )
  finally:
    allow_runner_stop.set()
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


@pytest.mark.filterwarnings("error::pytest.PytestUnhandledThreadExceptionWarning")
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


def test_standalone_async_supervisor_registers_signal_handlers(monkeypatch):
  supervisor = build_async_supervisor(tasks_settings=async_tasks_settings(), standalone=True)
  registered = []

  monkeypatch.setattr(signal, "signal", lambda sig, handler: registered.append((sig, handler)))
  process = supervisor.start()

  try:
    wait_until(lambda: Process.objects.filter(supervisor=process).count() == 2)
    assert [sig for sig, _handler in registered] == [signal.SIGTERM, signal.SIGINT, signal.SIGQUIT]
  finally:
    supervisor.stop()


def test_async_supervisor_sigterm_is_idempotent():
  supervisor = build_async_supervisor(tasks_settings=async_tasks_settings(), standalone=True)
  process = supervisor.start()

  try:
    wait_until(lambda: Process.objects.filter(supervisor=process).count() == 2)
    first = supervisor.handle_sigterm()
    second = supervisor.handle_sigterm()
    assert supervisor.stop_requested() is True
  finally:
    supervisor.stop()

  assert first is True
  assert second is False
  assert Process.objects.filter(supervisor=process).exists() is False


def test_async_supervisor_sigquit_takes_immediate_exit_path():
  exited = []
  supervisor = build_async_supervisor(tasks_settings=async_tasks_settings(), standalone=True)
  supervisor._exit_fn = lambda code: exited.append(code)

  supervisor.handle_sigquit()

  assert exited == [1]


def test_async_supervisor_stop_preserves_undrained_worker_until_work_finishes(monkeypatch):
  release = threading.Event()
  finished = threading.Event()
  tasks_settings = async_tasks_settings(dispatchers=[])
  tasks_settings["default"]["OPTIONS"]["shutdown_timeout"] = 0.01
  supervisor = build_async_supervisor(
    tasks_settings=tasks_settings,
  )
  process = supervisor.start()

  try:
    wait_until(lambda: Process.objects.filter(supervisor=process, kind="Worker").count() == 1)

    worker = supervisor.runners[0]
    worker_process_pk = worker.process.pk

    def blocking_job(job_id):
      try:
        release.wait()
      finally:
        finished.set()

    monkeypatch.setattr(worker, "_execute_job", blocking_job)

    worker.pool.submit(worker._execute_job, "slow-job")
    assert supervisor.runner_threads[0].daemon is False

    supervisor.stop()

    assert Process.objects.filter(pk=worker_process_pk).exists() is True
  finally:
    release.set()
    wait_until(finished.is_set)
    wait_until(lambda: Process.objects.filter(supervisor=process, kind="Worker").exists() is False)


def test_async_supervisor_passes_backend_heartbeat_interval_to_managed_runners():
  tasks_settings = async_tasks_settings(dispatchers=[])
  tasks_settings["default"]["OPTIONS"]["process_heartbeat_interval"] = 0.25
  supervisor = build_async_supervisor(tasks_settings=tasks_settings, standalone=False)

  process = supervisor.start()

  try:
    wait_until(lambda: Process.objects.filter(supervisor=process, kind="Worker").count() == 1)
    assert supervisor.runners[0]._heartbeat_interval == 0.25
  finally:
    supervisor.stop()


def test_fork_supervisor_starts_configured_children():
  launched = []

  def launcher(spec):
    launched.append(spec)
    return 80000 + len(launched)

  supervisor = build_fork_supervisor(
    tasks_settings=async_tasks_settings(
      recurring={
        "static-task": {
          "task_path": "tests.tasks.echo",
          "schedule": "* * * * *",
        }
      }
    ),
    launcher=launcher,
  )

  process = supervisor.start()

  assert Process.objects.filter(pk=process.pk, kind="Supervisor").exists() is True
  assert [spec["kind"] for spec in launched] == ["worker", "dispatcher", "scheduler"]
  assert sorted(supervisor.children) == [80001, 80002, 80003]
  supervisor.stop()


def test_fork_supervisor_starts_children_before_heartbeat_thread(monkeypatch):
  events = []
  supervisor = build_fork_supervisor(
    tasks_settings=async_tasks_settings(dispatchers=[], recurring={}),
  )

  def launcher(spec):
    events.append(("launch", spec["kind"], supervisor._heartbeat_thread is not None))
    return 80000 + len([event for event in events if event[0] == "launch"])

  def start_heartbeat_thread():
    events.append(("heartbeat", tuple(sorted(supervisor.children))))

  supervisor._launcher = launcher
  monkeypatch.setattr(supervisor, "_start_heartbeat_thread", start_heartbeat_thread)

  supervisor.start()

  try:
    assert events == [
      ("launch", "worker", False),
      ("heartbeat", (80001,)),
    ]
  finally:
    supervisor.stop()


def test_fork_supervisor_stop_waits_for_child_exit_before_hard_kill():
  killed = []
  waitpid_results = [(80001, 0)]

  def waitpid(_pid, _flags):
    if waitpid_results:
      return waitpid_results.pop(0)
    raise ChildProcessError

  supervisor = build_fork_supervisor(
    tasks_settings=async_tasks_settings(dispatchers=[], recurring={}),
    launcher=lambda spec: 80001,
    waitpid=waitpid,
    killer=lambda pid, sig: killed.append((pid, sig)),
  )
  supervisor_process = supervisor.start()
  child_process = make_process(pid=80001, name="worker-1", supervisor=supervisor_process)
  job = make_job(task_path="tests.tasks.echo")
  make_claimed_execution(job=job, process=child_process)

  supervisor.stop()

  assert killed == [(80001, signal.SIGTERM)]
  assert supervisor.children == {}
  assert FailedExecution.objects.get(job=job).exception_class == (
    f"{ProcessExitError.__module__}.{ProcessExitError.__qualname__}"
  )
  assert Process.objects.filter(pk=child_process.pk).exists() is False


def test_fork_supervisor_records_child_exit_status():
  waitpid_results = [(80001, 7 << 8)]
  launches = 0

  def launcher(spec):
    nonlocal launches
    launches += 1
    return 80000 + launches

  def waitpid(_pid, _flags):
    if waitpid_results:
      return waitpid_results.pop(0)
    return 0, 0

  supervisor = build_fork_supervisor(
    tasks_settings=async_tasks_settings(dispatchers=[], recurring={}),
    launcher=launcher,
    waitpid=waitpid,
  )
  supervisor_process = supervisor.start()
  child_process = make_process(pid=80001, name="worker-1", supervisor=supervisor_process)
  job = make_job(task_path="tests.tasks.echo")
  make_claimed_execution(job=job, process=child_process)

  try:
    supervisor.check_children()
  finally:
    supervisor.stop()

  assert FailedExecution.objects.get(job=job).message == "child process exited with status 7"


def test_fork_supervisor_stop_fails_claimed_jobs_when_child_map_is_stale():
  killed = []

  def waitpid(_pid, _flags):
    raise ChildProcessError

  supervisor = build_fork_supervisor(
    tasks_settings=async_tasks_settings(dispatchers=[], recurring={}),
    launcher=lambda spec: 80001,
    waitpid=waitpid,
    killer=lambda pid, sig: killed.append((pid, sig)),
  )
  supervisor_process = supervisor.start()
  child_process = make_process(pid=80001, name="worker-1", supervisor=supervisor_process)
  job = make_job(task_path="tests.tasks.echo")
  make_claimed_execution(job=job, process=child_process)

  supervisor.stop()

  assert killed == [(80001, signal.SIGTERM)]
  assert supervisor.children == {}
  assert FailedExecution.objects.get(job=job).exception_class == (
    f"{ProcessExitError.__module__}.{ProcessExitError.__qualname__}"
  )
  assert Process.objects.filter(pk=child_process.pk).exists() is False


def test_fork_supervisor_stop_kills_unreaped_child_and_fails_claimed_jobs():
  killed = []
  tasks_settings = async_tasks_settings(dispatchers=[], recurring={})
  tasks_settings["default"]["OPTIONS"]["shutdown_timeout"] = 0
  supervisor = build_fork_supervisor(
    tasks_settings=tasks_settings,
    launcher=lambda spec: 80001,
    waitpid=lambda _pid, _flags: (0, 0),
    killer=lambda pid, sig: killed.append((pid, sig)),
  )
  supervisor_process = supervisor.start()
  child_process = make_process(pid=80001, name="worker-1", supervisor=supervisor_process)
  job = make_job(task_path="tests.tasks.echo")
  make_claimed_execution(job=job, process=child_process)

  supervisor.stop()

  assert killed == [(80001, signal.SIGTERM), (80001, signal.SIGKILL)]
  assert FailedExecution.objects.get(job=job).exception_class == (
    f"{ProcessExitError.__module__}.{ProcessExitError.__qualname__}"
  )
  assert Process.objects.filter(pk=child_process.pk).exists() is False


def test_fork_launcher_closes_parent_connections_around_fork(monkeypatch):
  events = []
  supervisor = build_fork_supervisor(
    tasks_settings=async_tasks_settings(dispatchers=[], recurring={}),
    launcher=lambda spec: 80001,
  )

  monkeypatch.setattr("dj_queue.runtime.supervisor.os.fork", lambda: 80001)
  monkeypatch.setattr(
    "dj_queue.runtime.supervisor.connections",
    type("DummyConnections", (), {"close_all": lambda self: events.append("close")})(),
  )

  pid = supervisor._default_launcher({"runner_class": object, "kwargs": {}})

  assert pid == 80001
  assert events == ["close", "close"]


def test_fork_child_bootstrap_error_is_reported_and_exits_nonzero(monkeypatch):
  class ChildExit(BaseException):
    pass

  class BrokenRunner:
    def __init__(self, **_kwargs):
      raise RuntimeError("bootstrap failed")

  handled = []
  exits = []
  supervisor = build_fork_supervisor(
    tasks_settings=async_tasks_settings(dispatchers=[], recurring={}),
    launcher=lambda spec: 80001,
  )

  def exit_child(status):
    exits.append(status)
    raise ChildExit

  supervisor._child_exit_fn = exit_child

  monkeypatch.setattr("dj_queue.runtime.supervisor.os.fork", lambda: 0)
  monkeypatch.setattr(
    "dj_queue.runtime.supervisor.handle_thread_error",
    lambda error, **kwargs: handled.append((error, kwargs)),
  )

  with pytest.raises(ChildExit):
    supervisor._default_launcher(
      {
        "kind": "worker",
        "runner_class": BrokenRunner,
        "kwargs": {"name": "worker-1"},
      }
    )

  error, kwargs = handled[0]
  assert str(error) == "bootstrap failed"
  assert kwargs == {"context": "supervisor.child", "backend_alias": "default"}
  assert exits == [1]


def test_fork_child_sigterm_requests_runner_stop(monkeypatch):
  registered = {}
  stopped = []
  supervisor = build_fork_supervisor(
    tasks_settings=async_tasks_settings(dispatchers=[], recurring={}),
    launcher=lambda spec: 80001,
  )
  runner = type("DummyRunner", (), {"request_stop": lambda self: stopped.append(True)})()

  monkeypatch.setattr(signal, "signal", lambda sig, handler: registered.setdefault(sig, handler))

  supervisor._register_child_signal_handlers(runner)
  registered[signal.SIGTERM]()

  assert stopped == [True]


def test_dead_child_fails_claimed_jobs_and_replaces_runner():
  launched = []
  waitpid_results = [(90001, 0)]

  def launcher(spec):
    launched.append(spec)
    return 90000 + len(launched)

  def waitpid(_pid, _flags):
    if waitpid_results:
      return waitpid_results.pop(0)
    raise ChildProcessError

  supervisor = build_fork_supervisor(
    tasks_settings=async_tasks_settings(dispatchers=[], recurring={}),
    launcher=launcher,
    waitpid=waitpid,
  )
  supervisor_process = supervisor.start()
  try:
    child_process = make_process(pid=90001, name="worker-1", supervisor=supervisor_process)
    job = make_job(task_path="tests.tasks.echo")
    make_claimed_execution(job=job, process=child_process)

    replaced = supervisor.check_children()

    failed_execution = FailedExecution.objects.get(job=job)
    assert replaced == 90002
    assert failed_execution.exception_class == (
      f"{ProcessExitError.__module__}.{ProcessExitError.__qualname__}"
    )
    assert supervisor.children[90002]["kind"] == "worker"
    assert Process.objects.filter(pk=child_process.pk).exists() is False
  finally:
    supervisor.stop()


def test_fork_supervisor_backs_off_repeated_short_child_restarts(monkeypatch):
  now = 100.0
  launched = []
  waitpid_results = [(90001, 0), (90002, 0)]

  monkeypatch.setattr("dj_queue.runtime.supervisor.time.monotonic", lambda: now)

  def launcher(spec):
    launched.append((spec["kind"], now))
    return 90000 + len(launched)

  def waitpid(_pid, _flags):
    if waitpid_results:
      return waitpid_results.pop(0)
    raise ChildProcessError

  supervisor = build_fork_supervisor(
    tasks_settings=async_tasks_settings(dispatchers=[], recurring={}),
    launcher=launcher,
    waitpid=waitpid,
    killer=lambda _pid, _sig: None,
  )

  supervisor.start()
  try:
    assert supervisor.check_children() == 90002

    now += 0.01
    assert supervisor.check_children() is None
    assert launched == [("worker", 100.0), ("worker", 100.0)]

    now += 0.2
    assert supervisor.check_children() == 90003
    assert [kind for kind, _started_at in launched] == ["worker", "worker", "worker"]
    assert launched[-1][1] > 100.1
  finally:
    supervisor.stop()


def test_fork_supervisor_ignores_unknown_reaped_child_pid():
  waitpid_results = [(90009, 0)]

  def waitpid(_pid, _flags):
    if waitpid_results:
      return waitpid_results.pop(0)
    raise ChildProcessError

  supervisor = build_fork_supervisor(
    tasks_settings=async_tasks_settings(dispatchers=[], recurring={}),
    launcher=lambda spec: 90001,
    waitpid=waitpid,
  )
  supervisor.start()

  replaced = supervisor.check_children()

  assert replaced is None
  assert tuple(supervisor.children) == (90001,)
  supervisor.stop()


def test_dead_child_cleanup_uses_supervisor_scoped_process_identity():
  launched = []
  waitpid_results = [(90001, 0)]

  def launcher(spec):
    launched.append(spec)
    return 90000 + len(launched)

  def waitpid(_pid, _flags):
    if waitpid_results:
      return waitpid_results.pop(0)
    raise ChildProcessError

  supervisor = build_fork_supervisor(
    tasks_settings=async_tasks_settings(dispatchers=[], recurring={}),
    launcher=launcher,
    waitpid=waitpid,
  )
  supervisor_process = supervisor.start()
  dead_child_process = make_process(pid=90001, name="worker-1", supervisor=supervisor_process)
  reused_pid_process = make_process(pid=90001, name="worker-99")
  dead_child_job = make_job(task_path="tests.tasks.echo")
  reused_pid_job = make_job(task_path="tests.tasks.echo")
  make_claimed_execution(job=dead_child_job, process=dead_child_process)
  make_claimed_execution(job=reused_pid_job, process=reused_pid_process)

  supervisor.check_children()

  assert FailedExecution.objects.filter(job=dead_child_job).exists() is True
  assert FailedExecution.objects.filter(job=reused_pid_job).exists() is False
  assert (
    ClaimedExecution.objects.filter(job=reused_pid_job, process=reused_pid_process).exists()
    is True
  )
  assert Process.objects.filter(pk=dead_child_process.pk).exists() is False
  assert Process.objects.filter(pk=reused_pid_process.pk).exists() is True
  supervisor.stop()


def test_fork_supervisor_poll_once_checks_children_without_pruning_every_tick(monkeypatch):
  supervisor = build_fork_supervisor(
    tasks_settings=async_tasks_settings(recurring={}),
    launcher=lambda spec: 91000,
  )
  calls = []

  monkeypatch.setattr(Supervisor, "poll_once", lambda self: calls.append("prune") or [])
  monkeypatch.setattr(supervisor, "check_children", lambda: calls.append("children") or 91001)

  assert supervisor.poll_once() == 91001
  assert calls == ["prune", "children"]


def test_repeated_sigterm_is_idempotent():
  killed = []

  def killer(pid, sig):
    killed.append((pid, sig))

  supervisor = build_fork_supervisor(
    tasks_settings=async_tasks_settings(recurring={}),
    launcher=lambda spec: 91000 + len(killed) + 1,
    killer=killer,
  )
  supervisor.start()

  try:
    first = supervisor.handle_sigterm()
    second = supervisor.handle_sigterm()
    assert supervisor.stop_requested() is True
  finally:
    supervisor.stop()

  assert first is True
  assert second is False
  assert supervisor._graceful_shutdown_requested is True


def test_sigquit_takes_immediate_exit_path():
  exited = []
  supervisor = build_fork_supervisor(
    tasks_settings=async_tasks_settings(recurring={}),
    launcher=lambda spec: 92000,
    exit_fn=lambda code: exited.append(code),
  )
  supervisor.start()

  try:
    supervisor.handle_sigquit()
  finally:
    supervisor.stop()

  assert exited == [1]


def test_pidfile_created_and_deleted_for_standalone_supervisor(tmp_path):
  pidfile = tmp_path / "run" / "dj_queue.pid"
  tasks_settings = async_tasks_settings(recurring={})
  tasks_settings["default"]["OPTIONS"]["supervisor_pidfile"] = str(pidfile)
  supervisor = build_fork_supervisor(tasks_settings=tasks_settings, launcher=lambda spec: 93000)

  supervisor.start()

  assert pidfile.read_text() == str(supervisor.pid)

  supervisor.stop()

  assert pidfile.exists() is False


def test_stale_pidfile_is_overwritten(tmp_path):
  pidfile = tmp_path / "run" / "dj_queue.pid"
  pidfile.parent.mkdir(parents=True, exist_ok=True)
  pidfile.write_text("999999")
  tasks_settings = async_tasks_settings(recurring={})
  tasks_settings["default"]["OPTIONS"]["supervisor_pidfile"] = str(pidfile)
  supervisor = build_fork_supervisor(tasks_settings=tasks_settings, launcher=lambda spec: 94000)

  supervisor.start()

  assert pidfile.read_text() == str(supervisor.pid)
  supervisor.stop()


def test_supervisor_stop_does_not_delete_replaced_pidfile(tmp_path):
  pidfile = tmp_path / "run" / "dj_queue.pid"
  tasks_settings = async_tasks_settings(recurring={})
  tasks_settings["default"]["OPTIONS"]["supervisor_pidfile"] = str(pidfile)
  supervisor = build_fork_supervisor(tasks_settings=tasks_settings, launcher=lambda spec: 94000)

  supervisor.start()
  pidfile.write_text("999999")

  supervisor.stop()

  assert pidfile.exists() is True
  assert pidfile.read_text() == "999999"
