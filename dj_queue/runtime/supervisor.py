import logging
import os
import signal
import threading
import time
from datetime import timedelta

from django.db import connections
from django.utils import timezone

from dj_queue.config import load_backend_config
from dj_queue.db import psycopg_implementation
from dj_queue.exceptions import ProcessExitError, ProcessMissingError, ProcessPrunedError
from dj_queue.log import log_event
from dj_queue.operations.recovery import (
  fail_claimed_jobs_for_child,
  fail_orphaned_claimed_jobs,
  prune_stale_processes,
)
from dj_queue.runtime.base import BaseRunner, app_executor
from dj_queue.runtime.connection_budget import warn_if_persistent_connection_budget_is_tight
from dj_queue.runtime.errors import handle_thread_error
from dj_queue.runtime.pidfile import PidFile
from dj_queue.runtime.topology import runner_definitions

SUPERVISOR_RESTART_BACKOFF_BASE_DELAY = 0.1
SUPERVISOR_RESTART_BACKOFF_MAX_DELAY = 30.0
SUPERVISOR_RESTART_BACKOFF_RESET_AFTER = 60.0
SUPERVISOR_RECOVERY_BATCH_SIZE = 500


class Supervisor(BaseRunner):
  process_kind = "Supervisor"
  hook_prefix = "supervisor"
  polling_interval = 0.1

  def __init__(self, config, *, standalone=True, **kwargs):
    super().__init__(config, backend_config=config, **kwargs)
    self.standalone = standalone
    self.pidfile = None
    self._last_housekeeping_at = None
    self._restart_failures = {}
    self._host_started_at = {}
    self._pending_replacements = []
    self._graceful_shutdown_requested = False

  @classmethod
  def from_backend_config(
    cls, *, backend_alias="default", tasks_settings=None, cli_overrides=None, env=None, **kwargs
  ):
    config = load_backend_config(
      backend_alias,
      tasks_settings=tasks_settings,
      cli_overrides=cli_overrides,
      env=env,
    )
    return cls(config, **kwargs)

  def start(self):
    if self.process is not None:
      return self.process
    try:
      self._acquire_pidfile()
      process = self._start_process(start_heartbeat=False)
      warn_if_persistent_connection_budget_is_tight(self.config)
      warn_if_psycopg_is_pure_python(self.config)
      self.fail_orphaned_jobs()
      if self.standalone:
        self.register_signal_handlers()
      for definition in runner_definitions(self.config):
        self._launch_host(
          {
            "kind": definition.kind,
            "runner_class": definition.runner_class,
            "kwargs": self._runner_kwargs(definition.config, definition.name),
          }
        )
      # fork children must not inherit a running heartbeat thread
      self._start_heartbeat_thread()
    except Exception:
      self.stop()
      raise
    return process

  def stop(self):
    self.request_stop()
    for dead in (*self._pending_replacements, *self._stop_hosts()):
      try:
        if self._host_drained(dead):
          self._fail_claimed_jobs_for_host(dead)
      except Exception as error:
        handle_thread_error(error, context="supervisor.stop", config=self.backend_config)
    self._pending_replacements.clear()
    return super().stop()

  def register_signal_handlers(self):
    signal.signal(signal.SIGTERM, self.handle_sigterm)
    signal.signal(signal.SIGINT, self.handle_sigterm)
    signal.signal(signal.SIGQUIT, self.handle_sigquit)

  def handle_sigterm(self, *_args):
    if self._graceful_shutdown_requested:
      return False

    self._graceful_shutdown_requested = True
    self.request_stop()
    return True

  def handle_sigquit(self, *_args):
    self.shutdown_timeout = 0
    self._abort()

  def _runner_kwargs(self, config, name):
    return {
      "config": config,
      "backend_config": self.backend_config,
      "name": name,
      "hostname": self.hostname,
      "supervisor": self.process.pk if self.process is not None else None,
    }

  def poll_once(self):
    pruned_processes = []
    if self._housekeeping_due():
      try:
        failed_orphans = self.fail_orphaned_jobs(batch_size=SUPERVISOR_RECOVERY_BATCH_SIZE)
        remaining = SUPERVISOR_RECOVERY_BATCH_SIZE - len(failed_orphans)
        if remaining > 0:
          pruned_processes = self.prune_stale_process_rows(batch_size=remaining)
      except Exception as error:
        handle_thread_error(
          error,
          context="supervisor.housekeeping",
          config=self.backend_config,
        )
      self._last_housekeeping_at = time.monotonic()
    for process in pruned_processes:
      log_event(
        "process.pruned",
        backend_alias=self.backend_alias,
        process_name=process.name,
        pid=process.pid,
      )
    self.check_children()
    return pruned_processes

  def check_children(self):
    for dead in self._reap_hosts():
      name = dead["spec"]["kwargs"]["name"]
      delay = self._restart_backoff_delay(name, started_at=self._host_started_at.pop(name))
      self._pending_replacements.append({**dead, "not_before": time.monotonic() + delay})

    now = time.monotonic()
    for pending in self._pending_replacements:
      if pending["not_before"] > now or not self._host_drained(pending):
        continue
      spec = pending["spec"]
      name = spec["kwargs"]["name"]
      try:
        # a row the host left behind blocks its name, and a host that removed its own row
        # orphaned any claims it had not finished
        self._fail_claimed_jobs_for_host(pending)
        self.fail_orphaned_jobs()
        pid = self._launch_host(spec)
      except Exception as error:
        handle_thread_error(error, context="supervisor.replace", config=self.backend_config)
        pending["not_before"] = now + self._restart_backoff_delay(name)
        continue
      self._pending_replacements.remove(pending)
      log_event(
        "process.replaced",
        backend_alias=self.backend_alias,
        process_name=name,
        kind=spec["kind"],
        old_pid=pending["pid"],
        new_pid=pid,
        exit_status=pending["exit_status"],
      )
      return pid
    return None

  def _launch_host(self, spec):
    pid = self._launch(spec)
    self._host_started_at[spec["kwargs"]["name"]] = time.monotonic()
    return pid

  def _fail_claimed_jobs_for_host(self, dead):
    message = dead["message"]
    with app_executor():
      return fail_claimed_jobs_for_child(
        pid=dead["pid"],
        name=dead["spec"]["kwargs"]["name"],
        supervisor_id=self.process.pk if self.process is not None else None,
        error=ProcessExitError(message),
        traceback_text=message,
        config=self.backend_config,
      )

  # hosting strategy: this base hosts nothing, fork and async override it

  def _launch(self, spec):
    return None

  def _reap_hosts(self):
    return []

  def _host_drained(self, dead):
    return True

  def _stop_hosts(self):
    return []

  @property
  def housekeeping_interval(self):
    heartbeat_interval = self.config.process_heartbeat_interval
    if heartbeat_interval > 0:
      return max(min(self.config.process_alive_threshold, max(heartbeat_interval, 1)), 1)
    return max(min(self.config.process_alive_threshold, 60), 1)

  def _housekeeping_due(self):
    if self._last_housekeeping_at is None:
      return True
    return (time.monotonic() - self._last_housekeeping_at) >= self.housekeeping_interval

  def _restart_backoff_delay(self, name, *, started_at=None):
    if started_at is not None:
      runtime = time.monotonic() - started_at
      if runtime >= SUPERVISOR_RESTART_BACKOFF_RESET_AFTER:
        self._restart_failures.pop(name, None)
        return 0

    failures = self._restart_failures.get(name, 0) + 1
    self._restart_failures[name] = failures
    if failures <= 1:
      return 0
    return min(
      SUPERVISOR_RESTART_BACKOFF_BASE_DELAY * (2 ** (failures - 2)),
      SUPERVISOR_RESTART_BACKOFF_MAX_DELAY,
    )

  def process_metadata(self):
    return {
      "mode": self.config.mode,
      "standalone": self.standalone,
      "worker_count": len(self.config.workers),
      "dispatcher_count": len(self.config.dispatchers),
      "has_scheduler": self.config.scheduler is not None,
    }

  def _acquire_pidfile(self):
    if not self.standalone:
      return None
    if self.config.supervisor_pidfile is None:
      return None
    if self.pidfile is None:
      self.pidfile = PidFile(self.config.supervisor_pidfile, pid=self.pid)
      self.pidfile.acquire()
    return self.pidfile

  def _finish_stop(self, process):
    super()._finish_stop(process)
    if self.pidfile is not None:
      self.pidfile.release()
      self.pidfile = None

  def fail_orphaned_jobs(self, *, batch_size=SUPERVISOR_RECOVERY_BATCH_SIZE):
    with app_executor():
      return fail_orphaned_claimed_jobs(
        ProcessMissingError("process no longer registered"),
        traceback_text="process no longer registered",
        batch_size=batch_size,
        config=self.backend_config,
      )

  def prune_stale_process_rows(self, *, now=None, batch_size=SUPERVISOR_RECOVERY_BATCH_SIZE):
    if now is None:
      now = timezone.now()
    cutoff = now - timedelta(seconds=self.config.process_alive_threshold)
    with app_executor():
      return prune_stale_processes(
        cutoff=cutoff,
        error=ProcessPrunedError("process heartbeat expired"),
        traceback_text="process heartbeat expired",
        exclude_process=self.process,
        batch_size=batch_size,
        config=self.backend_config,
      )


class AsyncSupervisor(Supervisor):
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self._hosts = {}

  @property
  def runners(self):
    return [runner for _spec, runner, _thread in self._hosts.values()]

  @property
  def runner_threads(self):
    return [thread for _spec, _runner, thread in self._hosts.values()]

  def _launch(self, spec):
    runner = spec["runner_class"](**spec["kwargs"], pid=self.pid)
    with app_executor():
      try:
        runner.start()
      except Exception:
        runner.stop()
        raise
    thread = threading.Thread(target=self._run_runner, args=(runner,))
    # a replacement takes the dead runner's place
    self._hosts[runner.name] = (spec, runner, thread)
    thread.start()
    return runner.pid

  def _run_runner(self, runner):
    try:
      runner.run()
    except Exception as error:
      handle_thread_error(error, context="supervisor.child", config=self.backend_config)

  def _reap_hosts(self):
    return [
      {
        "spec": spec,
        "pid": self.pid,
        "exit_status": None,
        "message": "runner thread crashed",
        "runner": runner,
      }
      for name, (spec, runner, thread) in self._hosts.items()
      if name in self._host_started_at and not thread.is_alive()
    ]

  def _host_drained(self, dead):
    # in-flight jobs keep their claims and the process row until the pool drains
    pool = getattr(dead["runner"], "pool", None)
    return pool is None or pool.in_flight == 0

  def _stop_hosts(self):
    for runner in self.runners:
      runner.shutdown_timeout = self.shutdown_timeout
      runner.request_stop()
    for thread in self.runner_threads:
      thread.join()
    self._hosts.clear()
    return []


class ForkSupervisor(Supervisor):
  def __init__(self, *args, launcher=None, waitpid=None, killer=None, **kwargs):
    super().__init__(*args, **kwargs)
    self.children = {}
    self._launcher = launcher or self._default_launcher
    self._waitpid = waitpid or os.waitpid
    self._killer = killer or os.kill
    self._child_exit_fn = os._exit

  def _launch(self, spec):
    pid = self._launcher(spec)
    self.children[pid] = spec
    return pid

  def _reap_hosts(self):
    if not self.children:
      return []

    try:
      pid, status = self._waitpid(-1, os.WNOHANG)
    except ChildProcessError:
      return []

    spec = self.children.pop(pid, None)
    if spec is None:
      return []
    return [_dead_child(pid, spec, status)]

  def _stop_hosts(self):
    for pid in self.children:
      self._kill(pid, signal.SIGTERM)

    stopped = []
    started_at = time.monotonic()
    while self.children and time.monotonic() - started_at < self.shutdown_timeout:
      try:
        pid, status = self._waitpid(-1, os.WNOHANG)
      except ChildProcessError:
        # nothing is left to signal, and a pid may already belong to another process
        stopped.extend(_dead_child(pid, spec) for pid, spec in self.children.items())
        self.children.clear()
        break

      if not pid:
        time.sleep(min(0.05, self.shutdown_timeout))
        continue

      spec = self.children.pop(pid, None)
      if spec is not None:
        stopped.append(_dead_child(pid, spec, status))

    for pid, spec in self.children.items():
      self._kill(pid, signal.SIGKILL)
      stopped.append(_dead_child(pid, spec))
    self.children.clear()
    return stopped

  def _kill(self, pid, sig):
    try:
      self._killer(pid, sig)
    except ProcessLookupError:
      pass

  def _default_launcher(self, spec):
    connections.close_all()
    pid = os.fork()
    if pid == 0:
      connections.close_all()
      exit_status = 1
      try:
        runner = spec["runner_class"](**spec["kwargs"])
        self._register_child_signal_handlers(runner)
        if runner.run():
          exit_status = 0
      except Exception as error:
        handle_thread_error(
          error,
          context="supervisor.child",
          config=self.backend_config,
        )
      finally:
        connections.close_all()
      self._child_exit_fn(exit_status)
    connections.close_all()
    return pid

  def _register_child_signal_handlers(self, runner):
    def request_stop(*_args):
      runner.request_stop()
      return True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGQUIT, lambda *_args: self._child_exit_fn(1))


def _dead_child(pid, spec, status=None):
  return {
    "spec": spec,
    "pid": pid,
    "exit_status": status,
    "message": _child_exit_message(status),
  }


def _child_exit_message(status):
  if status is None:
    return "child process exited"
  if os.WIFEXITED(status):
    return f"child process exited with status {os.WEXITSTATUS(status)}"
  if os.WIFSIGNALED(status):
    return f"child process exited from signal {os.WTERMSIG(status)}"
  return f"child process exited with wait status {status}"


def warn_if_psycopg_is_pure_python(config):
  implementation = psycopg_implementation(config.database_alias)
  if implementation != "python":
    return

  log_event(
    "psycopg_pure_python.warning",
    level=logging.WARNING,
    backend_alias=config.backend_alias,
    database_alias=config.database_alias,
    psycopg_implementation=implementation,
    install="psycopg[binary] or psycopg[c]",
  )
