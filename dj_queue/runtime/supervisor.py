import os
import signal
import socket
import sys
import threading
import time

from django.db import connections
from django.utils import timezone
from datetime import timedelta

from dj_queue.config import load_backend_config
from dj_queue.exceptions import ProcessExitError, ProcessMissingError, ProcessPrunedError
from dj_queue.log import log_event
from dj_queue.operations.jobs import (
  fail_claimed_jobs_for_child,
  fail_claimed_jobs_for_pid,
  fail_claimed_jobs_for_process,
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


class Supervisor(BaseRunner):
  process_kind = "Supervisor"
  hook_prefix = "supervisor"
  polling_interval = 0.1

  def __init__(
    self,
    config,
    *,
    backend_alias="default",
    name=None,
    pid=None,
    hostname=None,
    sleeper=None,
    heartbeat_interval=None,
    standalone=True,
  ):
    super().__init__(
      config,
      backend_alias=backend_alias,
      name=name or f"supervisor-{os.getpid()}",
      pid=pid or os.getpid(),
      hostname=hostname or socket.gethostname(),
      sleeper=sleeper,
      heartbeat_interval=heartbeat_interval,
      process_alive_threshold=config.process_alive_threshold,
    )
    self.standalone = standalone
    self.pidfile = None
    self._last_housekeeping_at = None
    self._restart_failures = {}

  @classmethod
  def from_backend_config(
    cls,
    *,
    backend_alias="default",
    tasks_settings=None,
    cli_overrides=None,
    env=None,
    name=None,
    pid=None,
    hostname=None,
    standalone=True,
  ):
    config = load_backend_config(
      backend_alias,
      tasks_settings=tasks_settings,
      cli_overrides=cli_overrides,
      env=env,
    )
    return cls(
      config,
      backend_alias=backend_alias,
      name=name,
      pid=pid,
      hostname=hostname,
      standalone=standalone,
    )

  def start(self):
    return self._start_supervisor_process(start_heartbeat=True)

  def _start_supervisor_process(self, *, start_heartbeat):
    try:
      self._acquire_pidfile()
      process = self._start_process(start_heartbeat=start_heartbeat)
      warn_if_persistent_connection_budget_is_tight(
        self.config,
        backend_alias=self.backend_alias,
      )
      self.fail_startup_orphaned_jobs()
      return process
    except Exception:
      self.stop()
      raise

  def poll_once(self):
    pruned_processes = []
    if self._housekeeping_due():
      try:
        pruned_processes = self.prune_stale_process_rows()
      except Exception as error:
        handle_thread_error(
          error,
          context="supervisor.housekeeping",
          backend_alias=self.backend_alias,
        )
        self._last_housekeeping_at = time.monotonic()
        return []
      else:
        self._last_housekeeping_at = time.monotonic()
    for process in pruned_processes:
      log_event(
        "process.pruned",
        backend_alias=self.backend_alias,
        process_name=process.name,
        pid=process.pid,
      )
    return pruned_processes

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

  def _wait_restart_backoff(self, delay):
    deadline = time.monotonic() + delay
    while not self.stop_requested():
      remaining = deadline - time.monotonic()
      if remaining <= 0:
        return False
      time.sleep(min(0.05, remaining))
    return True

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

  def fail_startup_orphaned_jobs(self):
    with app_executor():
      return fail_orphaned_claimed_jobs(
        ProcessMissingError("process no longer registered at supervisor startup"),
        traceback_text="process no longer registered at supervisor startup",
        backend_alias=self.backend_alias,
      )

  def prune_stale_process_rows(self, *, now=None):
    if now is None:
      now = timezone.now()
    cutoff = now - timedelta(seconds=self.config.process_alive_threshold)
    with app_executor():
      return prune_stale_processes(
        cutoff=cutoff,
        error=ProcessPrunedError("process heartbeat expired"),
        traceback_text="process heartbeat expired",
        backend_alias=self.backend_alias,
        exclude_process=self.process,
      )


class AsyncSupervisor(Supervisor):
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.runners = []
    self.runner_threads = []
    self._graceful_shutdown_requested = False
    self._exit_fn = sys.exit

  def start(self):
    process = super().start()
    try:
      if self.standalone:
        self.register_signal_handlers()
      self.start_runners()
    except Exception:
      self.stop()
      raise
    return process

  def stop(self):
    timeout = max(float(self.config.shutdown_timeout), 0)
    deadline = time.monotonic() + timeout
    self._stop_event.set()
    for runner in self.runners:
      runner.request_stop()
    for thread in self.runner_threads:
      thread.join(timeout=max(deadline - time.monotonic(), 0))

    active_runners = []
    active_threads = []
    for index, runner in enumerate(self.runners):
      thread = self.runner_threads[index] if index < len(self.runner_threads) else None
      drained = runner.stop()
      if drained is False:
        active_runners.append(runner)
        if thread is not None:
          active_threads.append(thread)
    self.runners = active_runners
    self.runner_threads = active_threads
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
    self.request_stop()
    self._exit_fn(1)

  def start_runners(self):
    if self.runners:
      return self.runners

    for runner in self._build_runners():
      with app_executor():
        runner.start()
      thread = threading.Thread(target=self._run_managed_runner, args=(runner,))
      self.runners.append(runner)
      self.runner_threads.append(thread)
      thread.start()
    return self.runners

  def _run_managed_runner(self, runner):
    runner_started_at = time.monotonic()
    try:
      while not self.stop_requested():
        if runner.run_managed_poll_loop(host_stop_requested=self.stop_requested):
          return

        # runner crashed — drain active work before failing the claims it leaves behind
        if self._runner_has_active_work(runner):
          drained = runner.stop()
          if drained is False:
            if self.stop_requested():
              return
            if not self._wait_for_runner_stop(runner):
              return
          self._fail_crashed_runner_jobs(runner)
        else:
          self._fail_crashed_runner_jobs(runner)
          drained = runner.stop()
        if self.stop_requested():
          return
        if drained is False and not self._wait_for_runner_stop(runner):
          return
        delay = self._restart_backoff_delay(runner.name, started_at=runner_started_at)
        if delay and self._wait_restart_backoff(delay):
          return
        while not self.stop_requested():
          replacement = self._rebuild_runner(runner)
          try:
            replacement.start()
          except Exception as error:
            handle_thread_error(
              error,
              context="supervisor.replace",
              backend_alias=self.backend_alias,
            )
            replacement.stop()
            delay = self._restart_backoff_delay(runner.name)
            if delay and self._wait_restart_backoff(delay):
              return
            continue
          self._replace_runner(runner, replacement)
          log_event(
            "process.replaced",
            backend_alias=self.backend_alias,
            process_name=runner.name,
            kind=runner.process_kind,
          )
          runner = replacement
          runner_started_at = time.monotonic()
          break
    finally:
      if not self.stop_requested():
        runner.stop()

  def _wait_for_runner_stop(self, runner):
    while runner.process is not None and not self.stop_requested():
      time.sleep(0.01)
    return runner.process is None

  def _runner_has_active_work(self, runner):
    pool = getattr(runner, "pool", None)
    if pool is None:
      return False
    return pool.idle_capacity < pool.max_workers

  def _replace_runner(self, current, replacement):
    try:
      index = self.runners.index(current)
    except ValueError:
      return
    self.runners[index] = replacement

  def _fail_crashed_runner_jobs(self, runner):
    if runner.process is None:
      return
    with app_executor():
      fail_claimed_jobs_for_process(
        runner.process,
        ProcessExitError("runner thread crashed"),
        traceback_text="runner thread crashed",
        backend_alias=self.backend_alias,
      )

  def _rebuild_runner(self, runner):
    kwargs = {
      "config": runner.config,
      "backend_alias": self.backend_alias,
      "name": runner.name,
      "pid": self.pid,
      "hostname": self.hostname,
      "heartbeat_interval": self.config.process_heartbeat_interval,
      "process_alive_threshold": self.config.process_alive_threshold,
      "supervisor": self.process,
    }
    return runner.__class__(**kwargs)

  def _build_runners(self):
    return [
      definition.runner_class(
        definition.config,
        backend_alias=self.backend_alias,
        name=definition.name,
        pid=self.pid,
        hostname=self.hostname,
        heartbeat_interval=self.config.process_heartbeat_interval,
        process_alive_threshold=self.config.process_alive_threshold,
        supervisor=self.process,
      )
      for definition in runner_definitions(self.config)
    ]


class ForkSupervisor(Supervisor):
  def __init__(
    self,
    *args,
    launcher=None,
    waitpid=None,
    killer=None,
    exit_fn=None,
    **kwargs,
  ):
    super().__init__(*args, **kwargs)
    self.children = {}
    self._child_started_at = {}
    self._pending_child_replacements = []
    self._graceful_shutdown_requested = False
    self._launcher = launcher or self._default_launcher
    self._waitpid = waitpid or os.waitpid
    self._killer = killer or os.kill
    self._exit_fn = exit_fn or sys.exit
    self._child_exit_fn = os._exit

  @classmethod
  def from_backend_config(
    cls,
    *,
    backend_alias="default",
    tasks_settings=None,
    cli_overrides=None,
    env=None,
    name=None,
    pid=None,
    hostname=None,
    standalone=True,
    launcher=None,
    waitpid=None,
    killer=None,
    exit_fn=None,
  ):
    config = load_backend_config(
      backend_alias,
      tasks_settings=tasks_settings,
      cli_overrides=cli_overrides,
      env=env,
    )
    return cls(
      config,
      backend_alias=backend_alias,
      name=name,
      pid=pid,
      hostname=hostname,
      standalone=standalone,
      launcher=launcher,
      waitpid=waitpid,
      killer=killer,
      exit_fn=exit_fn,
    )

  def start(self):
    process = self._start_supervisor_process(start_heartbeat=False)
    try:
      if self.standalone:
        self.register_signal_handlers()
      self.start_children()
      self._start_heartbeat_thread()
    except Exception:
      self.stop()
      raise
    return process

  def stop(self):
    timeout = max(float(self.config.shutdown_timeout), 0)
    for pid in tuple(self.children):
      try:
        self._killer(pid, signal.SIGTERM)
      except ProcessLookupError:
        pass

    self._wait_for_children(timeout)
    for pid in tuple(self.children):
      spec = self.children.get(pid)
      try:
        self._killer(pid, signal.SIGKILL)
      except ProcessLookupError:
        pass
      self._fail_claimed_jobs_for_child(pid, spec)
      self.children.pop(pid, None)
      self._child_started_at.pop(pid, None)
    self._pending_child_replacements.clear()
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
    self.request_stop()
    self._exit_fn(1)

  def start_children(self):
    if self.children:
      return self.children

    for spec in self._build_runner_specs():
      self._launch_child(spec)
    return self.children

  def check_children(self):
    replacement_pid = self._start_due_child_replacement()
    if replacement_pid is not None:
      return replacement_pid
    if not self.children:
      return None

    try:
      pid, status = self._waitpid(-1, os.WNOHANG)
    except ChildProcessError:
      return None

    if not pid:
      return None

    spec = self.children.pop(pid, None)
    started_at = self._child_started_at.pop(pid, None)
    if spec is None:
      return None
    self._fail_claimed_jobs_for_child(pid, spec, status=status)
    return self._replace_child(spec, old_pid=pid, status=status, started_at=started_at)

  def _launch_child(self, spec):
    pid = self._launcher(spec)
    self.children[pid] = spec
    self._child_started_at[pid] = time.monotonic()
    return pid

  def _replace_child(self, spec, *, old_pid, status, started_at):
    delay = self._restart_backoff_delay(spec["kwargs"]["name"], started_at=started_at)
    if delay:
      self._pending_child_replacements.append(
        {
          "not_before": time.monotonic() + delay,
          "old_pid": old_pid,
          "spec": spec,
          "status": status,
        }
      )
      return None
    return self._launch_replacement_child(spec, old_pid=old_pid, status=status)

  def _start_due_child_replacement(self):
    now = time.monotonic()
    for index, pending in enumerate(self._pending_child_replacements):
      if pending["not_before"] > now:
        continue
      self._pending_child_replacements.pop(index)
      return self._launch_replacement_child(
        pending["spec"],
        old_pid=pending["old_pid"],
        status=pending["status"],
      )
    return None

  def _launch_replacement_child(self, spec, *, old_pid, status):
    replacement_pid = self._launch_child(spec)
    log_event(
      "process.replaced",
      backend_alias=self.backend_alias,
      old_pid=old_pid,
      new_pid=replacement_pid,
      exit_status=status,
      kind=spec["kind"],
    )
    return replacement_pid

  def _wait_for_children(self, timeout):
    deadline = time.monotonic() + timeout
    while self.children and time.monotonic() < deadline:
      try:
        pid, status = self._waitpid(-1, os.WNOHANG)
      except ChildProcessError:
        for child_pid, spec in tuple(self.children.items()):
          self._fail_claimed_jobs_for_child(child_pid, spec)
        self.children.clear()
        self._child_started_at.clear()
        return None

      if not pid:
        time.sleep(min(0.05, max(deadline - time.monotonic(), 0)))
        continue

      spec = self.children.pop(pid, None)
      self._child_started_at.pop(pid, None)
      if spec is not None:
        self._fail_claimed_jobs_for_child(pid, spec, status=status)
    return None

  def poll_once(self):
    super().poll_once()
    return self.check_children()

  def _fail_claimed_jobs_for_child(self, pid, spec, *, status=None):
    if spec is None:
      return self._fail_claimed_jobs_for_pid(pid)
    message = _child_exit_message(status)
    with app_executor():
      return fail_claimed_jobs_for_child(
        pid=pid,
        name=spec["kwargs"]["name"],
        supervisor_id=self.process.pk if self.process is not None else None,
        error=ProcessExitError(message),
        traceback_text=message,
        backend_alias=self.backend_alias,
      )

  def _fail_claimed_jobs_for_pid(self, pid):
    with app_executor():
      return fail_claimed_jobs_for_pid(
        pid,
        ProcessExitError("child process exited"),
        traceback_text="child process exited",
        backend_alias=self.backend_alias,
      )

  def _build_runner_specs(self):
    specs = []
    supervisor_id = self.process.pk if self.process else None

    for definition in runner_definitions(self.config):
      specs.append(
        {
          "kind": definition.kind,
          "runner_class": definition.runner_class,
          "kwargs": {
            "config": definition.config,
            "backend_alias": self.backend_alias,
            "name": definition.name,
            "hostname": self.hostname,
            "heartbeat_interval": self.config.process_heartbeat_interval,
            "process_alive_threshold": self.config.process_alive_threshold,
            "supervisor": supervisor_id,
          },
        }
      )

    return specs

  def _default_launcher(self, spec):
    connections.close_all()
    pid = os.fork()
    if pid == 0:
      connections.close_all()
      exit_status = 0
      try:
        runner = spec["runner_class"](**spec["kwargs"])
        self._register_child_signal_handlers(runner)
        runner.run()
      except Exception as error:
        exit_status = 1
        handle_thread_error(
          error,
          context="supervisor.child",
          backend_alias=self.backend_alias,
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


def _child_exit_message(status):
  if status is None:
    return "child process exited"
  if os.WIFEXITED(status):
    return f"child process exited with status {os.WEXITSTATUS(status)}"
  if os.WIFSIGNALED(status):
    return f"child process exited from signal {os.WTERMSIG(status)}"
  return f"child process exited with wait status {status}"
