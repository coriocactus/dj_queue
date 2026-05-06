import os
import signal
import socket
import threading
import time

from django.utils import timezone
from datetime import timedelta

from dj_queue.config import load_backend_config
from dj_queue.db import get_database_alias
from dj_queue.exceptions import ProcessExitError, ProcessMissingError, ProcessPrunedError
from dj_queue.log import log_event
from dj_queue.models import ClaimedExecution, Process
from dj_queue.operations.jobs import fail_claimed_job
from dj_queue.runtime.base import BaseRunner, app_executor
from dj_queue.runtime.dispatcher import Dispatcher
from dj_queue.runtime.errors import handle_thread_error
from dj_queue.runtime.pidfile import PidFile
from dj_queue.runtime.scheduler import Scheduler
from dj_queue.runtime.worker import Worker


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
    )
    self.standalone = standalone
    self.pidfile = None
    self._last_housekeeping_at = None

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
    self._acquire_pidfile()
    process = super().start()
    self.fail_startup_orphaned_jobs()
    return process

  def poll_once(self):
    pruned_processes = []
    if self._housekeeping_due():
      pruned_processes = self.prune_stale_process_rows()
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
    alias = get_database_alias(self.backend_alias)
    orphaned_job_ids = list(
      ClaimedExecution.objects.using(alias)
      .filter(process__isnull=True, job__backend_alias=self.backend_alias)
      .values_list("job_id", flat=True)
    )
    failed_jobs = []
    with app_executor():
      for job_id in orphaned_job_ids:
        failed_jobs.append(
          fail_claimed_job(
            job_id,
            ProcessMissingError("process no longer registered at supervisor startup"),
            traceback_text="process no longer registered at supervisor startup",
            backend_alias=self.backend_alias,
          )
        )
    return failed_jobs

  def prune_stale_process_rows(self, *, now=None):
    if now is None:
      now = timezone.now()
    alias = get_database_alias(self.backend_alias)
    cutoff = now - timedelta(seconds=self.config.process_alive_threshold)
    queryset = Process.objects.using(alias).filter(
      backend_alias=self.backend_alias,
      last_heartbeat_at__lt=cutoff,
    )
    if self.process is not None:
      queryset = queryset.exclude(pk=self.process.pk)

    stale_processes = list(queryset.order_by("last_heartbeat_at", "id"))
    pruned_processes = []
    for process in stale_processes:
      claimed_job_ids = list(
        ClaimedExecution.objects.using(alias)
        .filter(process=process)
        .values_list("job_id", flat=True)
      )
      with app_executor():
        for job_id in claimed_job_ids:
          fail_claimed_job(
            job_id,
            ProcessPrunedError("process heartbeat expired"),
            traceback_text="process heartbeat expired",
            backend_alias=self.backend_alias,
          )
      process.delete()
      pruned_processes.append(process)
    return pruned_processes


class AsyncSupervisor(Supervisor):
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.runners = []
    self.runner_threads = []
    self._graceful_shutdown_requested = False
    self._exit_fn = os._exit

  def start(self):
    process = super().start()
    if self.standalone:
      self.register_signal_handlers()
    self.start_runners()
    return process

  def stop(self):
    self._stop_event.set()
    for runner in self.runners:
      runner.request_stop()
    for thread in self.runner_threads:
      thread.join(timeout=1)

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
    self.stop()
    return True

  def handle_sigquit(self, *_args):
    self._exit_fn(1)

  def start_runners(self):
    if self.runners:
      return self.runners

    for runner in self._build_runners():
      runner.start()
      thread = threading.Thread(target=self._run_managed_runner, args=(runner,), daemon=True)
      self.runners.append(runner)
      self.runner_threads.append(thread)
      thread.start()
    return self.runners

  def _run_managed_runner(self, runner):
    try:
      while not self._stop_event.is_set():
        while not runner._stop_event.is_set() and not self._stop_event.is_set():
          try:
            runner.poll_once()
            runner.sleeper.sleep(runner.polling_interval)
          except Exception as error:
            handle_thread_error(
              error,
              context=f"{runner.hook_prefix}.run",
              backend_alias=self.backend_alias,
            )
            break

        if self._stop_event.is_set():
          return

        # runner crashed — fail its claimed jobs, then stop and replace
        self._fail_crashed_runner_jobs(runner)
        runner.stop()
        if self._stop_event.is_set():
          return
        log_event(
          "process.replaced",
          backend_alias=self.backend_alias,
          process_name=runner.name,
          kind=runner.process_kind,
        )
        replacement = self._rebuild_runner(runner)
        self._replace_runner(runner, replacement)
        if self._stop_event.is_set():
          return
        runner = replacement
        runner.start()
    finally:
      if not self._stop_event.is_set():
        runner.stop()

  def _replace_runner(self, current, replacement):
    try:
      index = self.runners.index(current)
    except ValueError:
      return
    self.runners[index] = replacement

  def _fail_crashed_runner_jobs(self, runner):
    if runner.process is None:
      return
    alias = get_database_alias(self.backend_alias)
    claimed_job_ids = list(
      ClaimedExecution.objects.using(alias)
      .filter(process=runner.process)
      .values_list("job_id", flat=True)
    )
    with app_executor():
      for job_id in claimed_job_ids:
        fail_claimed_job(
          job_id,
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
      "supervisor": self.process,
    }
    return runner.__class__(**kwargs)

  def _build_runners(self):
    runners = []

    for index, worker_config in enumerate(self.config.workers, start=1):
      for process_index in range(worker_config.processes):
        suffix = index if worker_config.processes == 1 else f"{index}-{process_index + 1}"
        runners.append(
          Worker(
            worker_config,
            backend_alias=self.backend_alias,
            name=f"worker-{suffix}",
            pid=self.pid,
            hostname=self.hostname,
            supervisor=self.process,
          )
        )

    for index, dispatcher_config in enumerate(self.config.dispatchers, start=1):
      runners.append(
        Dispatcher(
          dispatcher_config,
          backend_alias=self.backend_alias,
          name=f"dispatcher-{index}",
          pid=self.pid,
          hostname=self.hostname,
          supervisor=self.process,
        )
      )

    if self.config.scheduler is not None:
      runners.append(
        Scheduler(
          self.config,
          backend_alias=self.backend_alias,
          name="scheduler-1",
          pid=self.pid,
          hostname=self.hostname,
          supervisor=self.process,
        )
      )

    return runners


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
    self._graceful_shutdown_requested = False
    self._launcher = launcher or self._default_launcher
    self._waitpid = waitpid or os.waitpid
    self._killer = killer or os.kill
    self._exit_fn = exit_fn or os._exit

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
    process = super().start()
    if self.standalone:
      self.register_signal_handlers()
    self.start_children()
    return process

  def stop(self):
    for pid in tuple(self.children):
      try:
        self._killer(pid, signal.SIGTERM)
      except ProcessLookupError:
        pass
    self.children.clear()
    return super().stop()

  def register_signal_handlers(self):
    signal.signal(signal.SIGTERM, self.handle_sigterm)
    signal.signal(signal.SIGINT, self.handle_sigterm)
    signal.signal(signal.SIGQUIT, self.handle_sigquit)

  def handle_sigterm(self, *_args):
    if self._graceful_shutdown_requested:
      return False

    self._graceful_shutdown_requested = True
    self.stop()
    return True

  def handle_sigquit(self, *_args):
    self._exit_fn(1)

  def start_children(self):
    if self.children:
      return self.children

    for spec in self._build_runner_specs():
      pid = self._launcher(spec)
      self.children[pid] = spec
    return self.children

  def check_children(self):
    try:
      pid, _status = self._waitpid(-1, os.WNOHANG)
    except ChildProcessError:
      return None

    if not pid:
      return None

    spec = self.children.pop(pid)
    self._fail_claimed_jobs_for_pid(pid)
    replacement_pid = self._launcher(spec)
    self.children[replacement_pid] = spec
    log_event(
      "process.replaced",
      backend_alias=self.backend_alias,
      old_pid=pid,
      new_pid=replacement_pid,
      kind=spec["kind"],
    )
    return replacement_pid

  def poll_once(self):
    super().poll_once()
    return self.check_children()

  def _fail_claimed_jobs_for_pid(self, pid):
    alias = get_database_alias(self.backend_alias)
    process = (
      Process.objects.using(alias).filter(pid=pid, backend_alias=self.backend_alias).first()
    )
    if process is None:
      return []

    claimed_job_ids = list(
      ClaimedExecution.objects.using(alias)
      .filter(process=process)
      .values_list("job_id", flat=True)
    )
    failed_jobs = []
    with app_executor():
      for job_id in claimed_job_ids:
        failed_jobs.append(
          fail_claimed_job(
            job_id,
            ProcessExitError("child process exited"),
            traceback_text="child process exited",
            backend_alias=self.backend_alias,
          )
        )
    process.delete()
    return failed_jobs

  def _build_runner_specs(self):
    specs = []

    for index, worker_config in enumerate(self.config.workers, start=1):
      for process_index in range(worker_config.processes):
        suffix = index if worker_config.processes == 1 else f"{index}-{process_index + 1}"
        specs.append(
          {
            "kind": "worker",
            "runner_class": Worker,
            "kwargs": {
              "config": worker_config,
              "backend_alias": self.backend_alias,
              "name": f"worker-{suffix}",
              "hostname": self.hostname,
            },
          }
        )

    for index, dispatcher_config in enumerate(self.config.dispatchers, start=1):
      specs.append(
        {
          "kind": "dispatcher",
          "runner_class": Dispatcher,
          "kwargs": {
            "config": dispatcher_config,
            "backend_alias": self.backend_alias,
            "name": f"dispatcher-{index}",
            "hostname": self.hostname,
          },
        }
      )

    if self.config.scheduler is not None:
      specs.append(
        {
          "kind": "scheduler",
          "runner_class": Scheduler,
          "kwargs": {
            "config": self.config,
            "backend_alias": self.backend_alias,
            "name": "scheduler-1",
            "hostname": self.hostname,
          },
        }
      )

    return specs

  def _default_launcher(self, spec):
    pid = os.fork()
    if pid == 0:
      runner = spec["runner_class"](**spec["kwargs"])
      runner.run()
      self._exit_fn(0)
    return pid
