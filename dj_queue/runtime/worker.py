import os
import socket
import threading
import traceback

from django.utils import timezone

from dj_queue.config import load_backend_config
from dj_queue.db import get_database_alias
from dj_queue.exceptions import ProcessExitError
from dj_queue.models import Process
from dj_queue.operations.jobs import claim_ready_jobs, execute_claimed_job, fail_claimed_job
from dj_queue.runtime.base import BaseRunner, app_executor
from dj_queue.runtime.errors import handle_thread_error
from dj_queue.runtime.notify import build_wakeup_backend
from dj_queue.runtime.pool import WorkerPool


class Worker(BaseRunner):
  process_kind = "Worker"
  hook_prefix = "worker"

  def __init__(
    self,
    config,
    *,
    backend_alias="default",
    name=None,
    pid=None,
    hostname=None,
    sleeper=None,
    pool=None,
    wakeup_backend=None,
    heartbeat_interval=None,
    process_alive_threshold=None,
    supervisor=None,
  ):
    resolved_name = name or f"worker-{os.getpid()}"
    resolved_pid = pid or os.getpid()
    resolved_hostname = hostname or socket.gethostname()
    super().__init__(
      config,
      backend_alias=backend_alias,
      name=resolved_name,
      pid=resolved_pid,
      hostname=resolved_hostname,
      sleeper=sleeper,
      heartbeat_interval=heartbeat_interval,
      process_alive_threshold=process_alive_threshold,
      supervisor=supervisor,
    )
    self.pool = pool or WorkerPool(config.threads, wake_up=self.sleeper.wake_up)
    self.wakeup_backend = wakeup_backend or build_wakeup_backend(
      backend_alias=backend_alias,
      queues=config.queues,
      wake_up=self.sleeper.wake_up,
    )

  def start(self):
    process = super().start()
    self.wakeup_backend.start()
    return process

  def poll_once(self):
    if self.stop_requested():
      return []

    if self.process is None:
      self.start()

    idle_capacity = self.pool.idle_capacity
    if idle_capacity <= 0:
      return []
    claim_limit = self._claim_limit(idle_capacity)
    if claim_limit <= 0:
      return []

    with app_executor():
      claimed_jobs = claim_ready_jobs(
        limit=claim_limit,
        queues=self.config.queues,
        process=self.process,
        backend_alias=self.backend_alias,
      )

    submitted_jobs = []
    for claimed_job in claimed_jobs:
      try:
        future = self.pool.submit(self._execute_job, claimed_job)
      except Exception as exc:
        self._handle_submit_error(claimed_job, exc)
        continue
      future.add_done_callback(
        lambda future, claimed_job=claimed_job: self._handle_future(future, claimed_job)
      )
      submitted_jobs.append(claimed_job)
    return submitted_jobs

  def stop(self, *, timeout=None):
    if timeout is None:
      timeout = load_backend_config(self.backend_alias).shutdown_timeout

    process = self._begin_stop(stop_heartbeat=False)
    if process is None:
      return True

    finish_lock = threading.Lock()

    def finish():
      with finish_lock:
        if self.process is None:
          return None
        self._stop_heartbeat_thread()
        with app_executor():
          self._finish_stop(process)
      return None

    self.wakeup_backend.stop(timeout=timeout)
    drained = self.pool.shutdown(timeout, on_drained=finish)
    if drained:
      finish()
    else:
      self._mark_shutdown_draining(process, timeout=timeout)
    return drained

  def process_metadata(self):
    return {
      "queues": list(self.config.queues),
      "threads": self.config.threads,
      "prefetch_multiplier": self.config.prefetch_multiplier,
      "polling_interval": self.config.polling_interval,
    }

  def _claim_limit(self, idle_capacity):
    in_flight = getattr(self.pool, "in_flight", None)
    if in_flight is None:
      max_workers = getattr(self.pool, "max_workers", self.config.threads)
      in_flight = max(0, max_workers - idle_capacity)
    prefetch_limit = self.config.threads * self.config.prefetch_multiplier
    return max(0, prefetch_limit - in_flight)

  def _mark_shutdown_draining(self, process, *, timeout):
    metadata = dict(process.metadata or {})
    metadata.update(
      {
        "shutdown_state": "draining",
        "shutdown_started_at": timezone.now().isoformat(),
        "shutdown_timeout": timeout,
      }
    )
    active_jobs = getattr(self.pool, "in_flight", None)
    if active_jobs is not None:
      metadata["active_jobs"] = active_jobs
    process.metadata = metadata
    alias = get_database_alias(self.backend_alias)
    with app_executor():
      Process.objects.using(alias).filter(pk=process.pk).update(metadata=metadata)

  def _execute_job(self, claimed_job):
    with app_executor():
      return execute_claimed_job(claimed_job, backend_alias=self.backend_alias)

  def _handle_future(self, future, claimed_job=None):
    try:
      future.result()
    except Exception as exc:
      with app_executor():
        if claimed_job is not None:
          self._fail_claimed_job_after_worker_error(claimed_job, exc)
        handle_thread_error(exc, context="worker.execute", backend_alias=self.backend_alias)
      self.request_stop()

  def _handle_submit_error(self, claimed_job, error):
    with app_executor():
      self._fail_claimed_job_after_worker_error(
        claimed_job,
        ProcessExitError("worker stopped before job submission"),
      )
      handle_thread_error(error, context="worker.submit", backend_alias=self.backend_alias)
    self.request_stop()

  def _fail_claimed_job_after_worker_error(self, claimed_job, error):
    try:
      fail_claimed_job(
        claimed_job,
        error,
        traceback_text="".join(
          traceback.format_exception(type(error), error, error.__traceback__)
        ),
        backend_alias=self.backend_alias,
      )
    except Exception as cleanup_error:
      handle_thread_error(
        cleanup_error,
        context="worker.execute.cleanup",
        backend_alias=self.backend_alias,
      )
