import os
import socket

from dj_queue.config import load_backend_config
from dj_queue.operations.jobs import claim_ready_jobs, execute_claimed_job
from dj_queue.runtime.base import BaseRunner, app_executor, handle_thread_error
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
    if self.process is None:
      self.start()

    idle_capacity = self.pool.idle_capacity
    if idle_capacity <= 0:
      return []

    with app_executor():
      claimed_jobs = claim_ready_jobs(
        limit=idle_capacity,
        queues=self.config.queues,
        process=self.process,
        backend_alias=self.backend_alias,
      )

    for job in claimed_jobs:
      future = self.pool.submit(self._execute_job, job.id)
      future.add_done_callback(self._handle_future)
    return claimed_jobs

  def stop(self, *, timeout=None):
    if timeout is None:
      timeout = load_backend_config(self.backend_alias).shutdown_timeout

    process = self._begin_stop()
    if process is None:
      return True

    drained = self.pool.shutdown(timeout)
    self.wakeup_backend.stop()
    self._finish_stop(process)
    return drained

  def process_metadata(self):
    return {
      "queues": list(self.config.queues),
      "threads": self.config.threads,
      "polling_interval": self.config.polling_interval,
    }

  def _execute_job(self, job_id):
    with app_executor():
      return execute_claimed_job(job_id, backend_alias=self.backend_alias)

  def _handle_future(self, future):
    try:
      future.result()
    except Exception as exc:
      handle_thread_error(exc, context="worker.execute", backend_alias=self.backend_alias)
