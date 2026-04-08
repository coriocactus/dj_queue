import os
import socket

from django.utils import timezone

from dj_queue.config import load_backend_config
from dj_queue.db import get_database_alias
from dj_queue.models import Process
from dj_queue.operations.jobs import claim_ready_jobs, execute_claimed_job
from dj_queue.runtime.base import app_executor, handle_thread_error
from dj_queue.runtime.interruptible import InterruptibleSleeper
from dj_queue.runtime.notify import build_wakeup_backend
from dj_queue.runtime.pool import WorkerPool


class Worker:
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
  ):
    self.config = config
    self.backend_alias = backend_alias
    self.name = name or f"worker-{os.getpid()}"
    self.pid = pid or os.getpid()
    self.hostname = hostname or socket.gethostname()
    self.sleeper = sleeper or InterruptibleSleeper()
    self.pool = pool or WorkerPool(config.threads, wake_up=self.sleeper.wake_up)
    self.wakeup_backend = wakeup_backend or build_wakeup_backend(
      backend_alias=backend_alias,
      queues=config.queues,
      wake_up=self.sleeper.wake_up,
    )
    self.process = None

  def start(self):
    if self.process is None:
      self.process = self._register_process()
    self.wakeup_backend.start()
    return self.process

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

    drained = self.pool.shutdown(timeout)
    self.wakeup_backend.stop()
    self._deregister_process()
    return drained

  def _execute_job(self, job_id):
    with app_executor():
      return execute_claimed_job(job_id, backend_alias=self.backend_alias)

  def _handle_future(self, future):
    try:
      future.result()
    except Exception as exc:
      handle_thread_error(exc, context="worker.execute", backend_alias=self.backend_alias)

  def _register_process(self):
    alias = get_database_alias(self.backend_alias)
    return Process.objects.using(alias).create(
      kind="Worker",
      pid=self.pid,
      hostname=self.hostname,
      name=self.name,
      metadata={
        "queues": list(self.config.queues),
        "threads": self.config.threads,
        "polling_interval": self.config.polling_interval,
      },
      last_heartbeat_at=timezone.now(),
    )

  def _deregister_process(self):
    if self.process is None:
      return

    alias = get_database_alias(self.backend_alias)
    Process.objects.using(alias).filter(pk=self.process.pk).delete()
    self.process = None
