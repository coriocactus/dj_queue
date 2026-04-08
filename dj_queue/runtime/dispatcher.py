import os
import socket

from django.utils import timezone

from dj_queue.operations.concurrency import (
  cleanup_expired_semaphores,
  promote_expired_blocked_jobs,
)
from dj_queue.operations.jobs import promote_scheduled_jobs
from dj_queue.runtime.base import BaseRunner, app_executor


class Dispatcher(BaseRunner):
  process_kind = "Dispatcher"
  hook_prefix = "dispatcher"

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
  ):
    super().__init__(
      config,
      backend_alias=backend_alias,
      name=name or f"dispatcher-{os.getpid()}",
      pid=pid or os.getpid(),
      hostname=hostname or socket.gethostname(),
      sleeper=sleeper,
      heartbeat_interval=heartbeat_interval,
    )
    self._last_maintenance_at = None

  def poll_once(self):
    if self.process is None:
      self.start()

    with app_executor():
      promoted_jobs = promote_scheduled_jobs(
        batch_size=self.config.batch_size,
        backend_alias=self.backend_alias,
      )
      if self._maintenance_due():
        cleanup_expired_semaphores(backend_alias=self.backend_alias)
        promote_expired_blocked_jobs(
          batch_size=self.config.batch_size,
          backend_alias=self.backend_alias,
        )
        self._last_maintenance_at = timezone.now()
    return promoted_jobs

  def stop(self):
    return super().stop()

  def process_metadata(self):
    return {
      "batch_size": self.config.batch_size,
      "polling_interval": self.config.polling_interval,
      "concurrency_maintenance": self.config.concurrency_maintenance,
      "concurrency_maintenance_interval": self.config.concurrency_maintenance_interval,
    }

  def _maintenance_due(self):
    if not self.config.concurrency_maintenance:
      return False
    if self._last_maintenance_at is None:
      return True
    return (
      timezone.now() - self._last_maintenance_at
    ).total_seconds() >= self.config.concurrency_maintenance_interval
