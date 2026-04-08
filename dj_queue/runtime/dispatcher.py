import os
import socket

from django.utils import timezone

from dj_queue.db import get_database_alias
from dj_queue.models import Process
from dj_queue.operations.concurrency import (
  cleanup_expired_semaphores,
  promote_expired_blocked_jobs,
)
from dj_queue.operations.jobs import promote_scheduled_jobs
from dj_queue.runtime.base import app_executor


class Dispatcher:
  def __init__(
    self,
    config,
    *,
    backend_alias="default",
    name=None,
    pid=None,
    hostname=None,
  ):
    self.config = config
    self.backend_alias = backend_alias
    self.name = name or f"dispatcher-{os.getpid()}"
    self.pid = pid or os.getpid()
    self.hostname = hostname or socket.gethostname()
    self.process = None
    self._last_maintenance_at = None

  def start(self):
    if self.process is None:
      self.process = self._register_process()
    return self.process

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
    self._deregister_process()

  def _maintenance_due(self):
    if not self.config.concurrency_maintenance:
      return False
    if self._last_maintenance_at is None:
      return True
    return (
      timezone.now() - self._last_maintenance_at
    ).total_seconds() >= self.config.concurrency_maintenance_interval

  def _register_process(self):
    alias = get_database_alias(self.backend_alias)
    return Process.objects.using(alias).create(
      kind="Dispatcher",
      pid=self.pid,
      hostname=self.hostname,
      name=self.name,
      metadata={
        "batch_size": self.config.batch_size,
        "polling_interval": self.config.polling_interval,
        "concurrency_maintenance": self.config.concurrency_maintenance,
        "concurrency_maintenance_interval": self.config.concurrency_maintenance_interval,
      },
      last_heartbeat_at=timezone.now(),
    )

  def _deregister_process(self):
    if self.process is None:
      return

    alias = get_database_alias(self.backend_alias)
    Process.objects.using(alias).filter(pk=self.process.pk).delete()
    self.process = None
