import os
import socket

from django.utils import timezone

from dj_queue.config import load_backend_config
from dj_queue.operations.cleanup import (
  clear_failed_jobs,
  clear_finished_jobs,
  clear_recurring_executions,
)
from dj_queue.operations.recurring import fire_due_recurring_tasks, upsert_static_recurring_tasks
from dj_queue.runtime.base import BaseRunner, app_executor


class Scheduler(BaseRunner):
  process_kind = "Scheduler"
  hook_prefix = "scheduler"

  @property
  def polling_interval(self):
    scheduler = getattr(self.config, "scheduler", None)
    return self._normalized_polling_interval(getattr(scheduler, "polling_interval", None))

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
    process_alive_threshold=None,
    supervisor=None,
  ):
    super().__init__(
      config,
      backend_alias=backend_alias,
      name=name or f"scheduler-{os.getpid()}",
      pid=pid or os.getpid(),
      hostname=hostname or socket.gethostname(),
      sleeper=sleeper,
      heartbeat_interval=heartbeat_interval,
      process_alive_threshold=process_alive_threshold,
      supervisor=supervisor,
    )
    self._static_tasks_synced = False

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
  ):
    config = load_backend_config(
      backend_alias,
      tasks_settings=tasks_settings,
      cli_overrides=cli_overrides,
      env=env,
    )
    if config.scheduler is None:
      return None
    return cls(
      config,
      backend_alias=backend_alias,
      name=name,
      pid=pid,
      hostname=hostname,
    )

  def stop(self):
    return super().stop()

  def start(self):
    process = super().start()
    self.ensure_static_tasks_synced()
    return process

  def process_metadata(self):
    return {
      "dynamic_tasks_enabled": self.config.scheduler.dynamic_tasks_enabled,
      "polling_interval": self.config.scheduler.polling_interval,
      "static_task_count": len(self.config.recurring),
      "cleanup_enabled": any(
        (
          self.config.preserve_finished_jobs and self.config.clear_finished_jobs_after is not None,
          self.config.clear_failed_jobs_after is not None,
          self.config.clear_recurring_executions_after is not None,
        )
      ),
    }

  def sync_static_tasks(self):
    upsert_static_recurring_tasks(self.config.recurring, backend_alias=self.backend_alias)

  def ensure_static_tasks_synced(self):
    if self._static_tasks_synced:
      return False
    with app_executor():
      self.sync_static_tasks()
    self._static_tasks_synced = True
    return True

  def poll_once(self, *, now=None):
    if now is None:
      now = timezone.now()
    if self.process is None:
      self.start()
    else:
      self.ensure_static_tasks_synced()

    with app_executor():
      fired_jobs = fire_due_recurring_tasks(
        now,
        include_dynamic_tasks=self.config.scheduler.dynamic_tasks_enabled,
        backend_alias=self.backend_alias,
      )
      self._run_cleanup(now)
    return fired_jobs

  def _run_cleanup(self, now):
    deleted = 0
    if self.config.preserve_finished_jobs and self.config.clear_finished_jobs_after is not None:
      deleted += clear_finished_jobs(
        older_than=self.config.clear_finished_jobs_after,
        backend_alias=self.backend_alias,
        now=now,
      )
    if self.config.clear_failed_jobs_after is not None:
      deleted += clear_failed_jobs(
        older_than=self.config.clear_failed_jobs_after,
        backend_alias=self.backend_alias,
        now=now,
      )
    if self.config.clear_recurring_executions_after is not None:
      deleted += clear_recurring_executions(
        older_than=self.config.clear_recurring_executions_after,
        backend_alias=self.backend_alias,
        now=now,
      )
    return deleted
