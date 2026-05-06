import os
import socket
from datetime import timedelta

from croniter import croniter
from django.db.models import Q
from django.utils import timezone

from dj_queue.config import load_backend_config
from dj_queue.db import get_database_alias
from dj_queue.models import RecurringTask
from dj_queue.operations.cleanup import (
  clear_failed_jobs,
  clear_finished_jobs,
  clear_recurring_executions,
)
from dj_queue.operations.recurring import fire_recurring_task, upsert_static_recurring_tasks
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
      supervisor=supervisor,
    )

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

  def poll_once(self, *, now=None):
    if now is None:
      now = timezone.now()
    if self.process is None:
      self.start()

    with app_executor():
      self.sync_static_tasks()
      fired_jobs = self._fire_due_tasks(now)
      self._run_cleanup(now)
    return fired_jobs

  def _fire_due_tasks(self, now):
    alias = get_database_alias(self.backend_alias)
    queryset = (
      RecurringTask.objects.using(alias)
      .filter(backend_alias=self.backend_alias)
      .filter(Q(next_run_at__isnull=True) | Q(next_run_at__lte=now))
      .order_by("next_run_at", "key")
    )
    if not self.config.scheduler.dynamic_tasks_enabled:
      queryset = queryset.filter(static=True)

    fired_jobs = []
    for recurring_task in queryset:
      run_at = _latest_run_at(recurring_task.schedule, now)
      if run_at is None:
        continue
      execution = fire_recurring_task(recurring_task, run_at, backend_alias=self.backend_alias)
      if execution is not None and execution.job_id is not None:
        fired_jobs.append(execution.job)
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


def _latest_run_at(schedule, now):
  iterator = croniter(schedule, now + timedelta(seconds=1))
  return iterator.get_prev(type(now))
