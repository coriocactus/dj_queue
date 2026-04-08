import os
import socket
from datetime import timedelta

from croniter import croniter
from django.utils import timezone

from dj_queue.config import load_backend_config
from dj_queue.db import get_database_alias
from dj_queue.models import Process, RecurringTask
from dj_queue.operations.cleanup import clear_finished_jobs
from dj_queue.operations.recurring import fire_recurring_task, upsert_static_recurring_tasks
from dj_queue.runtime.base import app_executor


class Scheduler:
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
    self.name = name or f"scheduler-{os.getpid()}"
    self.pid = pid or os.getpid()
    self.hostname = hostname or socket.gethostname()
    self.process = None

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

  def start(self):
    if self.process is None:
      self.process = self._register_process()
    return self.process

  def stop(self):
    self._deregister_process()

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
    queryset = RecurringTask.objects.using(alias).order_by("key")
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
    if not self.config.preserve_finished_jobs:
      return 0
    if self.config.clear_finished_jobs_after is None:
      return 0
    return clear_finished_jobs(
      older_than=self.config.clear_finished_jobs_after,
      backend_alias=self.backend_alias,
      now=now,
    )

  def _register_process(self):
    alias = get_database_alias(self.backend_alias)
    return Process.objects.using(alias).create(
      kind="Scheduler",
      pid=self.pid,
      hostname=self.hostname,
      name=self.name,
      metadata={
        "dynamic_tasks_enabled": self.config.scheduler.dynamic_tasks_enabled,
        "polling_interval": self.config.scheduler.polling_interval,
        "static_task_count": len(self.config.recurring),
        "cleanup_enabled": self.config.preserve_finished_jobs
        and self.config.clear_finished_jobs_after is not None,
      },
      last_heartbeat_at=timezone.now(),
    )

  def _deregister_process(self):
    if self.process is None:
      return

    alias = get_database_alias(self.backend_alias)
    Process.objects.using(alias).filter(pk=self.process.pk).delete()
    self.process = None


def _latest_run_at(schedule, now):
  iterator = croniter(schedule, now + timedelta(seconds=1))
  return iterator.get_prev(type(now))
