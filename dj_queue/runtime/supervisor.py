import os
import socket

from django.utils import timezone
from datetime import timedelta

from dj_queue.config import load_backend_config
from dj_queue.exceptions import ProcessMissingError, ProcessPrunedError
from dj_queue.models import ClaimedExecution, Process
from dj_queue.operations.jobs import fail_claimed_job
from dj_queue.runtime.base import BaseRunner, app_executor


class Supervisor(BaseRunner):
  process_kind = "Supervisor"
  hook_prefix = "supervisor"

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
    process = super().start()
    self.fail_startup_orphaned_jobs()
    return process

  def poll_once(self):
    return []

  def process_metadata(self):
    return {
      "mode": self.config.mode,
      "standalone": self.standalone,
      "worker_count": len(self.config.workers),
      "dispatcher_count": len(self.config.dispatchers),
      "has_scheduler": self.config.scheduler is not None,
    }

  def fail_startup_orphaned_jobs(self):
    orphaned_job_ids = list(
      ClaimedExecution.objects.filter(process__isnull=True).values_list("job_id", flat=True)
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
    cutoff = now - timedelta(seconds=self.config.process_alive_threshold)
    queryset = Process.objects.filter(last_heartbeat_at__lt=cutoff)
    if self.process is not None:
      queryset = queryset.exclude(pk=self.process.pk)

    stale_processes = list(queryset.order_by("last_heartbeat_at", "id"))
    pruned_processes = []
    for process in stale_processes:
      claimed_job_ids = list(
        ClaimedExecution.objects.filter(process=process).values_list("job_id", flat=True)
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
