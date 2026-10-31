from django.db import transaction

from dj_queue.db import locked_queryset
from dj_queue.exceptions import exception_path
from dj_queue.log import event_logging_enabled, log_event
from dj_queue.models import (
  ClaimedExecution,
  FailedExecution,
  Process,
)
from dj_queue.operations._helpers import (
  _ensure_job_ids_have_no_other_execution_state,
)
from dj_queue.operations.concurrency import (
  release_concurrency_slot,
  release_recovered_concurrency_slots,
)


def fail_orphaned_claimed_jobs(error, *, config, traceback_text="", batch_size=500):
  alias = config.database_alias
  with transaction.atomic(using=alias):
    claimed_rows = list(
      locked_queryset(
        ClaimedExecution.objects.using(alias)
        .select_related("job")
        .filter(process__isnull=True, job__backend_alias=config.backend_alias)
        .order_by("id"),
        use_skip_locked=config.use_skip_locked,
      )[:batch_size]
    )
    return _fail_claimed_jobs(
      [claimed.job for claimed in claimed_rows],
      error,
      traceback_text=traceback_text,
      config=config,
    )


def fail_claimed_jobs_for_process(
  process,
  error,
  *,
  config,
  traceback_text="",
  delete_process=False,
):
  if process is None:
    return []

  alias = config.database_alias
  jobs = [
    claimed.job
    for claimed in (
      ClaimedExecution.objects.using(alias).select_related("job").filter(process_id=process.id)
    )
  ]
  failed_jobs = _fail_claimed_jobs(jobs, error, traceback_text=traceback_text, config=config)
  if delete_process:
    process.delete(using=alias)
  return failed_jobs


def fail_claimed_jobs_for_pid(pid, error, *, config, traceback_text=""):
  processes = list(
    Process.objects.using(config.database_alias).filter(
      pid=pid, backend_alias=config.backend_alias
    )[:2]
  )
  if len(processes) != 1:
    return []
  return fail_claimed_jobs_for_process(
    processes[0],
    error,
    traceback_text=traceback_text,
    delete_process=True,
    config=config,
  )


def fail_claimed_jobs_for_child(*, pid, name, supervisor_id, error, config, traceback_text=""):
  process = (
    Process.objects.using(config.database_alias)
    .filter(
      pid=pid,
      name=name,
      supervisor_id=supervisor_id,
      backend_alias=config.backend_alias,
    )
    .first()
  )
  return fail_claimed_jobs_for_process(
    process,
    error,
    traceback_text=traceback_text,
    delete_process=True,
    config=config,
  )


def prune_stale_processes(
  *,
  cutoff,
  error,
  config,
  traceback_text="",
  exclude_process=None,
  batch_size=500,
):
  alias = config.database_alias
  backend_alias = config.backend_alias
  pruned_processes = []

  with transaction.atomic(using=alias):
    queryset = Process.objects.using(alias).filter(
      backend_alias=backend_alias,
      last_heartbeat_at__lt=cutoff,
    )
    if exclude_process is not None:
      queryset = queryset.exclude(pk=exclude_process.pk)

    stale_processes = list(
      locked_queryset(
        queryset.order_by("last_heartbeat_at", "id"),
        use_skip_locked=config.use_skip_locked,
      )[:batch_size]
    )
    if not stale_processes:
      return []

    remaining = batch_size
    for process in stale_processes:
      claimed_rows = list(
        ClaimedExecution.objects.using(alias)
        .select_related("job")
        .filter(process_id=process.id)
        .order_by("id")[:remaining]
      )
      jobs = [claimed.job for claimed in claimed_rows]
      deleted, _ = (
        Process.objects.using(alias)
        .filter(
          pk=process.pk,
          backend_alias=backend_alias,
          last_heartbeat_at__lt=cutoff,
        )
        .delete()
      )
      if not deleted:
        continue

      _fail_claimed_jobs(jobs, error, traceback_text=traceback_text, config=config)
      pruned_processes.append(process)
      remaining -= len(jobs)
      if remaining == 0:
        break
    return pruned_processes


def _fail_claimed_jobs(jobs, error, *, traceback_text, config):
  jobs = list(jobs)
  if not jobs:
    return []
  backend_alias = config.backend_alias
  for job in jobs:
    if job.backend_alias != backend_alias:
      raise ClaimedExecution.DoesNotExist

  alias = config.database_alias
  job_ids = [job.id for job in jobs]
  exception_class = exception_path(error)
  message = str(error)

  with transaction.atomic(using=alias):
    deleted, _ = ClaimedExecution.objects.using(alias).filter(job_id__in=job_ids).delete()
    if deleted != len(job_ids):
      raise ClaimedExecution.DoesNotExist
    _ensure_job_ids_have_no_other_execution_state(
      alias,
      job_ids,
      ignored_models=(ClaimedExecution,),
    )
    FailedExecution.objects.using(alias).bulk_create(
      [
        FailedExecution(
          job_id=job.id,
          exception_class=exception_class,
          message=message,
          traceback=traceback_text,
        )
        for job in jobs
      ]
    )

    for job in release_recovered_concurrency_slots(jobs, config=config):
      release_concurrency_slot(job, config=config)

  if event_logging_enabled(backend_alias=backend_alias):
    for job in jobs:
      log_event(
        "job.failed",
        backend_alias=backend_alias,
        job_id=str(job.id),
        exception_class=exception_class,
        message=message,
      )
  return jobs
