from datetime import timedelta

from django.utils import timezone

from dj_queue.config import load_backend_config
from dj_queue.db import get_database_alias
from dj_queue.models import FailedExecution, Job, RecurringExecution


def clear_finished_jobs(
  *,
  older_than=None,
  task_path=None,
  batch_size=500,
  backend_alias="default",
  now=None,
):
  config = load_backend_config(backend_alias)
  if older_than is None:
    older_than = config.clear_finished_jobs_after
  if older_than is None:
    return 0

  alias = get_database_alias(backend_alias)
  if now is None:
    now = timezone.now()
  cutoff = now - timedelta(seconds=older_than)
  queryset = (
    Job.objects.using(alias)
    .filter(backend_alias=backend_alias, finished_at__lt=cutoff)
    .order_by("finished_at", "id")
  )
  if task_path is not None:
    queryset = queryset.filter(task_path=task_path)

  job_ids = list(queryset.values_list("pk", flat=True)[:batch_size])
  if not job_ids:
    return 0

  Job.objects.using(alias).filter(backend_alias=backend_alias, pk__in=job_ids).delete()
  return len(job_ids)


def clear_failed_jobs(
  *,
  older_than=None,
  task_path=None,
  batch_size=500,
  backend_alias="default",
  now=None,
):
  config = load_backend_config(backend_alias)
  if older_than is None:
    older_than = config.clear_failed_jobs_after
  if older_than is None:
    return 0

  alias = get_database_alias(backend_alias)
  if now is None:
    now = timezone.now()
  cutoff = now - timedelta(seconds=older_than)
  queryset = (
    FailedExecution.objects.using(alias)
    .filter(job__backend_alias=backend_alias, created_at__lt=cutoff)
    .order_by("created_at", "job_id")
  )
  if task_path is not None:
    queryset = queryset.filter(job__task_path=task_path)

  job_ids = list(queryset.values_list("job_id", flat=True)[:batch_size])
  if not job_ids:
    return 0

  Job.objects.using(alias).filter(backend_alias=backend_alias, pk__in=job_ids).delete()
  return len(job_ids)


def clear_recurring_executions(
  *,
  older_than=None,
  task_key=None,
  batch_size=500,
  backend_alias="default",
  now=None,
):
  config = load_backend_config(backend_alias)
  if older_than is None:
    older_than = config.clear_recurring_executions_after
  if older_than is None:
    return 0

  alias = get_database_alias(backend_alias)
  if now is None:
    now = timezone.now()
  cutoff = now - timedelta(seconds=older_than)
  queryset = (
    RecurringExecution.objects.using(alias)
    .filter(backend_alias=backend_alias, run_at__lt=cutoff)
    .order_by("run_at", "id")
  )
  if task_key is not None:
    queryset = queryset.filter(task_key=task_key)

  execution_ids = list(queryset.values_list("pk", flat=True)[:batch_size])
  if not execution_ids:
    return 0

  deleted, _ = (
    RecurringExecution.objects.using(alias)
    .filter(
      backend_alias=backend_alias,
      pk__in=execution_ids,
    )
    .delete()
  )
  return deleted
