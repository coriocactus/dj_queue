from datetime import timedelta

from django.db import transaction
from django.utils import timezone

from dj_queue.config import load_backend_config
from dj_queue.db import get_database_alias, locked_queryset
from dj_queue.models import FailedExecution, Job, RecurringExecution
from dj_queue.operations._helpers import (
  _consume_selected_rows,
  _ensure_job_ids_have_no_other_execution_state,
)


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

  with transaction.atomic(using=alias):
    job_ids = list(queryset.values_list("pk", flat=True)[:batch_size])
    if not job_ids:
      return 0

    _ensure_job_ids_have_no_other_execution_state(alias, job_ids)
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

  with transaction.atomic(using=alias):
    queryset = (
      FailedExecution.objects.using(alias)
      .filter(job__backend_alias=backend_alias, created_at__lt=cutoff)
      .order_by("created_at", "job_id")
    )
    if task_path is not None:
      queryset = queryset.filter(job__task_path=task_path)

    failed_rows = list(
      locked_queryset(queryset, use_skip_locked=config.use_skip_locked)[:batch_size]
    )
    if not failed_rows:
      return 0

    failed_rows = _consume_selected_rows(alias, FailedExecution, failed_rows)
    if not failed_rows:
      return 0

    job_ids = [failed.job_id for failed in failed_rows]
    _ensure_job_ids_have_no_other_execution_state(alias, job_ids)
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
