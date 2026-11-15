from django.db.models import Q

from dj_queue.db import database_capabilities
from dj_queue.exceptions import EnqueueError, exception_path
from dj_queue.models import (
  BlockedExecution,
  FailedExecution,
  Job,
  ReadyExecution,
  ScheduledExecution,
)
from dj_queue.models.jobs import job_status_relation_names
from dj_queue.sql.state import EXECUTION_STATE_MODELS

STATE_RELATIONS = {
  model: relation_name
  for model, relation_name in zip(EXECUTION_STATE_MODELS, job_status_relation_names())
}


def _ensure_no_other_execution_state(alias, job, *, ignored_models=()):
  _ensure_job_ids_have_no_other_execution_state(
    alias,
    [job.pk],
    ignored_models=ignored_models,
  )


def _ensure_job_ids_have_no_other_execution_state(alias, job_ids, *, ignored_models=()):
  conflicting_job_ids = _job_ids_with_other_execution_state(
    alias,
    job_ids,
    ignored_models=ignored_models,
  )
  if conflicting_job_ids:
    conflicting_job_id = next(iter(conflicting_job_ids))
    raise EnqueueError(f"job {conflicting_job_id} already has an execution-state row")


def _job_ids_with_other_execution_state(alias, job_ids, *, ignored_models=()):
  relation_names = [
    relation_name
    for model, relation_name in STATE_RELATIONS.items()
    if model not in ignored_models
  ]
  if not relation_names:
    return set()
  conflict_query = Q(**{f"{relation_names[0]}__isnull": False})
  for relation_name in relation_names[1:]:
    conflict_query |= Q(**{f"{relation_name}__isnull": False})
  return set(
    Job.objects.using(alias)
    .filter(pk__in=job_ids)
    .filter(conflict_query)
    .values_list("pk", flat=True)
  )


def _ensure_state_rows_belong_to_backend(rows, backend_alias):
  for row in rows:
    if row.job.backend_alias != backend_alias:
      raise EnqueueError(f"job {row.job_id} belongs to backend {row.job.backend_alias!r}")


def _ready_execution_row(
  job,
  *,
  backend_alias,
  queue_name=None,
  priority=None,
  ready_at=None,
  created_at=None,
):
  return ReadyExecution(
    **_ready_execution_fields(
      job,
      backend_alias=backend_alias,
      queue_name=queue_name,
      priority=priority,
      ready_at=ready_at,
      created_at=created_at,
    )
  )


def _create_ready_execution(
  alias,
  job,
  *,
  backend_alias,
  queue_name,
  priority=None,
  ready_at=None,
  created_at=None,
  check_conflicts,
):
  if check_conflicts:
    _ensure_no_other_execution_state(alias, job)
  return ReadyExecution.objects.using(alias).create(
    **_ready_execution_fields(
      job,
      backend_alias=backend_alias,
      queue_name=queue_name,
      priority=priority,
      ready_at=ready_at,
      created_at=created_at,
    )
  )


def _bulk_create_ready_executions(alias, ready_rows, *, check_conflicts):
  ready_rows = tuple(ready_rows)
  if not ready_rows:
    return

  if check_conflicts:
    _ensure_job_ids_have_no_other_execution_state(alias, [row.job_id for row in ready_rows])
  ReadyExecution.objects.using(alias).bulk_create(ready_rows)


def _scheduled_execution_row(job, *, backend_alias, scheduled_at=None, created_at=None):
  return ScheduledExecution(
    **_scheduled_execution_fields(
      job,
      backend_alias=backend_alias,
      scheduled_at=scheduled_at,
      created_at=created_at,
    )
  )


def _create_scheduled_execution(alias, job, *, backend_alias, scheduled_at=None):
  return ScheduledExecution.objects.using(alias).create(
    **_scheduled_execution_fields(
      job,
      backend_alias=backend_alias,
      scheduled_at=scheduled_at,
    )
  )


def _blocked_execution_row(job, *, backend_alias, concurrency_key, expires_at, created_at):
  return BlockedExecution(
    **_blocked_execution_fields(
      job,
      backend_alias=backend_alias,
      concurrency_key=concurrency_key,
      expires_at=expires_at,
      created_at=created_at,
    )
  )


def _create_blocked_execution(
  alias,
  job,
  *,
  backend_alias,
  concurrency_key=None,
  expires_at,
  queue_name=None,
  priority=None,
  check_conflicts=True,
):
  if check_conflicts:
    _ensure_no_other_execution_state(alias, job)
  return BlockedExecution.objects.using(alias).create(
    **_blocked_execution_fields(
      job,
      backend_alias=backend_alias,
      concurrency_key=concurrency_key,
      expires_at=expires_at,
      queue_name=queue_name,
      priority=priority,
    )
  )


def _ready_execution_fields(
  job,
  *,
  backend_alias,
  queue_name=None,
  priority=None,
  ready_at=None,
  created_at=None,
):
  fields = {
    "job_id": job.id,
    "backend_alias": _execution_backend_alias(job, backend_alias),
    "queue_name": _execution_queue_name(job, queue_name),
    "priority": _execution_priority(job, priority),
    "latency_started_at": ready_at,
  }
  if created_at is not None:
    fields["created_at"] = created_at
  return fields


def _scheduled_execution_fields(job, *, backend_alias, scheduled_at=None, created_at=None):
  fields = {
    "job_id": job.id,
    "backend_alias": _execution_backend_alias(job, backend_alias),
    "queue_name": job.queue_name,
    "priority": job.priority,
    "scheduled_at": scheduled_at if scheduled_at is not None else job.scheduled_at,
  }
  if created_at is not None:
    fields["created_at"] = created_at
  return fields


def _blocked_execution_fields(
  job,
  *,
  backend_alias,
  concurrency_key=None,
  expires_at,
  queue_name=None,
  priority=None,
  created_at=None,
):
  fields = {
    "job_id": job.id,
    "backend_alias": _execution_backend_alias(job, backend_alias),
    "queue_name": _execution_queue_name(job, queue_name),
    "priority": _execution_priority(job, priority),
    "concurrency_key": concurrency_key if concurrency_key is not None else job.concurrency_key,
    "expires_at": expires_at,
  }
  if created_at is not None:
    fields["created_at"] = created_at
  return fields


def _failed_execution_row(job, error, *, traceback_text=""):
  return FailedExecution(
    job_id=job.id,
    exception_class=exception_path(error),
    message=str(error),
    traceback=traceback_text,
  )


def _execution_backend_alias(job, backend_alias):
  if job.backend_alias != backend_alias:
    raise EnqueueError(f"job {job.id} belongs to backend {job.backend_alias!r}")
  return job.backend_alias


def _execution_queue_name(job, queue_name):
  return job.queue_name if queue_name is None else queue_name


def _execution_priority(job, priority):
  return job.priority if priority is None else priority


def _consume_selected_rows(alias, model, rows):
  if database_capabilities(alias).backend_family in {"mysql", "mariadb"}:
    from dj_queue.sql.mysql import consume_ready_rows

    if model is ReadyExecution:
      return consume_ready_rows(alias, rows)

  if not database_capabilities(alias).uses_serialized_writes:
    model.objects.using(alias).filter(pk__in=[row.pk for row in rows]).delete()
    return rows

  consumed_rows = []
  for row in rows:
    deleted, _ = model.objects.using(alias).filter(pk=row.pk).delete()
    if deleted:
      consumed_rows.append(row)
  return consumed_rows
