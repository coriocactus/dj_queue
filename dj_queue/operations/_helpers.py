import json

from django.db import connections
from django.db.models import Q

from dj_queue.db import database_capabilities
from dj_queue.exceptions import EnqueueError
from dj_queue.models import (
  BlockedExecution,
  ClaimedExecution,
  FailedExecution,
  Job,
  Pause,
  ReadyExecution,
  ScheduledExecution,
)

EXECUTION_STATE_MODELS = (
  ReadyExecution,
  ScheduledExecution,
  ClaimedExecution,
  BlockedExecution,
  FailedExecution,
)
STATE_RELATIONS = {
  model: relation_name
  for model, relation_name in (
    (ReadyExecution, "ready_execution"),
    (ScheduledExecution, "scheduled_execution"),
    (ClaimedExecution, "claimed_execution"),
    (BlockedExecution, "blocked_execution"),
    (FailedExecution, "failed_execution"),
  )
}


def _normalize_payload(args, kwargs):
  try:
    return json.loads(json.dumps({"args": list(args), "kwargs": dict(kwargs)}))
  except (TypeError, ValueError) as exc:
    raise EnqueueError("payload must be JSON round-trippable") from exc


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


def _state_models_except(*ignored_models):
  ignored = set(ignored_models)
  return tuple(model for model in EXECUTION_STATE_MODELS if model not in ignored)


def _state_absence_checks_sql(models, *, quote, job_id_expression):
  return " AND ".join(
    _state_absence_sql(model, quote=quote, job_id_expression=job_id_expression) for model in models
  )


def _state_absence_sql(model, *, quote, job_id_expression):
  state_table = quote(model._meta.db_table)
  state_job_id_column = quote(model._meta.get_field("job").column)
  return (
    f"NOT EXISTS ("
    f"SELECT 1 FROM {state_table} "
    f"WHERE {state_table}.{state_job_id_column} = {job_id_expression}"
    f")"
  )


def _task_option(task, name, default=None):
  if hasattr(task, name):
    return getattr(task, name)
  return getattr(task.func, name, default)


def _lock_active_pauses(alias, backend_alias, queue_names=None):
  queryset = Pause.objects.using(alias).select_for_update().filter(backend_alias=backend_alias)
  if queue_names is not None:
    active_queue_names = tuple(queue_name for queue_name in queue_names if queue_name)
    if not active_queue_names:
      return set()
    queryset = queryset.filter(queue_name__in=active_queue_names)
  return set(queryset.values_list("queue_name", flat=True))


def _exclude_active_pauses(queryset, alias, backend_alias):
  paused_queue_names = (
    Pause.objects.using(alias).filter(backend_alias=backend_alias).values("queue_name")
  )
  return queryset.exclude(queue_name__in=paused_queue_names)


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
  queue_name=None,
  priority=None,
  ready_at=None,
  created_at=None,
):
  queue_name = _execution_queue_name(job, queue_name)
  return _create_ready_execution_locked(
    alias,
    job,
    backend_alias=backend_alias,
    queue_name=queue_name,
    priority=priority,
    ready_at=ready_at,
    created_at=created_at,
    check_conflicts=True,
  )


def _create_ready_execution_locked(
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
  _lock_active_pauses(alias, backend_alias, {queue_name})
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


def _bulk_create_ready_executions_locked(alias, ready_rows, *, backend_alias, check_conflicts):
  ready_rows = tuple(ready_rows)
  if not ready_rows:
    return None

  if check_conflicts:
    _ensure_job_ids_have_no_other_execution_state(alias, [row.job_id for row in ready_rows])
  _lock_active_pauses(alias, backend_alias, {row.queue_name for row in ready_rows})
  return _bulk_create(alias, ReadyExecution, ready_rows)


def _ready_execution_rows(jobs, *, backend_alias, ready_at=None, created_at=None):
  return [
    _ready_execution_row(
      job,
      backend_alias=backend_alias,
      ready_at=ready_at,
      created_at=created_at,
    )
    for job in jobs
  ]


def _scheduled_execution_row(job, *, backend_alias, scheduled_at=None, created_at=None):
  return ScheduledExecution(
    **_scheduled_execution_fields(
      job,
      backend_alias=backend_alias,
      scheduled_at=scheduled_at,
      created_at=created_at,
    )
  )


def _create_scheduled_execution(
  alias, job, *, backend_alias, scheduled_at=None, check_conflicts=True
):
  if check_conflicts:
    _ensure_no_other_execution_state(alias, job)
  return ScheduledExecution.objects.using(alias).create(
    **_scheduled_execution_fields(
      job,
      backend_alias=backend_alias,
      scheduled_at=scheduled_at,
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
):
  return {
    "job_id": job.id,
    "backend_alias": _execution_backend_alias(job, backend_alias),
    "queue_name": _execution_queue_name(job, queue_name),
    "priority": _execution_priority(job, priority),
    "concurrency_key": concurrency_key if concurrency_key is not None else job.concurrency_key,
    "expires_at": expires_at,
  }


def _execution_backend_alias(job, backend_alias):
  if job.backend_alias != backend_alias:
    raise EnqueueError(f"job {job.id} belongs to backend {job.backend_alias!r}")
  return job.backend_alias


def _execution_queue_name(job, queue_name):
  return job.queue_name if queue_name is None else queue_name


def _execution_priority(job, priority):
  return job.priority if priority is None else priority


def _consume_selected_rows(alias, model, rows):
  if not database_capabilities(alias).uses_serialized_writes:
    model.objects.using(alias).filter(pk__in=[row.pk for row in rows]).delete()
    return rows

  consumed_rows = []
  for row in rows:
    deleted, _ = model.objects.using(alias).filter(pk=row.pk).delete()
    if deleted:
      consumed_rows.append(row)
  return consumed_rows


def _bulk_create(alias, model, objects):
  objects = tuple(objects)
  if not objects:
    return None

  fields = [field for field in model._meta.concrete_fields if not field.generated]
  batch_size = connections[alias].ops.bulk_batch_size(fields, objects)
  if batch_size is None or batch_size <= 0:
    batch_size = len(objects)
  model.objects.using(alias).bulk_create(objects, batch_size=batch_size)
  return None
