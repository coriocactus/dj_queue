import json

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

STATE_RELATIONS = {
  ReadyExecution: "ready_execution",
  ScheduledExecution: "scheduled_execution",
  ClaimedExecution: "claimed_execution",
  BlockedExecution: "blocked_execution",
  FailedExecution: "failed_execution",
}


def _normalize_payload(args, kwargs):
  try:
    return json.loads(json.dumps({"args": list(args), "kwargs": dict(kwargs)}))
  except (TypeError, ValueError) as exc:
    raise EnqueueError("payload must be JSON round-trippable") from exc


def _ensure_no_other_execution_state(alias, job, *, ignored_models=()):
  if _job_ids_with_other_execution_state(alias, [job.pk], ignored_models=ignored_models):
    raise EnqueueError(f"job {job.id} already has an execution-state row")


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
    "job": job,
    "backend_alias": backend_alias,
    "queue_name": _execution_queue_name(job, queue_name),
    "priority": _execution_priority(job, priority),
    "latency_started_at": ready_at,
  }
  if created_at is not None:
    fields["created_at"] = created_at
  return fields


def _scheduled_execution_fields(job, *, backend_alias, scheduled_at=None, created_at=None):
  fields = {
    "job": job,
    "backend_alias": backend_alias,
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
    "job": job,
    "backend_alias": backend_alias,
    "queue_name": _execution_queue_name(job, queue_name),
    "priority": _execution_priority(job, priority),
    "concurrency_key": concurrency_key if concurrency_key is not None else job.concurrency_key,
    "expires_at": expires_at,
  }


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
