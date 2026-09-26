import inspect
from dataclasses import dataclass
from functools import lru_cache
from uuid import uuid4

from django.db import transaction
from django.utils import timezone

from dj_queue.config import load_allowed_queues
from dj_queue.db import (
  get_database_alias,
  retry_transient_database_errors,
)
from dj_queue.exceptions import EnqueueError
from dj_queue.log import event_logging_enabled, log_event
from dj_queue.models import (
  BlockedExecution,
  Job,
  ScheduledExecution,
)
from dj_queue.operations._helpers import (
  _bulk_create,
  _bulk_create_ready_executions_locked,
  _normalize_payload,
  _task_option,
)
from dj_queue.operations.concurrency import (
  concurrency_settings,
)
from dj_queue.operations.dispatch import (
  DispatchEntry,
  DispatchOutcome,
  build_dispatch_rows,
  dispatch_decision,
  dispatch_job,
)
from dj_queue.wakeup import notify_ready_queues_on_commit


@dataclass(frozen=True, slots=True)
class _JobSubmission:
  task: object
  fields: dict


def enqueue_job(task, args, kwargs, *, backend_alias="default", job_id=None, config=None):
  job, _ = enqueue_job_with_dispatch(
    task,
    args,
    kwargs,
    backend_alias=backend_alias,
    job_id=job_id,
    config=config,
  )
  return job


def enqueue_job_with_dispatch(
  task,
  args,
  kwargs,
  *,
  backend_alias="default",
  validate=True,
  job_id=None,
  config=None,
):
  submission = _prepare_submission(
    task,
    args,
    kwargs,
    backend_alias=backend_alias,
    validate=validate,
    job_id=job_id,
    config=config,
  )
  alias = get_database_alias(backend_alias, config=config)

  def enqueue_transition():
    with transaction.atomic(using=alias):
      job = Job.objects.using(alias).create(**submission.fields)
      dispatch_outcome = dispatch_job(
        job,
        task=task,
        backend_alias=backend_alias,
        check_conflicts=False,
        config=config,
      )
      return job, dispatch_outcome

  job, dispatch_outcome = retry_transient_database_errors(enqueue_transition)

  if dispatch_outcome.should_notify:
    notify_ready_queues_on_commit((job.queue_name,), backend_alias=backend_alias, config=config)

  if event_logging_enabled(backend_alias=backend_alias):
    log_event(
      "job.enqueued",
      backend_alias=backend_alias,
      job_id=str(job.id),
      task_path=job.task_path,
      queue_name=job.queue_name,
      priority=job.priority,
    )
  return job, dispatch_outcome


def _prepare_submission(task, args, kwargs, *, backend_alias, validate, job_id=None, config=None):
  if validate:
    validate_queue_allowed(task.queue_name, backend_alias=backend_alias, config=config)
    validate_priority(task.priority)
  payload = _normalize_payload(args, kwargs)
  concurrency_key = _resolve_concurrency_key(task, args, kwargs)
  limit, duration, on_conflict = _concurrency_policy(
    task, concurrency_key, backend_alias=backend_alias, config=config
  )
  return _JobSubmission(
    task,
    {
      "id": job_id if job_id is not None else uuid4(),
      "task_path": task.module_path,
      "queue_name": task.queue_name,
      "priority": task.priority,
      "payload": payload,
      "backend_alias": backend_alias,
      "scheduled_at": task.run_after,
      "concurrency_key": concurrency_key,
      "concurrency_limit": limit,
      "concurrency_duration": duration,
      "concurrency_on_conflict": on_conflict,
    },
  )


def enqueue_jobs_bulk(task_calls, *, backend_alias="default", validate=True, config=None):
  submissions = [
    _prepare_submission(
      task, args, kwargs, backend_alias=backend_alias, validate=validate, config=config
    )
    for task, args, kwargs in task_calls
  ]
  if not submissions:
    return []

  alias = get_database_alias(backend_alias, config=config)
  prepared, ready_queue_names = retry_transient_database_errors(
    lambda: _enqueue_bulk_once(
      submissions, alias=alias, backend_alias=backend_alias, config=config
    )
  )
  notify_ready_queues_on_commit(ready_queue_names, backend_alias=backend_alias, config=config)
  _log_bulk_enqueued((entry.outcome for entry in prepared), backend_alias=backend_alias)
  return [
    (entry.job, submission.task, entry.outcome)
    for entry, submission in zip(prepared, submissions, strict=True)
  ]


def _enqueue_bulk_once(submissions, *, alias, backend_alias, config=None):
  now = timezone.now()
  prepared = []
  for submission in submissions:
    job = Job(**submission.fields, created_at=now, updated_at=now)
    prepared.append(
      DispatchEntry(job=job, decision=dispatch_decision(job, now=now, config=config))
    )

  with transaction.atomic(using=alias):
    _bulk_create(alias, Job, [entry.job for entry in prepared])
    rows = build_dispatch_rows(prepared, backend_alias=backend_alias, now=now, config=config)
    _bulk_create_ready_executions_locked(
      alias, rows.ready, backend_alias=backend_alias, check_conflicts=False
    )
    _bulk_create(alias, ScheduledExecution, rows.scheduled)
    _bulk_create(alias, BlockedExecution, rows.blocked)
    if rows.discarded:
      Job.objects.using(alias).bulk_update(
        rows.discarded, ["finished_at", "return_value", "updated_at"]
      )
  return prepared, rows.ready_queue_names


def _log_bulk_enqueued(outcomes, *, backend_alias):
  if not event_logging_enabled(backend_alias=backend_alias):
    return
  counts = {outcome: 0 for outcome in DispatchOutcome}
  job_count = 0
  for outcome in outcomes:
    counts[outcome] += 1
    job_count += 1
  log_event(
    "jobs.enqueued",
    backend_alias=backend_alias,
    job_count=job_count,
    ready_count=counts[DispatchOutcome.READY],
    scheduled_count=counts[DispatchOutcome.SCHEDULED],
    blocked_count=counts[DispatchOutcome.BLOCKED],
    discarded_count=counts[DispatchOutcome.DISCARDED],
  )


def validate_queue_allowed(queue_name, *, backend_alias="default", config=None):
  allowed_queues = load_allowed_queues(backend_alias) if config is None else config.allowed_queues
  if allowed_queues and queue_name not in allowed_queues:
    raise EnqueueError(f"queue {queue_name!r} is not allowed for backend {backend_alias!r}")


def validate_priority(priority):
  if type(priority) is not int or priority < -100 or priority > 100:
    raise EnqueueError("priority must be an integer from -100 to 100")


def _resolve_concurrency_key(task, args, kwargs):
  option = _task_option(task, "concurrency_key")
  if option in (None, ""):
    return None

  if callable(option):
    value = option(*args, **kwargs)
  elif isinstance(option, str):
    try:
      value = option.format(**_bound_arguments(task, args, kwargs))
    except (IndexError, KeyError, ValueError) as exc:
      raise EnqueueError("could not resolve concurrency_key") from exc
  else:
    raise EnqueueError("concurrency_key must be a string or callable")

  if not isinstance(value, str) or not value or len(value) > 255:
    raise EnqueueError("concurrency_key must resolve to a non-empty string up to 255 chars")
  return value


def _concurrency_policy(task, concurrency_key, *, backend_alias, config=None):
  if concurrency_key is None:
    return None, None, None
  return concurrency_settings(task, backend_alias=backend_alias, config=config)


def _bound_arguments(task, args, kwargs):
  signature = _task_call_signature(task.func, task.takes_context)
  bound = signature.bind(*args, **kwargs)
  bound.apply_defaults()
  return bound.arguments


@lru_cache(maxsize=1024)
def _task_call_signature(func, takes_context):
  signature = inspect.signature(func)
  parameters = tuple(signature.parameters.values())
  if takes_context and parameters:
    signature = signature.replace(parameters=parameters[1:])
  return signature
