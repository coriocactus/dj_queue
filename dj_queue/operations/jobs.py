import inspect
import json
from datetime import timedelta

from django.db import transaction
from django.db.models import Q
from django.utils import timezone
from django.utils.module_loading import import_string

from dj_queue.config import load_backend_config
from dj_queue.db import get_database_alias, locked_queryset
from dj_queue.exceptions import EnqueueError
from dj_queue.log import log_event
from dj_queue.models import (
  BlockedExecution,
  ClaimedExecution,
  FailedExecution,
  Job,
  Pause,
  ReadyExecution,
  ScheduledExecution,
)
from dj_queue.operations.concurrency import (
  semaphore_acquire,
  semaphore_release,
  unblock_next_blocked_job,
)


def enqueue_job(task, args, kwargs, *, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  payload = _normalize_payload(args, kwargs)
  concurrency_key = _resolve_concurrency_key(task, args, kwargs)

  with transaction.atomic(using=alias):
    job = Job.objects.using(alias).create(
      task_path=task.module_path,
      queue_name=task.queue_name,
      priority=task.priority,
      payload=payload,
      backend_name=backend_alias,
      scheduled_at=task.run_after,
      concurrency_key=concurrency_key,
    )
    _dispatch_job(job, task=task, backend_alias=backend_alias)

  log_event(
    "job.enqueued",
    job_id=str(job.id),
    task_path=job.task_path,
    queue_name=job.queue_name,
    priority=job.priority,
  )
  return job


def enqueue_jobs_bulk(task_calls, *, backend_alias="default"):
  return [
    enqueue_job(task, args, kwargs, backend_alias=backend_alias)
    for task, args, kwargs in task_calls
  ]


def claim_ready_jobs(
  *,
  limit,
  queues=None,
  process=None,
  backend_alias="default",
  use_skip_locked=None,
):
  if limit <= 0:
    return []

  alias = get_database_alias(backend_alias)
  if use_skip_locked is None:
    use_skip_locked = load_backend_config(backend_alias).use_skip_locked

  paused_queue_names = list(Pause.objects.using(alias).values_list("queue_name", flat=True))

  with transaction.atomic(using=alias):
    queryset = ReadyExecution.objects.using(alias).select_related("job")
    if paused_queue_names:
      queryset = queryset.exclude(queue_name__in=paused_queue_names)
    queryset = _filter_queue_selectors(queryset, queues)
    queryset = queryset.order_by("-priority", "id")
    ready_rows = list(locked_queryset(queryset, use_skip_locked=use_skip_locked)[:limit])
    if not ready_rows:
      return []

    jobs = [row.job for row in ready_rows]
    ReadyExecution.objects.using(alias).filter(pk__in=[row.pk for row in ready_rows]).delete()
    for job in jobs:
      ClaimedExecution.objects.using(alias).create(job=job, process=process)

  for job in jobs:
    log_event("job.claimed", job_id=str(job.id), queue_name=job.queue_name, priority=job.priority)
  return jobs


def complete_claimed_job(job_id, return_value, *, backend_alias="default"):
  alias = get_database_alias(backend_alias)

  with transaction.atomic(using=alias):
    claimed = ClaimedExecution.objects.using(alias).select_related("job").get(job_id=job_id)
    job = claimed.job
    now = timezone.now()
    config = load_backend_config(job.backend_name)

    if config.preserve_finished_jobs:
      job.finished_at = now
      job.return_value = return_value
      job.save(using=alias, update_fields=["finished_at", "return_value", "updated_at"])
      claimed.delete(using=alias)
    else:
      job.delete(using=alias)

  _release_concurrency_slot(job)
  log_event("job.executed", job_id=str(job.id), status="success")
  return job


def fail_claimed_job(job_id, error, *, traceback_text="", backend_alias="default"):
  alias = get_database_alias(backend_alias)

  with transaction.atomic(using=alias):
    claimed = ClaimedExecution.objects.using(alias).select_related("job").get(job_id=job_id)
    job = claimed.job
    claimed.delete(using=alias)
    FailedExecution.objects.using(alias).create(
      job=job,
      exception_class=_exception_path(error),
      message=str(error),
      traceback=traceback_text,
    )

  _release_concurrency_slot(job)
  log_event(
    "job.failed",
    job_id=str(job.id),
    exception_class=_exception_path(error),
    message=str(error),
  )
  return job


def promote_scheduled_jobs(*, batch_size, backend_alias="default", use_skip_locked=None):
  alias = get_database_alias(backend_alias)
  if use_skip_locked is None:
    use_skip_locked = load_backend_config(backend_alias).use_skip_locked
  now = timezone.now()

  with transaction.atomic(using=alias):
    queryset = (
      ScheduledExecution.objects.using(alias)
      .select_related("job")
      .filter(scheduled_at__lte=now)
      .order_by("scheduled_at", "-priority", "id")
    )
    scheduled_rows = list(locked_queryset(queryset, use_skip_locked=use_skip_locked)[:batch_size])
    jobs = [row.job for row in scheduled_rows]
    if not jobs:
      return []

    ScheduledExecution.objects.using(alias).filter(
      pk__in=[row.pk for row in scheduled_rows]
    ).delete()
    for job in jobs:
      _dispatch_existing_job(job)
    return jobs


def retry_failed_job(job_id, *, backend_alias="default"):
  alias = get_database_alias(backend_alias)

  with transaction.atomic(using=alias):
    failed = FailedExecution.objects.using(alias).select_related("job").get(job_id=job_id)
    job = failed.job
    failed.delete(using=alias)
    job.return_value = None
    job.finished_at = None
    job.save(using=alias, update_fields=["return_value", "finished_at", "updated_at"])
    _dispatch_existing_job(job)

  log_event("job.retried", job_id=str(job.id), queue_name=job.queue_name, priority=job.priority)
  return job


def discard_failed_job(job_id, *, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  queryset = Job.objects.using(alias).filter(pk=job_id, failed_execution__isnull=False)
  deleted = queryset.count()
  if not deleted:
    return 0

  queryset.delete()
  log_event("job.discarded", job_id=str(job_id), reason="failed")
  return deleted


def discard_ready_jobs(*, job_ids=None, batch_size=500, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  config = load_backend_config(backend_alias)

  with transaction.atomic(using=alias):
    queryset = ReadyExecution.objects.using(alias).select_related("job").order_by("id")
    if job_ids is not None:
      queryset = queryset.filter(job_id__in=job_ids)
    ready_rows = list(
      locked_queryset(queryset, use_skip_locked=config.use_skip_locked)[:batch_size]
    )
    jobs = [row.job for row in ready_rows]
    if not jobs:
      return 0

    Job.objects.using(alias).filter(pk__in=[job.pk for job in jobs]).delete()

  for job in jobs:
    _release_concurrency_slot(job)
    log_event("job.discarded", job_id=str(job.id), reason="ready")
  return len(jobs)


def discard_blocked_jobs(*, job_ids=None, batch_size=500, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  config = load_backend_config(backend_alias)

  with transaction.atomic(using=alias):
    queryset = BlockedExecution.objects.using(alias).select_related("job").order_by("id")
    if job_ids is not None:
      queryset = queryset.filter(job_id__in=job_ids)
    blocked_rows = list(
      locked_queryset(queryset, use_skip_locked=config.use_skip_locked)[:batch_size]
    )
    jobs = [row.job for row in blocked_rows]
    if not jobs:
      return 0

    Job.objects.using(alias).filter(pk__in=[job.pk for job in jobs]).delete()

  for job in jobs:
    log_event("job.discarded", job_id=str(job.id), reason="blocked")
  return len(jobs)


def _dispatch_existing_job(job):
  task = import_string(job.task_path)
  _dispatch_job(job, task=task, backend_alias=job.backend_name)


def _dispatch_job(job, *, task, backend_alias):
  alias = get_database_alias(backend_alias)
  now = timezone.now()

  if job.scheduled_at is not None and job.scheduled_at > now:
    ScheduledExecution.objects.using(alias).create(
      job=job,
      queue_name=job.queue_name,
      priority=job.priority,
      scheduled_at=job.scheduled_at,
    )
    return "scheduled"

  if not job.concurrency_key:
    ReadyExecution.objects.using(alias).create(
      job=job,
      queue_name=job.queue_name,
      priority=job.priority,
    )
    return "ready"

  limit, duration_seconds, on_conflict = _concurrency_settings(task, backend_alias=backend_alias)
  if semaphore_acquire(
    job.concurrency_key,
    limit=limit,
    duration_seconds=duration_seconds,
    backend_alias=backend_alias,
  ):
    ReadyExecution.objects.using(alias).create(
      job=job,
      queue_name=job.queue_name,
      priority=job.priority,
    )
    return "ready"

  if on_conflict == "discard":
    job.finished_at = now
    job.return_value = None
    job.save(using=alias, update_fields=["finished_at", "return_value", "updated_at"])
    return "discarded"

  BlockedExecution.objects.using(alias).create(
    job=job,
    queue_name=job.queue_name,
    priority=job.priority,
    concurrency_key=job.concurrency_key,
    expires_at=now + timedelta(seconds=duration_seconds),
  )
  return "blocked"


def _release_concurrency_slot(job):
  if not job.concurrency_key:
    return

  task = import_string(job.task_path)
  limit, duration_seconds, _ = _concurrency_settings(task, backend_alias=job.backend_name)
  semaphore_release(
    job.concurrency_key,
    duration_seconds=duration_seconds,
    backend_alias=job.backend_name,
  )
  unblock_next_blocked_job(
    job.concurrency_key,
    limit=limit,
    duration_seconds=duration_seconds,
    backend_alias=job.backend_name,
    use_skip_locked=load_backend_config(job.backend_name).use_skip_locked,
  )


def _concurrency_settings(task, *, backend_alias):
  limit = _task_option(task, "concurrency_limit")
  if limit in (None, ""):
    raise EnqueueError("concurrency_limit is required when concurrency_key is set")

  limit = int(limit)
  duration_seconds = int(
    _task_option(
      task,
      "concurrency_duration",
      load_backend_config(backend_alias).default_concurrency_duration,
    )
  )
  on_conflict = str(_task_option(task, "on_conflict", "block"))
  if on_conflict not in {"block", "discard"}:
    raise EnqueueError("on_conflict must be 'block' or 'discard'")
  return limit, duration_seconds, on_conflict


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


def _bound_arguments(task, args, kwargs):
  signature = inspect.signature(task.func)
  parameters = tuple(signature.parameters.values())
  if task.takes_context and parameters:
    signature = signature.replace(parameters=parameters[1:])

  bound = signature.bind(*args, **kwargs)
  bound.apply_defaults()
  return bound.arguments


def _filter_queue_selectors(queryset, queues):
  if queues in (None, (), "*", ["*"], ("*",)):
    return queryset

  selectors = (queues,) if isinstance(queues, str) else tuple(queues)
  condition = Q()
  for selector in selectors:
    if selector == "*":
      return queryset
    if selector.endswith("*"):
      condition |= Q(queue_name__startswith=selector[:-1])
    else:
      condition |= Q(queue_name=selector)

  if not condition:
    return queryset.none()
  return queryset.filter(condition)


def _normalize_payload(args, kwargs):
  try:
    return json.loads(json.dumps({"args": list(args), "kwargs": dict(kwargs)}))
  except TypeError as exc:
    raise EnqueueError("payload must be JSON round-trippable") from exc


def _task_option(task, name, default=None):
  if hasattr(task, name):
    return getattr(task, name)
  return getattr(task.func, name, default)


def _exception_path(error):
  return f"{error.__class__.__module__}.{error.__class__.__qualname__}"
