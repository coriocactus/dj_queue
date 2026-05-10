from datetime import timedelta

from django.db import transaction
from django.db.models import F
from django.utils import timezone
from django.utils.module_loading import import_string

from dj_queue.config import load_backend_config
from dj_queue.db import database_capabilities, get_database_alias, locked_queryset
from dj_queue.exceptions import EnqueueError
from dj_queue.log import log_event
from dj_queue.models import BlockedExecution, ClaimedExecution, ReadyExecution, Semaphore
from dj_queue.operations._helpers import (
  _consume_selected_rows,
  _lock_active_pauses,
  _task_option,
)
from dj_queue.operations._insert import create_ignore_conflicts
from dj_queue.runtime import notify as runtime_notify


def semaphore_acquire(
  key,
  *,
  limit,
  duration_seconds,
  backend_alias="default",
):
  alias = get_database_alias(backend_alias)
  now = timezone.now()
  expires_at = now + timedelta(seconds=duration_seconds)

  with transaction.atomic(using=alias):
    if create_ignore_conflicts(
      Semaphore,
      using=alias,
      key=key,
      value=limit - 1,
      limit=limit,
      expires_at=expires_at,
    ):
      return True

  # mysql-family backends can deadlock if a skipped insert and row lock happen in one tx
  with transaction.atomic(using=alias):
    updated = (
      Semaphore.objects.using(alias)
      .filter(key=key, value__gt=0)
      .update(
        value=F("value") - 1,
        expires_at=expires_at,
        updated_at=now,
      )
    )
  return updated > 0


def semaphore_release(key, *, duration_seconds, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  expires_at = timezone.now() + timedelta(seconds=duration_seconds)

  with transaction.atomic(using=alias):
    semaphore = Semaphore.objects.using(alias).select_for_update().filter(key=key).first()
    if semaphore is None:
      return False

    semaphore.value = min(semaphore.limit, semaphore.value + 1)
    semaphore.expires_at = expires_at
    semaphore.save(using=alias, update_fields=["value", "expires_at", "updated_at"])
    return True


def concurrency_settings(task, *, backend_alias):
  limit = _task_option(task, "concurrency_limit")
  if limit in (None, ""):
    raise EnqueueError("concurrency_limit is required when concurrency_key is set")

  limit = _positive_int_option(limit, "concurrency_limit")
  duration_seconds = _positive_int_option(
    _task_option(
      task,
      "concurrency_duration",
      load_backend_config(backend_alias).default_concurrency_duration,
    ),
    "concurrency_duration",
  )
  on_conflict = str(_task_option(task, "on_conflict", "block"))
  if on_conflict not in {"block", "discard"}:
    raise EnqueueError("on_conflict must be 'block' or 'discard'")
  return limit, duration_seconds, on_conflict


def unblock_next_blocked_job(
  key,
  *,
  limit,
  duration_seconds,
  backend_alias="default",
  use_skip_locked=True,
):
  alias = get_database_alias(backend_alias)
  now = timezone.now()

  with transaction.atomic(using=alias):
    queryset = (
      BlockedExecution.objects.using(alias)
      .select_related("job")
      .filter(backend_alias=backend_alias, concurrency_key=key)
      .order_by("-priority", "id")
    )
    blocked = locked_queryset(queryset, use_skip_locked=use_skip_locked).first()
    if blocked is None:
      return None

    if not semaphore_acquire(
      key,
      limit=limit,
      duration_seconds=duration_seconds,
      backend_alias=backend_alias,
    ):
      return None

    job = blocked.job
    queue_name = blocked.queue_name
    priority = blocked.priority
    blocked.delete(using=alias)
    _lock_active_pauses(alias, backend_alias, {queue_name})
    ReadyExecution.objects.using(alias).create(
      job=job,
      backend_alias=backend_alias,
      queue_name=queue_name,
      priority=priority,
      latency_started_at=now,
    )

  log_event(
    "job.unblocked",
    job_id=str(job.id),
    concurrency_key=key,
  )
  runtime_notify.notify_ready_queues((job.queue_name,), backend_alias=backend_alias)
  return job


def cleanup_expired_semaphores(*, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  active_concurrency_keys = (
    ClaimedExecution.objects.using(alias)
    .exclude(job__concurrency_key__isnull=True)
    .exclude(job__concurrency_key="")
    .values_list("job__concurrency_key", flat=True)
  )
  queryset = (
    Semaphore.objects.using(alias)
    .filter(expires_at__lte=timezone.now())
    .exclude(key__in=active_concurrency_keys)
  )
  deleted = queryset.count()
  if not deleted:
    return 0

  queryset.delete()
  return deleted


def promote_expired_blocked_jobs(*, batch_size=500, backend_alias="default", use_skip_locked=None):
  alias = get_database_alias(backend_alias)
  if use_skip_locked is None:
    use_skip_locked = load_backend_config(backend_alias).use_skip_locked
  now = timezone.now()
  promoted_jobs = []
  task_settings = {}
  uses_serialized_writes = database_capabilities(alias).uses_serialized_writes

  with transaction.atomic(using=alias):
    queryset = (
      BlockedExecution.objects.using(alias)
      .select_related("job")
      .filter(backend_alias=backend_alias, expires_at__lte=now)
      .order_by("expires_at", "-priority", "id")
    )
    blocked_rows = list(locked_queryset(queryset, use_skip_locked=use_skip_locked)[:batch_size])
    if not blocked_rows:
      return []
    if uses_serialized_writes:
      blocked_rows = _consume_selected_rows(alias, BlockedExecution, blocked_rows)
      if not blocked_rows:
        return []

    for blocked in blocked_rows:
      job = blocked.job
      limit, duration_seconds = task_settings.get(job.task_path, (None, None))
      if limit is None:
        task = import_string(job.task_path)
        limit, duration_seconds, _ = concurrency_settings(task, backend_alias=backend_alias)
        task_settings[job.task_path] = (limit, duration_seconds)

      if semaphore_acquire(
        blocked.concurrency_key,
        limit=limit,
        duration_seconds=duration_seconds,
        backend_alias=backend_alias,
      ):
        queue_name = blocked.queue_name
        priority = blocked.priority
        if not uses_serialized_writes:
          blocked.delete(using=alias)
        _lock_active_pauses(alias, backend_alias, {queue_name})
        ReadyExecution.objects.using(alias).create(
          job=job,
          backend_alias=backend_alias,
          queue_name=queue_name,
          priority=priority,
          latency_started_at=now,
        )
        promoted_jobs.append(job)
      else:
        expires_at = now + timedelta(seconds=duration_seconds)
        if uses_serialized_writes:
          BlockedExecution.objects.using(alias).create(
            job=job,
            backend_alias=backend_alias,
            queue_name=blocked.queue_name,
            priority=blocked.priority,
            concurrency_key=blocked.concurrency_key,
            expires_at=expires_at,
          )
        else:
          blocked.expires_at = expires_at
          blocked.save(using=alias, update_fields=["expires_at"])

  for job in promoted_jobs:
    log_event("job.unblocked", job_id=str(job.id), concurrency_key=job.concurrency_key)
    runtime_notify.notify_ready_queues((job.queue_name,), backend_alias=backend_alias)
  return promoted_jobs


def _positive_int_option(value, name):
  try:
    number = int(value)
  except (TypeError, ValueError, OverflowError) as exc:
    raise EnqueueError(f"{name} must be a positive integer") from exc

  if number <= 0:
    raise EnqueueError(f"{name} must be a positive integer")
  return number
