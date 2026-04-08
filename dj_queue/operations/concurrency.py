from datetime import timedelta

from django.db import IntegrityError, transaction
from django.utils import timezone
from django.utils.module_loading import import_string

from dj_queue.config import load_backend_config
from dj_queue.db import get_database_alias, locked_queryset
from dj_queue.log import log_event
from dj_queue.models import BlockedExecution, ReadyExecution, Semaphore
from dj_queue.runtime import notify as runtime_notify


def semaphore_acquire(
  key,
  *,
  limit,
  duration_seconds,
  backend_alias="default",
):
  alias = get_database_alias(backend_alias)
  expires_at = timezone.now() + timedelta(seconds=duration_seconds)

  for attempt in range(2):
    try:
      with transaction.atomic(using=alias):
        semaphore = Semaphore.objects.using(alias).select_for_update().filter(key=key).first()
        if semaphore is None:
          Semaphore.objects.using(alias).create(
            key=key,
            value=limit - 1,
            limit=limit,
            expires_at=expires_at,
          )
          return True

        if semaphore.value <= 0:
          return False

        semaphore.value -= 1
        semaphore.expires_at = expires_at
        semaphore.save(using=alias, update_fields=["value", "expires_at", "updated_at"])
        return True
    except IntegrityError:
      # two workers can both miss the row, then race to create the unique key
      # retry once so the loser can load the row created by the winner
      if attempt == 0:
        continue
      continue

  return False


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


def unblock_next_blocked_job(
  key,
  *,
  limit,
  duration_seconds,
  backend_alias="default",
  use_skip_locked=True,
):
  alias = get_database_alias(backend_alias)

  with transaction.atomic(using=alias):
    queryset = (
      BlockedExecution.objects.using(alias)
      .select_related("job")
      .filter(concurrency_key=key)
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
    ReadyExecution.objects.using(alias).create(
      job=job,
      queue_name=queue_name,
      priority=priority,
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
  queryset = Semaphore.objects.using(alias).filter(expires_at__lte=timezone.now())
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

  with transaction.atomic(using=alias):
    queryset = (
      BlockedExecution.objects.using(alias)
      .select_related("job")
      .filter(expires_at__lte=now)
      .order_by("expires_at", "-priority", "id")
    )
    blocked_rows = list(locked_queryset(queryset, use_skip_locked=use_skip_locked)[:batch_size])

  for blocked in blocked_rows:
    task = import_string(blocked.job.task_path)
    limit = int(getattr(task.func, "concurrency_limit"))
    duration_seconds = int(getattr(task.func, "concurrency_duration", 60))

    with transaction.atomic(using=alias):
      refreshed = (
        BlockedExecution.objects.using(alias).select_related("job").filter(pk=blocked.pk).first()
      )
      if refreshed is None:
        continue

      if semaphore_acquire(
        refreshed.concurrency_key,
        limit=limit,
        duration_seconds=duration_seconds,
        backend_alias=backend_alias,
      ):
        job = refreshed.job
        queue_name = refreshed.queue_name
        priority = refreshed.priority
        refreshed.delete(using=alias)
        ReadyExecution.objects.using(alias).create(
          job=job,
          queue_name=queue_name,
          priority=priority,
        )
        promoted_jobs.append(job)
      else:
        refreshed.expires_at = timezone.now() + timedelta(seconds=duration_seconds)
        refreshed.save(using=alias, update_fields=["expires_at"])

  for job in promoted_jobs:
    log_event("job.unblocked", job_id=str(job.id), concurrency_key=job.concurrency_key)
    runtime_notify.notify_ready_queues((job.queue_name,), backend_alias=backend_alias)
  return promoted_jobs
