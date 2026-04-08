from datetime import timedelta

from django.db import IntegrityError, transaction
from django.utils import timezone

from dj_queue.db import get_database_alias, locked_queryset
from dj_queue.log import log_event
from dj_queue.models import BlockedExecution, ReadyExecution, Semaphore


def semaphore_acquire(
  key,
  *,
  limit,
  duration_seconds,
  backend_alias="default",
):
  alias = get_database_alias(backend_alias)
  expires_at = timezone.now() + timedelta(seconds=duration_seconds)

  for _ in range(2):
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
  return job


def cleanup_expired_semaphores(*, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  queryset = Semaphore.objects.using(alias).filter(expires_at__lte=timezone.now())
  deleted = queryset.count()
  if not deleted:
    return 0

  queryset.delete()
  return deleted
