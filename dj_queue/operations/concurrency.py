from datetime import timedelta

from django.db import transaction
from django.db.models import F
from django.utils import timezone
from django.utils.module_loading import import_string

from dj_queue.config import load_backend_config
from dj_queue.db import get_database_alias, locked_queryset
from dj_queue.log import log_event
from dj_queue.models import BlockedExecution, Job, Pause, ReadyExecution, Semaphore
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
      .filter(concurrency_key=key, job__backend_alias=backend_alias)
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

    job = Job.objects.using(alias).get(pk=blocked.job_id, backend_alias=backend_alias)
    queue_name = blocked.queue_name
    priority = blocked.priority
    blocked.delete(using=alias)
    _lock_active_pauses(alias, backend_alias, {queue_name})
    ReadyExecution.objects.using(alias).create(
      job=job,
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
  task_settings = {}

  with transaction.atomic(using=alias):
    queryset = (
      BlockedExecution.objects.using(alias)
      .filter(job__backend_alias=backend_alias, expires_at__lte=now)
      .order_by("expires_at", "-priority", "id")
    )
    blocked_rows = list(locked_queryset(queryset, use_skip_locked=use_skip_locked)[:batch_size])
    if not blocked_rows:
      return []

    jobs_by_id = {
      job.id: job
      for job in Job.objects.using(alias).filter(
        backend_alias=backend_alias,
        pk__in=[b.job_id for b in blocked_rows],
      )
    }

    for blocked in blocked_rows:
      job = jobs_by_id[blocked.job_id]
      limit, duration_seconds = task_settings.get(job.task_path, (None, None))
      if limit is None:
        task = import_string(job.task_path)
        limit = int(getattr(task.func, "concurrency_limit"))
        duration_seconds = int(getattr(task.func, "concurrency_duration", 60))
        task_settings[job.task_path] = (limit, duration_seconds)

      if semaphore_acquire(
        blocked.concurrency_key,
        limit=limit,
        duration_seconds=duration_seconds,
        backend_alias=backend_alias,
      ):
        queue_name = blocked.queue_name
        priority = blocked.priority
        blocked.delete(using=alias)
        _lock_active_pauses(alias, backend_alias, {queue_name})
        ReadyExecution.objects.using(alias).create(
          job=job,
          queue_name=queue_name,
          priority=priority,
          latency_started_at=now,
        )
        promoted_jobs.append(job)
      else:
        blocked.expires_at = now + timedelta(seconds=duration_seconds)
        blocked.save(using=alias, update_fields=["expires_at"])

  for job in promoted_jobs:
    log_event("job.unblocked", job_id=str(job.id), concurrency_key=job.concurrency_key)
    runtime_notify.notify_ready_queues((job.queue_name,), backend_alias=backend_alias)
  return promoted_jobs


def _lock_active_pauses(alias, backend_alias, queue_names):
  active_queue_names = tuple(queue_name for queue_name in queue_names if queue_name)
  if not active_queue_names:
    return None

  list(
    Pause.objects.using(alias)
    .select_for_update()
    .filter(backend_alias=backend_alias, queue_name__in=active_queue_names)
    .values_list("queue_name", flat=True)
  )
  return None
