from collections import defaultdict
from datetime import timedelta
from dataclasses import dataclass
from enum import StrEnum

from django.db import connections, transaction
from django.db.models import Case, F, IntegerField, Q, Value, When
from django.db.models.functions import Greatest, Least
from django.utils import timezone
from django.utils.module_loading import import_string

from dj_queue.config import load_backend_config
from dj_queue.db import (
  database_capabilities,
  get_database_alias,
  locked_queryset,
  retry_transient_database_errors,
)
from dj_queue.exceptions import EnqueueError
from dj_queue.log import log_event
from dj_queue.models import (
  BlockedExecution,
  ClaimedExecution,
  Job,
  Semaphore,
)
from dj_queue.operations._helpers import (
  _bulk_create_ready_executions_locked,
  _consume_selected_rows,
  _create_blocked_execution,
  _create_ready_execution_locked,
  _ensure_no_other_execution_state,
  _ensure_state_rows_belong_to_backend,
  _lock_active_pauses,
  _ready_execution_row,
  _task_option,
)
from dj_queue.operations._insert import create_ignore_conflicts
from dj_queue.sql import backend_sql
from dj_queue.sql import common as sql_common
from dj_queue.wakeup import notify_ready_queues_on_commit


class SlotHandoffMode(StrEnum):
  ACQUIRE = "acquire"
  RELEASE_CLAIMED = "release_claimed"
  CONSUME_RELEASED = "consume_released"


@dataclass(frozen=True, slots=True)
class BlockedJobRef:
  id: object
  backend_alias: str
  queue_name: str
  priority: int
  concurrency_key: str

  @property
  def pk(self):
    return self.id


@dataclass(frozen=True, slots=True)
class ClaimedHandoffRef:
  job_id: object
  claimed_at: object


@dataclass(frozen=True, slots=True)
class BlockedSlot:
  job: BlockedJobRef
  slot_acquired: bool


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
  backend_family = database_capabilities(alias).backend_family

  if backend_family in {"postgresql", "mysql", "mariadb"}:
    return backend_sql(alias).semaphore_acquire(
      alias,
      key,
      limit=limit,
      expires_at=expires_at,
      now=now,
    )

  with _operation_atomic(alias):
    if create_ignore_conflicts(
      Semaphore,
      using=alias,
      key=key,
      value=limit - 1,
      limit=limit,
      expires_at=expires_at,
    ):
      return True

  reconciled_available = _reconciled_available_expression(limit)
  with _operation_atomic(alias):
    updated = (
      Semaphore.objects.using(alias)
      .filter(key=key, value__gt=F("limit") - Value(limit))
      .update(
        value=reconciled_available - Value(1),
        limit=limit,
        expires_at=expires_at,
        updated_at=now,
      )
    )
    if updated:
      return True

    Semaphore.objects.using(alias).filter(key=key).filter(
      Q(value__lt=reconciled_available) | Q(value__gt=reconciled_available) | ~Q(limit=limit)
    ).update(
      value=reconciled_available,
      limit=limit,
      updated_at=now,
    )
  return False


def semaphore_acquire_many(
  key,
  *,
  count,
  limit,
  duration_seconds,
  backend_alias="default",
):
  if count <= 0:
    return 0
  alias = get_database_alias(backend_alias)
  now = timezone.now()
  expires_at = now + timedelta(seconds=duration_seconds)

  with _operation_atomic(alias):
    acquired = min(count, limit)
    semaphore, created = (
      Semaphore.objects.using(alias)
      .select_for_update()
      .get_or_create(
        key=key,
        defaults={
          "value": limit - acquired,
          "limit": limit,
          "expires_at": expires_at,
          "updated_at": now,
        },
      )
    )
    if created:
      return acquired

    available = min(limit, max(0, semaphore.value + limit - semaphore.limit))
    acquired = min(count, available)
    value = available - acquired
    if acquired:
      semaphore.value = value
      semaphore.limit = limit
      semaphore.expires_at = expires_at
      semaphore.updated_at = now
      semaphore.save(using=alias, update_fields=["value", "limit", "expires_at", "updated_at"])
    elif semaphore.value != value or semaphore.limit != limit:
      semaphore.value = value
      semaphore.limit = limit
      semaphore.updated_at = now
      semaphore.save(using=alias, update_fields=["value", "limit", "updated_at"])
    return acquired


def semaphore_release(key, *, limit=None, duration_seconds, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  now = timezone.now()
  expires_at = now + timedelta(seconds=duration_seconds)

  if limit is not None:
    updated = (
      Semaphore.objects.using(alias)
      .filter(key=key)
      .update(
        value=Least(
          Value(limit),
          Greatest(Value(0), F("value") + Value(limit) - F("limit") + Value(1)),
        ),
        limit=limit,
        expires_at=expires_at,
        updated_at=now,
      )
    )
    return updated > 0

  updated = (
    Semaphore.objects.using(alias)
    .filter(key=key)
    .update(
      value=Case(
        When(value__gte=F("limit"), then=F("limit")),
        default=F("value") + 1,
        output_field=IntegerField(),
      ),
      expires_at=expires_at,
      updated_at=now,
    )
  )
  return updated > 0


def release_recovered_concurrency_slots(jobs, *, backend_alias="default"):
  grouped_jobs = defaultdict(list)
  for job in jobs:
    if job.backend_alias != backend_alias:
      raise EnqueueError(f"job {job.id} belongs to backend {job.backend_alias!r}")
    if job.concurrency_key:
      grouped_jobs[job.concurrency_key].append(job)

  fallback_jobs = []
  alias = get_database_alias(backend_alias)
  for key, group_jobs in grouped_jobs.items():
    settings = _recovered_release_settings(alias, group_jobs, backend_alias=backend_alias)
    if settings is None:
      fallback_jobs.extend(group_jobs)
      continue
    limit, duration_seconds = settings
    _release_recovered_concurrency_group(
      alias,
      key,
      release_count=len(group_jobs),
      limit=limit,
      duration_seconds=duration_seconds,
      backend_alias=backend_alias,
    )
  return fallback_jobs


def _recovered_release_settings(alias, jobs, *, backend_alias):
  config = load_backend_config(backend_alias)
  settings = set()
  for job in jobs:
    try:
      task = import_string(job.task_path)
      limit, duration_seconds, _ = concurrency_settings(task, backend_alias=backend_alias)
    except (AttributeError, EnqueueError, ImportError):
      limit = _semaphore_limit(alias, job.concurrency_key) or 1
      duration_seconds = config.default_concurrency_duration
    settings.add((limit, duration_seconds))
    if len(settings) > 1:
      return None
  return next(iter(settings))


def _semaphore_limit(alias, key):
  return Semaphore.objects.using(alias).filter(key=key).values_list("limit", flat=True).first()


def _release_recovered_concurrency_group(
  alias,
  key,
  *,
  release_count,
  limit,
  duration_seconds,
  backend_alias,
):
  now = timezone.now()
  expires_at = now + timedelta(seconds=duration_seconds)
  config = load_backend_config(backend_alias)

  with _operation_atomic(alias):
    blocked_rows = []
    if release_count > 0:
      queryset = (
        BlockedExecution.objects.using(alias)
        .select_related("job")
        .filter(backend_alias=backend_alias, concurrency_key=key)
        .order_by("-priority", "id")
      )
      blocked_rows = list(
        locked_queryset(queryset, use_skip_locked=config.use_skip_locked)[:release_count]
      )
      if blocked_rows:
        _ensure_state_rows_belong_to_backend(blocked_rows, backend_alias)

    semaphore = Semaphore.objects.using(alias).select_for_update().filter(key=key).first()
    if semaphore is None:
      return []

    available = min(limit, max(0, semaphore.value + limit - semaphore.limit + release_count))
    promote_count = min(release_count, available, len(blocked_rows))
    promoted_rows = blocked_rows[:promote_count]
    if promoted_rows:
      promoted_rows = _consume_selected_rows(alias, BlockedExecution, promoted_rows)

    promoted_jobs = [blocked.job for blocked in promoted_rows]
    semaphore.value = available - len(promoted_jobs)
    semaphore.limit = limit
    semaphore.expires_at = expires_at
    semaphore.updated_at = now
    semaphore.save(using=alias, update_fields=["value", "limit", "expires_at", "updated_at"])

    _bulk_create_ready_executions_locked(
      alias,
      [
        _ready_execution_row(
          blocked.job,
          backend_alias=backend_alias,
          queue_name=blocked.queue_name,
          priority=blocked.priority,
          ready_at=now,
        )
        for blocked in promoted_rows
      ],
      backend_alias=backend_alias,
      check_conflicts=True,
    )

  for job in promoted_jobs:
    log_event(
      "job.unblocked",
      backend_alias=backend_alias,
      job_id=str(job.id),
      concurrency_key=key,
    )
  if promoted_jobs:
    notify_ready_queues_on_commit(
      tuple(dict.fromkeys(job.queue_name for job in promoted_jobs)),
      backend_alias=backend_alias,
    )
  return promoted_jobs


def _consume_released_semaphore_slot(alias, key, *, limit, duration_seconds, now):
  expires_at = now + timedelta(seconds=duration_seconds)
  updated = (
    Semaphore.objects.using(alias)
    .filter(key=key, value__gt=0)
    .update(
      value=F("value") - 1,
      limit=limit,
      expires_at=expires_at,
      updated_at=now,
    )
  )
  return updated > 0


def _handoff_released_claimed_slot(alias, key, *, limit, duration_seconds, now):
  expires_at = now + timedelta(seconds=duration_seconds)
  released_available = _released_available_expression(limit)
  updated = (
    Semaphore.objects.using(alias)
    .filter(key=key, value__gt=F("limit") - Value(limit) - Value(1))
    .update(
      value=released_available - Value(1),
      limit=limit,
      expires_at=expires_at,
      updated_at=now,
    )
  )
  return updated > 0


def _released_available_expression(limit):
  return Least(
    Value(limit),
    Greatest(Value(0), F("value") + Value(limit) - F("limit") + Value(1)),
  )


def _reconciled_available_expression(limit):
  return Least(Value(limit), Greatest(Value(0), F("value") + Value(limit) - F("limit")))


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
  slot_handoff=SlotHandoffMode.ACQUIRE,
):
  alias = get_database_alias(backend_alias)
  now = timezone.now()
  slot_handoff = SlotHandoffMode(slot_handoff)

  with _operation_atomic(alias):
    blocked_slot = _consume_blocked_job_for_slot(
      alias,
      backend_alias=backend_alias,
      key=key,
      limit=limit,
      duration_seconds=duration_seconds,
      now=now,
      use_skip_locked=use_skip_locked,
      slot_handoff=slot_handoff,
    )
    if blocked_slot is None or not blocked_slot.slot_acquired:
      return None
    job_ref = blocked_slot.job

    _create_ready_execution_after_blocked_consume(
      alias,
      job=job_ref,
      backend_alias=backend_alias,
      queue_name=job_ref.queue_name,
      priority=job_ref.priority,
      ready_at=now,
    )

  log_event(
    "job.unblocked",
    backend_alias=backend_alias,
    job_id=str(job_ref.id),
    concurrency_key=key,
  )
  notify_ready_queues_on_commit((job_ref.queue_name,), backend_alias=backend_alias)
  return job_ref


def claim_next_blocked_job(
  key,
  *,
  limit,
  duration_seconds,
  process_id,
  backend_alias="default",
  use_skip_locked=True,
):
  if process_id is None or limit != 1:
    return None

  alias = get_database_alias(backend_alias)
  now = timezone.now()
  promoted_job_ref = None
  claimed_ref = None

  with _operation_atomic(alias):
    blocked_slot = _consume_blocked_job_for_slot(
      alias,
      backend_alias=backend_alias,
      key=key,
      limit=limit,
      duration_seconds=duration_seconds,
      now=now,
      use_skip_locked=use_skip_locked,
      slot_handoff=SlotHandoffMode.RELEASE_CLAIMED,
    )
    if blocked_slot is None or not blocked_slot.slot_acquired:
      return None

    job_ref = blocked_slot.job
    if _lock_active_pauses(alias, backend_alias, {job_ref.queue_name}):
      _create_ready_execution_after_blocked_consume(
        alias,
        job=job_ref,
        backend_alias=backend_alias,
        queue_name=job_ref.queue_name,
        priority=job_ref.priority,
        ready_at=now,
      )
      promoted_job_ref = job_ref
    else:
      _create_claimed_execution_after_blocked_consume(
        alias,
        job=job_ref,
        process_id=process_id,
        claimed_at=now,
      )
      claimed_ref = ClaimedHandoffRef(job_id=job_ref.id, claimed_at=now)

  if promoted_job_ref is not None:
    log_event(
      "job.unblocked",
      backend_alias=backend_alias,
      job_id=str(promoted_job_ref.id),
      concurrency_key=key,
    )
    notify_ready_queues_on_commit((promoted_job_ref.queue_name,), backend_alias=backend_alias)
  return claimed_ref


def _consume_blocked_job_for_slot(
  alias,
  *,
  backend_alias,
  key,
  limit,
  duration_seconds,
  now,
  use_skip_locked,
  slot_handoff,
):
  slot_handoff = SlotHandoffMode(slot_handoff)
  capabilities = database_capabilities(alias)
  postgres_released_slot = (
    slot_handoff is SlotHandoffMode.RELEASE_CLAIMED and capabilities.backend_family == "postgresql"
  )
  if postgres_released_slot:
    blocked = backend_sql(alias).consume_next_blocked_job_with_released_slot(
      alias,
      backend_alias=backend_alias,
      key=key,
      limit=limit,
      duration_seconds=duration_seconds,
      now=now,
      use_skip_locked=use_skip_locked and capabilities.supports_skip_locked,
    )
    slot_acquired = bool(blocked and blocked.pop("slot_acquired"))
  else:
    blocked = _consume_next_blocked_job(
      alias,
      backend_alias=backend_alias,
      key=key,
      use_skip_locked=use_skip_locked,
    )
    slot_acquired = False
  if blocked is None:
    return None
  if blocked["job_backend_alias"] != backend_alias:
    raise EnqueueError(
      f"job {blocked['job_id']} belongs to backend {blocked['job_backend_alias']!r}"
    )

  if slot_handoff is SlotHandoffMode.RELEASE_CLAIMED and not postgres_released_slot:
    slot_acquired = _handoff_released_claimed_slot(
      alias,
      key,
      limit=limit,
      duration_seconds=duration_seconds,
      now=now,
    )
  elif slot_handoff is SlotHandoffMode.CONSUME_RELEASED:
    slot_acquired = _consume_released_semaphore_slot(
      alias,
      key,
      limit=limit,
      duration_seconds=duration_seconds,
      now=now,
    )

  if not slot_acquired and slot_handoff is not SlotHandoffMode.RELEASE_CLAIMED:
    slot_acquired = semaphore_acquire(
      key,
      limit=limit,
      duration_seconds=duration_seconds,
      backend_alias=backend_alias,
    )

  job_ref = _blocked_job_ref(blocked, backend_alias=backend_alias)
  if not slot_acquired and not postgres_released_slot:
    _restore_blocked_execution(alias, blocked, job_ref, backend_alias=backend_alias)
  return BlockedSlot(job=job_ref, slot_acquired=slot_acquired)


def _blocked_job_ref(blocked, *, backend_alias):
  return BlockedJobRef(
    id=blocked["job_id"],
    queue_name=blocked["queue_name"],
    priority=blocked["priority"],
    concurrency_key=blocked["concurrency_key"],
    backend_alias=backend_alias,
  )


def _restore_blocked_execution(alias, blocked, job_ref, *, backend_alias):
  _create_blocked_execution(
    alias,
    job_ref,
    backend_alias=backend_alias,
    queue_name=job_ref.queue_name,
    priority=job_ref.priority,
    concurrency_key=job_ref.concurrency_key,
    expires_at=blocked["expires_at"],
    check_conflicts=True,
  )


def _consume_next_blocked_job(alias, *, backend_alias, key, use_skip_locked):
  capabilities = database_capabilities(alias)
  if capabilities.backend_family == "postgresql":
    return backend_sql(alias).consume_next_blocked_job(
      alias,
      backend_alias=backend_alias,
      key=key,
      use_skip_locked=use_skip_locked and capabilities.supports_skip_locked,
    )

  queryset = (
    BlockedExecution.objects.using(alias)
    .select_related("job")
    .filter(backend_alias=backend_alias, concurrency_key=key)
    .order_by("-priority", "id")
  )
  blocked = locked_queryset(queryset, use_skip_locked=use_skip_locked).first()
  if blocked is None:
    return None
  _ensure_state_rows_belong_to_backend([blocked], backend_alias)

  consumed = _consume_selected_rows(alias, BlockedExecution, [blocked])
  if not consumed:
    return None
  return _blocked_execution_values(blocked)


def _blocked_execution_values(blocked):
  return {
    "job_id": blocked.job_id,
    "job_backend_alias": blocked.job.backend_alias,
    "queue_name": blocked.queue_name,
    "priority": blocked.priority,
    "concurrency_key": blocked.concurrency_key,
    "expires_at": blocked.expires_at,
  }


def _create_ready_execution_after_blocked_consume(
  alias,
  *,
  job,
  backend_alias,
  queue_name,
  priority,
  ready_at,
):
  _lock_active_pauses(alias, backend_alias, {queue_name})
  created = sql_common.create_ready_execution_after_blocked_consume(
    alias,
    job=job,
    backend_alias=backend_alias,
    queue_name=queue_name,
    priority=priority,
    ready_at=ready_at,
  )
  if created != 1:
    raise EnqueueError(f"job {job.id} already has an execution-state row")


def _create_claimed_execution_after_blocked_consume(alias, *, job, process_id, claimed_at):
  _ensure_no_other_execution_state(alias, job, ignored_models=(BlockedExecution,))
  ClaimedExecution.objects.using(alias).create(
    job_id=job.id,
    process_id=process_id,
    created_at=claimed_at,
  )


def cleanup_expired_semaphores(*, batch_size=500, backend_alias="default"):
  batch_size = _positive_int_option(batch_size, "batch_size")
  alias = get_database_alias(backend_alias)
  use_skip_locked = load_backend_config(backend_alias).use_skip_locked

  def cleanup_transition():
    now = timezone.now()
    with transaction.atomic(using=alias):
      queryset = locked_queryset(
        Semaphore.objects.using(alias).filter(expires_at__lte=now),
        use_skip_locked=use_skip_locked,
      )
      semaphores = list(
        queryset.order_by("expires_at", "key").values_list("pk", "key")[:batch_size]
      )
      if not semaphores:
        return 0
      semaphore_keys = [key for _semaphore_id, key in semaphores]
      active_keys = set(
        Job.objects.using(alias)
        .filter(concurrency_key__in=semaphore_keys)
        .filter(Q(claimed_execution__isnull=False) | Q(ready_execution__isnull=False))
        .values_list("concurrency_key", flat=True)
        .distinct()
      )
      semaphore_ids = [semaphore_id for semaphore_id, key in semaphores if key not in active_keys]
      if not semaphore_ids:
        return 0
      deleted, _ = Semaphore.objects.using(alias).filter(pk__in=semaphore_ids).delete()
      return deleted

  return retry_transient_database_errors(cleanup_transition)


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
    _ensure_state_rows_belong_to_backend(blocked_rows, backend_alias)
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
        _create_ready_execution_locked(
          alias,
          job=job,
          backend_alias=backend_alias,
          queue_name=queue_name,
          priority=priority,
          ready_at=now,
          check_conflicts=True,
        )
        promoted_jobs.append(job)
      else:
        expires_at = now + timedelta(seconds=duration_seconds)
        if uses_serialized_writes:
          _create_blocked_execution(
            alias,
            job,
            backend_alias=backend_alias,
            queue_name=blocked.queue_name,
            priority=blocked.priority,
            concurrency_key=blocked.concurrency_key,
            expires_at=expires_at,
            check_conflicts=True,
          )
        else:
          blocked.expires_at = expires_at
          blocked.save(using=alias, update_fields=["expires_at"])

  for job in promoted_jobs:
    log_event(
      "job.unblocked",
      backend_alias=backend_alias,
      job_id=str(job.id),
      concurrency_key=job.concurrency_key,
    )
    notify_ready_queues_on_commit((job.queue_name,), backend_alias=backend_alias)
  return promoted_jobs


def _positive_int_option(value, name):
  if isinstance(value, bool):
    raise EnqueueError(f"{name} must be a positive integer")
  if isinstance(value, int):
    number = value
  elif isinstance(value, str):
    normalized = value.strip()
    unsigned = normalized[1:] if normalized[:1] == "+" else normalized
    if not unsigned.isdecimal():
      raise EnqueueError(f"{name} must be a positive integer")
    number = int(normalized)
  else:
    raise EnqueueError(f"{name} must be a positive integer")

  if number <= 0:
    raise EnqueueError(f"{name} must be a positive integer")
  return number


def _operation_atomic(alias):
  return transaction.atomic(using=alias, savepoint=not connections[alias].in_atomic_block)
