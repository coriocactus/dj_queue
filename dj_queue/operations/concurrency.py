from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum

from django.db import transaction
from django.db.models import F, Q, Value
from django.db.models.functions import Greatest, Least
from django.utils import timezone
from django.utils.module_loading import import_string

from dj_queue.db import (
  database_capabilities,
  locked_queryset,
  retry_transient_database_errors,
)
from dj_queue.exceptions import DispatchPolicyError, EnqueueError, exception_path
from dj_queue.log import log_event
from dj_queue.models import (
  BlockedExecution,
  Job,
  Semaphore,
)
from dj_queue.operations.state_rows import (
  _bulk_create_ready_executions,
  _consume_selected_rows,
  _create_blocked_execution,
  _create_ready_execution,
  _ensure_no_other_execution_state,
  _ensure_state_rows_belong_to_backend,
  _failed_execution_row,
  _ready_execution_row,
)
from dj_queue.sql import backend_sql
from dj_queue.sql import common as sql_common
from dj_queue.wakeup import notify_ready_queues_on_commit


class SlotHandoffMode(StrEnum):
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


def semaphore_acquire(key, *, limit, duration_seconds, config):
  alias = config.database_alias
  if database_capabilities(alias).backend_family == "sqlite":
    return bool(
      semaphore_acquire_many(
        key, count=1, limit=limit, duration_seconds=duration_seconds, config=config
      )
    )

  now = timezone.now()
  return backend_sql(alias).semaphore_acquire(
    alias,
    key,
    limit=limit,
    expires_at=now + timedelta(seconds=duration_seconds),
    now=now,
  )


def semaphore_acquire_many(
  key,
  *,
  count,
  limit,
  duration_seconds,
  config,
):
  if count <= 0:
    return 0
  alias = config.database_alias
  expires_at = timezone.now() + timedelta(seconds=duration_seconds)

  with transaction.atomic(using=alias, savepoint=False):
    acquired = min(count, limit)
    semaphore = None
    # cleanup can delete the conflicting row before it is locked
    while semaphore is None:
      if sql_common.create_ignore_conflicts(
        Semaphore,
        using=alias,
        key=key,
        value=limit - acquired,
        limit=limit,
        expires_at=expires_at,
      ):
        return acquired
      semaphore = Semaphore.objects.using(alias).select_for_update().filter(key=key).first()

    available = _available_slots(semaphore, limit)
    acquired = min(count, available)
    if acquired or semaphore.value != available or semaphore.limit != limit:
      semaphore.value = available - acquired
      semaphore.limit = limit
      if acquired:
        semaphore.expires_at = expires_at
      semaphore.save(using=alias, update_fields=["value", "limit", "expires_at", "updated_at"])
    return acquired


def semaphore_release(key, *, limit, duration_seconds, config):
  alias = config.database_alias
  now = timezone.now()
  available = _released_available_expression(limit)
  updated = (
    Semaphore.objects.using(alias)
    .filter(key=key)
    .update(
      value=available,
      limit=limit,
      expires_at=now + timedelta(seconds=duration_seconds),
      updated_at=now,
    )
  )
  return updated > 0


def release_recovered_concurrency_slots(jobs, *, config):
  grouped_jobs = defaultdict(list)
  for job in jobs:
    if job.backend_alias != config.backend_alias:
      raise EnqueueError(f"job {job.id} belongs to backend {job.backend_alias!r}")
    if job.concurrency_key:
      grouped_jobs[job.concurrency_key].append(job)

  fallback_jobs = []
  for key, group_jobs in grouped_jobs.items():
    settings = _recovered_release_settings(group_jobs, config=config)
    if settings is None:
      fallback_jobs.extend(group_jobs)
      continue
    limit, duration_seconds = settings
    _release_recovered_concurrency_group(
      key,
      release_count=len(group_jobs),
      limit=limit,
      duration_seconds=duration_seconds,
      config=config,
    )
  return fallback_jobs


def _recovered_release_settings(jobs, *, config):
  settings = {_release_settings(job, config=config) for job in jobs}
  if len(settings) > 1:
    return None
  return next(iter(settings))


def _release_settings(job, *, config, task=None):
  try:
    limit, duration_seconds, _ = concurrency_settings_for_job(job, task=task, config=config)
  except DispatchPolicyError:
    limit = _semaphore_limit(config.database_alias, job.concurrency_key) or 1
    duration_seconds = config.default_concurrency_duration
  return limit, duration_seconds


def _semaphore_limit(alias, key):
  return Semaphore.objects.using(alias).filter(key=key).values_list("limit", flat=True).first()


def _release_recovered_concurrency_group(
  key,
  *,
  release_count,
  limit,
  duration_seconds,
  config,
):
  alias = config.database_alias
  backend_alias = config.backend_alias
  now = timezone.now()
  expires_at = now + timedelta(seconds=duration_seconds)

  with transaction.atomic(using=alias, savepoint=False):
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

    available = _available_slots(semaphore, limit, released=release_count)
    promote_count = min(release_count, available, len(blocked_rows))
    promoted_rows = blocked_rows[:promote_count]
    if promoted_rows:
      promoted_rows = _consume_selected_rows(alias, BlockedExecution, promoted_rows)

    promoted_jobs = [blocked.job for blocked in promoted_rows]
    semaphore.value = available - len(promoted_jobs)
    semaphore.limit = limit
    semaphore.expires_at = expires_at
    semaphore.updated_at = now
    semaphore.save(
      using=alias,
      update_fields=["value", "limit", "expires_at", "updated_at"],
    )

    _bulk_create_ready_executions(
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
      tuple(dict.fromkeys(job.queue_name for job in promoted_jobs)), config=config
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
  available = _released_available_expression(limit)
  updated = (
    Semaphore.objects.using(alias)
    .filter(key=key, value__gt=F("limit") - Value(limit) - Value(1))
    .update(
      value=available - Value(1),
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


def _available_slots(semaphore, limit, *, released=0):
  return min(limit, max(0, semaphore.value + limit - semaphore.limit + released))


def _task_option(task, name, default=None):
  if hasattr(task, name):
    return getattr(task, name)
  return getattr(task.func, name, default)


def concurrency_settings(task, *, config):
  limit = _task_option(task, "concurrency_limit")
  if limit in (None, ""):
    raise EnqueueError("concurrency_limit is required when concurrency_key is set")

  limit = _positive_int_option(limit, "concurrency_limit")
  duration_seconds = _positive_int_option(
    _task_option(task, "concurrency_duration", config.default_concurrency_duration),
    "concurrency_duration",
  )
  on_conflict = str(_task_option(task, "on_conflict", "block"))
  if on_conflict not in {"block", "discard"}:
    raise EnqueueError("on_conflict must be 'block' or 'discard'")
  return limit, duration_seconds, on_conflict


def concurrency_settings_for_job(job, *, config, task=None):
  policy = (
    job.concurrency_limit,
    job.concurrency_duration,
    job.concurrency_on_conflict,
  )
  if all(value is None for value in policy):
    try:
      if task is None:
        task = import_string(job.task_path)
      return concurrency_settings(task, config=config)
    except (AttributeError, EnqueueError, ImportError) as error:
      raise DispatchPolicyError(
        f"job {job.id} has no resolvable concurrency policy: {error}"
      ) from error

  if any(value is None for value in policy):
    raise DispatchPolicyError(f"job {job.id} has incomplete persisted concurrency policy")

  try:
    limit = _positive_int_option(job.concurrency_limit, "concurrency_limit")
    duration_seconds = _positive_int_option(
      job.concurrency_duration,
      "concurrency_duration",
    )
    on_conflict = str(job.concurrency_on_conflict)
    if on_conflict not in {"block", "discard"}:
      raise EnqueueError("on_conflict must be 'block' or 'discard'")
  except EnqueueError as error:
    raise DispatchPolicyError(
      f"job {job.id} has invalid persisted concurrency policy: {error}"
    ) from error
  return limit, duration_seconds, on_conflict


def unblock_next_blocked_job(
  key,
  *,
  limit,
  duration_seconds,
  slot_handoff,
  config,
):
  alias = config.database_alias
  backend_alias = config.backend_alias
  now = timezone.now()

  with transaction.atomic(using=alias, savepoint=False):
    job_ref = _consume_blocked_job_for_slot(
      alias,
      backend_alias=backend_alias,
      key=key,
      limit=limit,
      duration_seconds=duration_seconds,
      now=now,
      use_skip_locked=config.use_skip_locked,
      slot_handoff=slot_handoff,
    )
    if job_ref is None:
      return None

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
  notify_ready_queues_on_commit((job_ref.queue_name,), config=config)
  return job_ref


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
  capabilities = database_capabilities(alias)
  if (
    slot_handoff is SlotHandoffMode.RELEASE_CLAIMED and capabilities.backend_family == "postgresql"
  ):
    blocked = backend_sql(alias).consume_next_blocked_job_with_released_slot(
      alias,
      backend_alias=backend_alias,
      key=key,
      limit=limit,
      duration_seconds=duration_seconds,
      now=now,
      use_skip_locked=use_skip_locked and capabilities.supports_skip_locked,
    )
    if blocked is None:
      return None
    if blocked["job_backend_alias"] != backend_alias:
      raise EnqueueError(
        f"job {blocked['job_id']} belongs to backend {blocked['job_backend_alias']!r}"
      )
    if not blocked["slot_acquired"]:
      return None
    return BlockedJobRef(
      id=blocked["job_id"],
      backend_alias=backend_alias,
      queue_name=blocked["queue_name"],
      priority=blocked["priority"],
      concurrency_key=blocked["concurrency_key"],
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

  take_slot = (
    _handoff_released_claimed_slot
    if slot_handoff is SlotHandoffMode.RELEASE_CLAIMED
    else _consume_released_semaphore_slot
  )
  if not take_slot(alias, key, limit=limit, duration_seconds=duration_seconds, now=now):
    return None
  if not _consume_selected_rows(alias, BlockedExecution, [blocked]):
    raise EnqueueError("could not consume selected blocked job")
  return BlockedJobRef(
    id=blocked.job_id,
    backend_alias=backend_alias,
    queue_name=blocked.queue_name,
    priority=blocked.priority,
    concurrency_key=blocked.concurrency_key,
  )


def _create_ready_execution_after_blocked_consume(
  alias,
  *,
  job,
  backend_alias,
  queue_name,
  priority,
  ready_at,
):
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


def cleanup_expired_semaphores(*, config, batch_size=500):
  batch_size = _positive_int_option(batch_size, "batch_size")
  alias = config.database_alias

  def cleanup_transition():
    now = timezone.now()
    with transaction.atomic(using=alias):
      queryset = locked_queryset(
        Semaphore.objects.using(alias).filter(expires_at__lte=now),
        use_skip_locked=config.use_skip_locked,
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

  return retry_transient_database_errors(cleanup_transition, using=alias)


def promote_expired_blocked_jobs(*, config, batch_size=500):
  alias = config.database_alias
  backend_alias = config.backend_alias
  uses_serialized_writes = database_capabilities(alias).uses_serialized_writes

  def promote_transition():
    now = timezone.now()
    promoted_jobs = []
    policy_failures = []
    task_settings = {}
    with transaction.atomic(using=alias):
      queryset = (
        BlockedExecution.objects.using(alias)
        .select_related("job")
        .filter(backend_alias=backend_alias, expires_at__lte=now)
        .order_by("expires_at", "-priority", "id")
      )
      blocked_rows = list(
        locked_queryset(queryset, use_skip_locked=config.use_skip_locked)[:batch_size]
      )
      if not blocked_rows:
        return [], []
      _ensure_state_rows_belong_to_backend(blocked_rows, backend_alias)
      if uses_serialized_writes:
        blocked_rows = _consume_selected_rows(alias, BlockedExecution, blocked_rows)
        if not blocked_rows:
          return [], []

      for blocked in blocked_rows:
        job = blocked.job
        try:
          limit, duration_seconds = _blocked_concurrency_settings(
            job, task_settings, config=config
          )
        except DispatchPolicyError as error:
          if _fail_blocked_policy(alias, blocked, error, consumed=uses_serialized_writes):
            policy_failures.append((job, error))
          continue

        if not semaphore_acquire(
          blocked.concurrency_key,
          limit=limit,
          duration_seconds=duration_seconds,
          config=config,
        ):
          _renew_blocked_execution(
            alias,
            blocked,
            expires_at=now + timedelta(seconds=duration_seconds),
            consumed=uses_serialized_writes,
          )
          continue

        _promote_blocked_execution(alias, blocked, ready_at=now, consumed=uses_serialized_writes)
        promoted_jobs.append(job)
    return promoted_jobs, policy_failures

  promoted_jobs, policy_failures = retry_transient_database_errors(promote_transition, using=alias)

  for job in promoted_jobs:
    log_event(
      "job.unblocked",
      backend_alias=backend_alias,
      job_id=str(job.id),
      concurrency_key=job.concurrency_key,
    )
    notify_ready_queues_on_commit((job.queue_name,), config=config)
  for job, error in policy_failures:
    log_event(
      "job.failed",
      backend_alias=backend_alias,
      job_id=str(job.id),
      failure_kind="dispatch_policy",
      exception_class=exception_path(error),
      message=str(error),
    )
  return promoted_jobs


def _blocked_concurrency_settings(job, task_settings, *, config):
  historical_policy = all(
    value is None
    for value in (
      job.concurrency_limit,
      job.concurrency_duration,
      job.concurrency_on_conflict,
    )
  )
  if historical_policy and job.task_path in task_settings:
    return task_settings[job.task_path]

  limit, duration_seconds, _ = concurrency_settings_for_job(job, config=config)
  if historical_policy:
    task_settings[job.task_path] = (limit, duration_seconds)
  return limit, duration_seconds


def _fail_blocked_policy(alias, blocked, error, *, consumed):
  if not consumed and not _consume_selected_rows(alias, BlockedExecution, [blocked]):
    return False
  _ensure_no_other_execution_state(alias, blocked.job, ignored_models=(BlockedExecution,))
  _failed_execution_row(blocked.job, error).save(using=alias)
  return True


def _promote_blocked_execution(alias, blocked, *, ready_at, consumed):
  if not consumed:
    blocked.delete(using=alias)
  _create_ready_execution(
    alias,
    job=blocked.job,
    backend_alias=blocked.backend_alias,
    queue_name=blocked.queue_name,
    priority=blocked.priority,
    ready_at=ready_at,
    check_conflicts=True,
  )


def _renew_blocked_execution(alias, blocked, *, expires_at, consumed):
  if consumed:
    _create_blocked_execution(
      alias,
      blocked.job,
      backend_alias=blocked.backend_alias,
      queue_name=blocked.queue_name,
      priority=blocked.priority,
      concurrency_key=blocked.concurrency_key,
      expires_at=expires_at,
      check_conflicts=True,
    )
    return
  blocked.expires_at = expires_at
  blocked.save(using=alias, update_fields=["expires_at"])


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


def release_concurrency_slot(job, *, config, task=None):
  if not job.concurrency_key:
    return

  limit, duration_seconds = _release_settings(job, task=task, config=config)
  if (
    unblock_next_blocked_job(
      job.concurrency_key,
      limit=limit,
      duration_seconds=duration_seconds,
      slot_handoff=SlotHandoffMode.RELEASE_CLAIMED,
      config=config,
    )
    is not None
  ):
    return

  semaphore_release(
    job.concurrency_key,
    limit=limit,
    duration_seconds=duration_seconds,
    config=config,
  )
  unblock_next_blocked_job(
    job.concurrency_key,
    limit=limit,
    duration_seconds=duration_seconds,
    slot_handoff=SlotHandoffMode.CONSUME_RELEASED,
    config=config,
  )
