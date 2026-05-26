from datetime import timedelta

from django.db import connections, transaction
from django.db.models import Case, F, IntegerField, Value, When
from django.db.models.functions import Greatest, Least
from django.utils import timezone
from django.utils.module_loading import import_string

from dj_queue.config import load_backend_config
from dj_queue.db import database_capabilities, get_database_alias, locked_queryset
from dj_queue.exceptions import EnqueueError
from dj_queue.log import log_event
from dj_queue.models import (
  BlockedExecution,
  ClaimedExecution,
  Job,
  ReadyExecution,
  Semaphore,
)
from dj_queue.operations._helpers import (
  _consume_selected_rows,
  _create_blocked_execution,
  _create_ready_execution_locked,
  _ensure_state_rows_belong_to_backend,
  _lock_active_pauses,
  _state_absence_checks_sql,
  _state_models_except,
  _task_option,
)
from dj_queue.operations._insert import create_ignore_conflicts
from dj_queue.wakeup import notify_ready_queues_on_commit


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

  if backend_family in {"mysql", "mariadb"}:
    return _mysql_family_semaphore_acquire(
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

    Semaphore.objects.using(alias).filter(key=key).update(
      value=reconciled_available,
      limit=limit,
      updated_at=now,
    )
  return False


def _mysql_family_semaphore_acquire(alias, key, *, limit, expires_at, now):
  connection = connections[alias]
  table = connection.ops.quote_name(Semaphore._meta.db_table)
  pk_column = connection.ops.quote_name(Semaphore._meta.pk.column)
  key_column = connection.ops.quote_name("key")
  value_column = connection.ops.quote_name("value")
  limit_column = connection.ops.quote_name("limit")
  expires_at_column = connection.ops.quote_name("expires_at")
  created_at_column = connection.ops.quote_name("created_at")
  updated_at_column = connection.ops.quote_name("updated_at")
  reconciled_available = f"LEAST(VALUES({limit_column}), GREATEST(0, {value_column} + VALUES({limit_column}) - {limit_column}))"

  # one upsert avoids mysql-family deadlocks from mixing ignored inserts and follow-up updates
  with connection.cursor() as cursor:
    cursor.execute(
      f"""
      INSERT INTO {table} (
        {key_column},
        {value_column},
        {limit_column},
        {expires_at_column},
        {created_at_column},
        {updated_at_column}
      )
      VALUES (%s, %s, %s, %s, %s, %s)
      ON DUPLICATE KEY UPDATE
        {expires_at_column} = IF(
          {reconciled_available} > 0,
          %s,
          {expires_at_column}
        ),
        {updated_at_column} = %s,
        {pk_column} = IF(
          {reconciled_available} > 0,
          LAST_INSERT_ID({pk_column}),
          LAST_INSERT_ID(0) + {pk_column}
        ),
        {value_column} = IF(
          {reconciled_available} > 0,
          {reconciled_available} - 1,
          {reconciled_available}
        ),
        {limit_column} = VALUES({limit_column})
      """,
      [key, limit - 1, limit, expires_at, now, now, expires_at, now],
    )
    return cursor.lastrowid != 0


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
  handoff_released_slot=False,
  release_slot=False,
):
  alias = get_database_alias(backend_alias)
  now = timezone.now()

  with _operation_atomic(alias):
    blocked = _consume_next_blocked_job(
      alias,
      backend_alias=backend_alias,
      key=key,
      use_skip_locked=use_skip_locked,
    )
    if blocked is None:
      return None
    if blocked["job_backend_alias"] != backend_alias:
      raise EnqueueError(
        f"job {blocked['job_id']} belongs to backend {blocked['job_backend_alias']!r}"
      )

    slot_acquired = False
    if release_slot:
      slot_acquired = _handoff_released_claimed_slot(
        alias,
        key,
        limit=limit,
        duration_seconds=duration_seconds,
        now=now,
      )
    elif handoff_released_slot:
      slot_acquired = _consume_released_semaphore_slot(
        alias,
        key,
        limit=limit,
        duration_seconds=duration_seconds,
        now=now,
      )

    if not slot_acquired and not release_slot:
      slot_acquired = semaphore_acquire(
        key,
        limit=limit,
        duration_seconds=duration_seconds,
        backend_alias=backend_alias,
      )

    mock_job = Job(
      id=blocked["job_id"],
      queue_name=blocked["queue_name"],
      priority=blocked["priority"],
      concurrency_key=blocked["concurrency_key"],
      backend_alias=backend_alias,
    )

    if not slot_acquired:
      _create_blocked_execution(
        alias,
        mock_job,
        backend_alias=backend_alias,
        queue_name=blocked["queue_name"],
        priority=blocked["priority"],
        concurrency_key=blocked["concurrency_key"],
        expires_at=blocked["expires_at"],
        check_conflicts=True,
      )
      return None

    _create_ready_execution_after_blocked_consume(
      alias,
      job=mock_job,
      backend_alias=backend_alias,
      queue_name=blocked["queue_name"],
      priority=blocked["priority"],
      ready_at=now,
    )

  log_event(
    "job.unblocked",
    job_id=str(mock_job.id),
    concurrency_key=key,
  )
  notify_ready_queues_on_commit((blocked["queue_name"],), backend_alias=backend_alias)
  return mock_job


def _consume_next_blocked_job(alias, *, backend_alias, key, use_skip_locked):
  capabilities = database_capabilities(alias)
  if capabilities.backend_family == "postgresql":
    return _postgres_consume_next_blocked_job(
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


def _postgres_consume_next_blocked_job(alias, *, backend_alias, key, use_skip_locked):
  connection = connections[alias]
  quote = connection.ops.quote_name
  table = quote(BlockedExecution._meta.db_table)
  pk_column = quote(BlockedExecution._meta.pk.column)
  job_id_column = quote(BlockedExecution._meta.get_field("job").column)
  backend_alias_column = quote(BlockedExecution._meta.get_field("backend_alias").column)
  queue_name_column = quote(BlockedExecution._meta.get_field("queue_name").column)
  priority_column = quote(BlockedExecution._meta.get_field("priority").column)
  concurrency_key_column = quote(BlockedExecution._meta.get_field("concurrency_key").column)
  expires_at_column = quote(BlockedExecution._meta.get_field("expires_at").column)
  jobs_table = quote(Job._meta.db_table)
  jobs_id_column = quote(Job._meta.get_field("id").column)
  jobs_backend_alias_column = quote(Job._meta.get_field("backend_alias").column)
  skip_locked_sql = " SKIP LOCKED" if use_skip_locked else ""

  with connection.cursor() as cursor:
    cursor.execute(
      f"""
      WITH selected AS (
        SELECT
          {table}.{pk_column},
          {jobs_table}.{jobs_backend_alias_column} AS job_backend_alias
        FROM {table}
        JOIN {jobs_table}
          ON {jobs_table}.{jobs_id_column} = {table}.{job_id_column}
        WHERE {table}.{backend_alias_column} = %s
          AND {table}.{concurrency_key_column} = %s
        ORDER BY {table}.{priority_column} DESC, {table}.{pk_column} ASC
        LIMIT 1
        FOR UPDATE OF {table}{skip_locked_sql}
      ), deleted AS (
        DELETE FROM {table}
        USING selected
        WHERE {table}.{pk_column} = selected.{pk_column}
        RETURNING
          {table}.{job_id_column},
          {table}.{queue_name_column},
          {table}.{priority_column},
          {table}.{concurrency_key_column},
          {table}.{expires_at_column},
          selected.job_backend_alias
      )
      SELECT * FROM deleted
      """,
      [backend_alias, key],
    )
    row = cursor.fetchone()

  if row is None:
    return None
  return {
    "job_id": row[0],
    "queue_name": row[1],
    "priority": row[2],
    "concurrency_key": row[3],
    "expires_at": row[4],
    "job_backend_alias": row[5],
  }


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
  connection = connections[alias]
  quote = connection.ops.quote_name
  ready_table = quote(ReadyExecution._meta.db_table)
  job_id_column = quote(ReadyExecution._meta.get_field("job").column)
  backend_alias_column = quote(ReadyExecution._meta.get_field("backend_alias").column)
  queue_name_column = quote(ReadyExecution._meta.get_field("queue_name").column)
  priority_column = quote(ReadyExecution._meta.get_field("priority").column)
  created_at_column = quote(ReadyExecution._meta.get_field("created_at").column)
  latency_started_at_column = quote(ReadyExecution._meta.get_field("latency_started_at").column)
  job_id = Job._meta.get_field("id").get_db_prep_value(
    job.pk,
    connection=connection,
    prepared=False,
  )
  state_models = _state_models_except(BlockedExecution)
  state_checks = _state_absence_checks_sql(
    state_models,
    quote=quote,
    job_id_expression="%s",
  )

  with connection.cursor() as cursor:
    cursor.execute(
      f"""
      INSERT INTO {ready_table} (
        {job_id_column},
        {backend_alias_column},
        {queue_name_column},
        {priority_column},
        {created_at_column},
        {latency_started_at_column}
      )
      SELECT %s, %s, %s, %s, %s, %s
      WHERE {state_checks}
      """,
      [
        job_id,
        backend_alias,
        queue_name,
        priority,
        ready_at,
        ready_at,
        *([job_id] * len(state_models)),
      ],
    )
    created = cursor.rowcount

  if created != 1:
    raise EnqueueError(f"job {job.id} already has an execution-state row")


def cleanup_expired_semaphores(*, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  claimed_concurrency_keys = (
    ClaimedExecution.objects.using(alias)
    .exclude(job__concurrency_key__isnull=True)
    .exclude(job__concurrency_key="")
    .values_list("job__concurrency_key", flat=True)
  )
  ready_concurrency_keys = (
    ReadyExecution.objects.using(alias)
    .exclude(job__concurrency_key__isnull=True)
    .exclude(job__concurrency_key="")
    .values_list("job__concurrency_key", flat=True)
  )
  queryset = (
    Semaphore.objects.using(alias)
    .filter(expires_at__lte=timezone.now())
    .exclude(key__in=claimed_concurrency_keys)
    .exclude(key__in=ready_concurrency_keys)
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
    log_event("job.unblocked", job_id=str(job.id), concurrency_key=job.concurrency_key)
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
