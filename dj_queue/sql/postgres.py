from datetime import timedelta

from django.db import connections

from dj_queue.exceptions import EnqueueError
from dj_queue.models import (
  BlockedExecution,
  ClaimedExecution,
  Job,
  Pause,
  ReadyExecution,
  Semaphore,
)
from dj_queue.sql.state import state_absence_checks_sql, state_models_except


def create_ignore_conflicts(connection, model, *, columns, placeholders, params):
  table = connection.ops.quote_name(model._meta.db_table)
  with connection.cursor() as cursor:
    cursor.execute(
      f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING",
      params,
    )
    return cursor.rowcount > 0


def semaphore_acquire(alias, key, *, limit, expires_at, now):
  connection = connections[alias]
  quote = connection.ops.quote_name
  table = quote(Semaphore._meta.db_table)
  key_column = quote("key")
  value_column = quote("value")
  active_count_column = quote("active_count")
  limit_column = quote("limit")
  expires_at_column = quote("expires_at")
  created_at_column = quote("created_at")
  updated_at_column = quote("updated_at")
  acquired_available = (
    f"GREATEST(0, EXCLUDED.{limit_column} - ({table}.{active_count_column} + 1))"
  )
  current_available = f"GREATEST(0, %s - {active_count_column})"

  with connection.cursor() as cursor:
    cursor.execute(
      f"""
      WITH acquired AS (
        INSERT INTO {table} (
          {key_column},
          {value_column},
          {active_count_column},
          {limit_column},
          {expires_at_column},
          {created_at_column},
          {updated_at_column}
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ({key_column}) DO UPDATE
        SET
          {value_column} = {acquired_available},
          {active_count_column} = {table}.{active_count_column} + 1,
          {limit_column} = EXCLUDED.{limit_column},
          {expires_at_column} = EXCLUDED.{expires_at_column},
          {updated_at_column} = EXCLUDED.{updated_at_column}
        WHERE {table}.{active_count_column} < EXCLUDED.{limit_column}
        RETURNING TRUE AS acquired
      ), reconciled AS (
        UPDATE {table}
        SET
          {value_column} = {current_available},
          {limit_column} = %s,
          {updated_at_column} = %s
        WHERE {key_column} = %s
          AND NOT EXISTS (SELECT 1 FROM acquired)
          AND (
            {value_column} <> {current_available}
            OR {limit_column} <> %s
          )
        RETURNING FALSE AS acquired
      )
      SELECT acquired FROM acquired
      UNION ALL
      SELECT acquired FROM reconciled
      """,
      [
        key,
        limit - 1,
        1,
        limit,
        expires_at,
        now,
        now,
        limit,
        limit,
        now,
        key,
        limit,
        limit,
      ],
    )
    row = cursor.fetchone()

  return bool(row and row[0])


def consume_next_blocked_job_with_released_slot(
  alias,
  *,
  backend_alias,
  key,
  duration_seconds,
  now,
  use_skip_locked,
):
  connection = connections[alias]
  quote = connection.ops.quote_name
  blocked_table = quote(BlockedExecution._meta.db_table)
  blocked_pk_column = quote(BlockedExecution._meta.pk.column)
  blocked_job_id_column = quote(BlockedExecution._meta.get_field("job").column)
  blocked_backend_alias_column = quote(BlockedExecution._meta.get_field("backend_alias").column)
  blocked_queue_name_column = quote(BlockedExecution._meta.get_field("queue_name").column)
  blocked_priority_column = quote(BlockedExecution._meta.get_field("priority").column)
  blocked_concurrency_key_column = quote(
    BlockedExecution._meta.get_field("concurrency_key").column
  )
  blocked_expires_at_column = quote(BlockedExecution._meta.get_field("expires_at").column)
  jobs_table = quote(Job._meta.db_table)
  jobs_id_column = quote(Job._meta.get_field("id").column)
  jobs_backend_alias_column = quote(Job._meta.get_field("backend_alias").column)
  semaphore_table = quote(Semaphore._meta.db_table)
  semaphore_key_column = quote(Semaphore._meta.get_field("key").column)
  semaphore_value_column = quote(Semaphore._meta.get_field("value").column)
  semaphore_active_count_column = quote(Semaphore._meta.get_field("active_count").column)
  semaphore_limit_column = quote(Semaphore._meta.get_field("limit").column)
  semaphore_expires_at_column = quote(Semaphore._meta.get_field("expires_at").column)
  semaphore_updated_at_column = quote(Semaphore._meta.get_field("updated_at").column)
  skip_locked_sql = " SKIP LOCKED" if use_skip_locked else ""
  expires_at = now + timedelta(seconds=duration_seconds)
  current_available = (
    f"GREATEST(0, {semaphore_table}.{semaphore_limit_column} - "
    f"{semaphore_table}.{semaphore_active_count_column})"
  )

  with connection.cursor() as cursor:
    cursor.execute(
      f"""
      WITH selected AS (
        SELECT
          {blocked_table}.{blocked_pk_column},
          {blocked_table}.{blocked_job_id_column},
          {blocked_table}.{blocked_queue_name_column},
          {blocked_table}.{blocked_priority_column},
          {blocked_table}.{blocked_concurrency_key_column},
          {blocked_table}.{blocked_expires_at_column},
          {jobs_table}.{jobs_backend_alias_column} AS job_backend_alias
        FROM {blocked_table}
        JOIN {jobs_table}
          ON {jobs_table}.{jobs_id_column} = {blocked_table}.{blocked_job_id_column}
        WHERE {blocked_table}.{blocked_backend_alias_column} = %s
          AND {blocked_table}.{blocked_concurrency_key_column} = %s
        ORDER BY {blocked_table}.{blocked_priority_column} DESC, {blocked_table}.{blocked_pk_column} ASC
        LIMIT 1
        FOR UPDATE OF {blocked_table}{skip_locked_sql}
      ), slot AS (
        UPDATE {semaphore_table}
        SET
          {semaphore_value_column} = {current_available},
          {semaphore_expires_at_column} = %s,
          {semaphore_updated_at_column} = %s
        WHERE {semaphore_table}.{semaphore_key_column} = %s
          AND EXISTS (SELECT 1 FROM selected)
          AND {semaphore_table}.{semaphore_active_count_column} > 0
          AND {semaphore_table}.{semaphore_active_count_column} <= {semaphore_table}.{semaphore_limit_column}
        RETURNING TRUE AS acquired
      ), deleted AS (
        DELETE FROM {blocked_table}
        USING selected, slot
        WHERE {blocked_table}.{blocked_pk_column} = selected.{blocked_pk_column}
        RETURNING {blocked_table}.{blocked_pk_column}
      )
      SELECT
        selected.{blocked_job_id_column},
        selected.{blocked_queue_name_column},
        selected.{blocked_priority_column},
        selected.{blocked_concurrency_key_column},
        selected.{blocked_expires_at_column},
        selected.job_backend_alias,
        EXISTS (SELECT 1 FROM slot) AS slot_acquired,
        EXISTS (SELECT 1 FROM deleted) AS deleted
      FROM selected
      """,
      [
        backend_alias,
        key,
        expires_at,
        now,
        key,
      ],
    )
    row = cursor.fetchone()

  if row is None:
    return None
  if row[6] and not row[7]:
    raise EnqueueError("could not consume selected blocked job")
  return {
    "job_id": row[0],
    "queue_name": row[1],
    "priority": row[2],
    "concurrency_key": row[3],
    "expires_at": row[4],
    "job_backend_alias": row[5],
    "slot_acquired": row[6],
  }


def consume_next_blocked_job(alias, *, backend_alias, key, use_skip_locked):
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


def select_ready_rows_by_exact_queues(
  alias,
  *,
  backend_alias,
  selectors,
  limit,
  use_skip_locked,
):
  selectors = tuple(dict.fromkeys(selectors))
  if not selectors or limit <= 0:
    return []

  connection = connections[alias]
  quote = connection.ops.quote_name
  ready_table = quote(ReadyExecution._meta.db_table)
  jobs_table = quote(Job._meta.db_table)
  pause_table = quote(Pause._meta.db_table)
  ready_pk_column = quote(ReadyExecution._meta.pk.column)
  ready_job_column = quote(ReadyExecution._meta.get_field("job").column)
  ready_backend_column = quote(ReadyExecution._meta.get_field("backend_alias").column)
  ready_queue_column = quote(ReadyExecution._meta.get_field("queue_name").column)
  ready_priority_column = quote(ReadyExecution._meta.get_field("priority").column)
  job_pk_column = quote(Job._meta.pk.column)
  pause_backend_column = quote(Pause._meta.get_field("backend_alias").column)
  pause_queue_column = quote(Pause._meta.get_field("queue_name").column)
  ready_columns = _model_columns_sql(ReadyExecution, quote=quote, table_alias="ready")
  job_columns = _model_columns_sql(Job, quote=quote, table_alias="job")
  skip_locked_sql = " SKIP LOCKED" if use_skip_locked else ""

  ctes = []
  params = []
  for index, selector in enumerate(selectors):
    prior_counts = " + ".join(
      f"(SELECT COUNT(*) FROM selector_{prior_index})" for prior_index in range(index)
    )
    remaining_sql = "%s" if not prior_counts else f"GREATEST(0, %s - ({prior_counts}))"
    ctes.append(
      f"""
      selector_{index} AS (
        SELECT
          {index} AS selector_rank,
          {ready_columns},
          {job_columns}
        FROM {ready_table} ready
        JOIN {jobs_table} job
          ON job.{job_pk_column} = ready.{ready_job_column}
        WHERE ready.{ready_backend_column} = %s
          AND ready.{ready_queue_column} = %s
          AND NOT EXISTS (
            SELECT 1
            FROM {pause_table} pause
            WHERE pause.{pause_backend_column} = %s
              AND pause.{pause_queue_column} = ready.{ready_queue_column}
          )
        ORDER BY ready.{ready_priority_column} DESC, ready.{ready_pk_column} ASC
        LIMIT {remaining_sql}
        FOR UPDATE OF ready{skip_locked_sql}
      )
      """
    )
    params.extend([backend_alias, selector, backend_alias, limit])

  selectors_sql = " UNION ALL ".join(
    f"SELECT * FROM selector_{index}" for index in range(len(selectors))
  )
  with connection.cursor() as cursor:
    cursor.execute(
      f"""
      WITH {", ".join(ctes)}
      SELECT *
      FROM ({selectors_sql}) selected_ready
      ORDER BY selector_rank ASC, ready_priority DESC, ready_id ASC
      """,
      params,
    )
    columns = [column[0] for column in cursor.description]
    return [
      _ready_row_from_record(alias, dict(zip(columns, row, strict=True)))
      for row in cursor.fetchall()
    ]


def consume_ready_and_create_claimed_executions(alias, ready_rows, *, process, claimed_at):
  connection = connections[alias]
  quote = connection.ops.quote_name
  ready_table = quote(ReadyExecution._meta.db_table)
  ready_pk_column = quote(ReadyExecution._meta.pk.column)
  ready_job_id_column = quote(ReadyExecution._meta.get_field("job").column)
  claimed_table = quote(ClaimedExecution._meta.db_table)
  claimed_job_id_column = quote(ClaimedExecution._meta.get_field("job").column)
  process_id_column = quote(ClaimedExecution._meta.get_field("process").column)
  created_at_column = quote(ClaimedExecution._meta.get_field("created_at").column)
  values_sql = ", ".join(["(%s::bigint)"] * len(ready_rows))
  state_checks = state_absence_checks_sql(
    state_models_except(ReadyExecution),
    quote=quote,
    job_id_expression="claimed_input.job_id",
  )
  process_id = process.pk if process is not None else None

  with connection.cursor() as cursor:
    cursor.execute(
      f"""
      WITH selected_ready(id) AS (
        VALUES {values_sql}
      ), deleted_ready AS (
        DELETE FROM {ready_table}
        USING selected_ready
        WHERE {ready_table}.{ready_pk_column} = selected_ready.id
        RETURNING {ready_table}.{ready_job_id_column}
      ), claimed_input AS (
        SELECT
          deleted_ready.{ready_job_id_column} AS job_id,
          %s::bigint AS process_id,
          %s::timestamptz AS created_at
        FROM deleted_ready
      )
      INSERT INTO {claimed_table} (
        {claimed_job_id_column},
        {process_id_column},
        {created_at_column}
      )
      SELECT
        claimed_input.job_id,
        claimed_input.process_id,
        claimed_input.created_at
      FROM claimed_input
      WHERE {state_checks}
      RETURNING {claimed_job_id_column}
      """,
      [*[row.pk for row in ready_rows], process_id, claimed_at],
    )
    return [row[0] for row in cursor.fetchall()]


def delete_claimed_and_finish_job_if_no_execution_state(alias, job, return_value, *, finished_at):
  connection = connections[alias]
  quote = connection.ops.quote_name
  claimed_table = quote(ClaimedExecution._meta.db_table)
  claimed_job_id_column = quote(ClaimedExecution._meta.get_field("job").column)
  jobs_table = quote(Job._meta.db_table)
  job_id_column = quote(Job._meta.get_field("id").column)
  backend_alias_column = quote(Job._meta.get_field("backend_alias").column)
  finished_at_column = quote(Job._meta.get_field("finished_at").column)
  return_value_column = quote(Job._meta.get_field("return_value").column)
  updated_at_column = quote(Job._meta.get_field("updated_at").column)
  state_checks = state_absence_checks_sql(
    state_models_except(ClaimedExecution),
    quote=quote,
    job_id_expression=f"{jobs_table}.{job_id_column}",
  )
  job_id = Job._meta.get_field("id").get_db_prep_value(
    job.pk,
    connection=connection,
    prepared=False,
  )
  prepared_return_value = Job._meta.get_field("return_value").get_db_prep_save(
    return_value,
    connection=connection,
  )

  with connection.cursor() as cursor:
    cursor.execute(
      f"""
      WITH deleted_claim AS (
        DELETE FROM {claimed_table}
        WHERE {claimed_table}.{claimed_job_id_column} = %s
        RETURNING {claimed_job_id_column}
      ),
      updated_job AS (
        UPDATE {jobs_table}
        SET
          {finished_at_column} = %s,
          {return_value_column} = %s,
          {updated_at_column} = %s
        WHERE
          {jobs_table}.{job_id_column} = %s
          AND {jobs_table}.{backend_alias_column} = %s
          AND EXISTS (SELECT 1 FROM deleted_claim)
          AND {state_checks}
        RETURNING {job_id_column}
      )
      SELECT
        (SELECT COUNT(*) FROM deleted_claim),
        (SELECT COUNT(*) FROM updated_job)
      """,
      [job_id, finished_at, prepared_return_value, finished_at, job_id, job.backend_alias],
    )
    return cursor.fetchone()


def connection_capacity(alias):
  with connections[alias].cursor() as cursor:
    cursor.execute(
      "select current_setting('max_connections')::int, "
      "current_setting('superuser_reserved_connections')::int"
    )
    return cursor.fetchone()


def listen_channel(connection, channel):
  with connection.cursor() as cursor:
    cursor.execute(f"LISTEN {channel}")


def notify_channel(connection, channel, payload):
  with connection.cursor() as cursor:
    cursor.execute("SELECT pg_notify(%s, %s)", [channel, payload])


def _model_columns_sql(model, *, quote, table_alias):
  return ",\n          ".join(
    f"{table_alias}.{quote(field.column)} AS {quote(f'{table_alias}_{field.attname}')}"
    for field in model._meta.concrete_fields
  )


def _ready_row_from_record(alias, record):
  connection = connections[alias]
  job = _model_from_record(Job, alias, connection, record, prefix="job")
  ready = _model_from_record(ReadyExecution, alias, connection, record, prefix="ready")
  ready.job = job
  return ready


def _model_from_record(model, alias, connection, record, *, prefix):
  obj = model(
    **{
      field.attname: _field_value_from_record(
        field,
        record[f"{prefix}_{field.attname}"],
        connection,
      )
      for field in model._meta.concrete_fields
    }
  )
  obj._state.adding = False
  obj._state.db = alias
  return obj


def _field_value_from_record(field, value, connection):
  converter = getattr(field, "from_db_value", None)
  if converter is None:
    return value
  return converter(value, None, connection)
