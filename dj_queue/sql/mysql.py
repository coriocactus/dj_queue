from django.db import connections

from dj_queue.models import Pause, ReadyExecution, Semaphore


def create_ignore_conflicts(connection, model, *, columns, placeholders, params):
  quote = connection.ops.quote_name
  table = quote(model._meta.db_table)
  pk_column = quote(model._meta.pk.column)
  with connection.cursor() as cursor:
    cursor.execute(
      f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) "
      f"ON DUPLICATE KEY UPDATE {pk_column} = {pk_column} + LAST_INSERT_ID(0)",
      params,
    )
    return cursor.lastrowid != 0


def select_ready_row_ids(
  alias,
  *,
  backend_alias,
  limit,
  use_skip_locked,
):
  connection = connections[alias]
  quote = connection.ops.quote_name
  ready_table = quote(ReadyExecution._meta.db_table)
  pause_table = quote(Pause._meta.db_table)
  ready_index = quote("djq_re_b_prio_d_idx")
  ready_pk_column = quote(ReadyExecution._meta.pk.column)
  ready_backend_column = quote(ReadyExecution._meta.get_field("backend_alias").column)
  ready_queue_column = quote(ReadyExecution._meta.get_field("queue_name").column)
  ready_priority_column = quote(ReadyExecution._meta.get_field("priority").column)
  pause_backend_column = quote(Pause._meta.get_field("backend_alias").column)
  pause_queue_column = quote(Pause._meta.get_field("queue_name").column)
  skip_locked_sql = " SKIP LOCKED" if use_skip_locked else ""

  with connection.cursor() as cursor:
    cursor.execute(
      f"""
      SELECT ready.{ready_pk_column}
      FROM {ready_table} ready FORCE INDEX ({ready_index})
      WHERE ready.{ready_backend_column} = %s
        AND NOT EXISTS (
          SELECT 1
          FROM {pause_table} pause
          WHERE pause.{pause_backend_column} = %s
            AND pause.{pause_queue_column} = ready.{ready_queue_column}
        )
      ORDER BY ready.{ready_priority_column} DESC, ready.{ready_pk_column} ASC
      LIMIT %s
      FOR UPDATE{skip_locked_sql}
      """,
      [backend_alias, backend_alias, limit],
    )
    return [row[0] for row in cursor.fetchall()]


def consume_ready_rows(alias, rows):
  connection = connections[alias]
  quote = connection.ops.quote_name
  ready_table = quote(ReadyExecution._meta.db_table)
  ready_pk_column = quote(ReadyExecution._meta.pk.column)
  selected_rows_sql = " UNION ALL ".join(
    ["SELECT %s AS selected_id", *["SELECT %s"] * (len(rows) - 1)]
  )
  with connection.cursor() as cursor:
    cursor.execute(
      f"DELETE ready FROM ({selected_rows_sql}) selected "
      f"STRAIGHT_JOIN {ready_table} ready "
      f"ON ready.{ready_pk_column} = selected.selected_id",
      [row.pk for row in rows],
    )
  return rows


def semaphore_acquire(alias, key, *, limit, expires_at, now):
  connection = connections[alias]
  table = connection.ops.quote_name(Semaphore._meta.db_table)
  pk_column = connection.ops.quote_name(Semaphore._meta.pk.column)
  key_column = connection.ops.quote_name("key")
  value_column = connection.ops.quote_name("value")
  active_count_column = connection.ops.quote_name("active_count")
  limit_column = connection.ops.quote_name("limit")
  expires_at_column = connection.ops.quote_name("expires_at")
  created_at_column = connection.ops.quote_name("created_at")
  updated_at_column = connection.ops.quote_name("updated_at")
  reconciled_available = f"LEAST(%s, GREATEST(0, {value_column} + %s - {limit_column}))"
  should_touch = (
    f"{reconciled_available} > 0 "
    f"OR {value_column} <> {reconciled_available} "
    f"OR {active_count_column} IS NULL "
    f"OR {active_count_column} <> %s - {reconciled_available} "
    f"OR {limit_column} <> %s"
  )
  reconciled_available_params = (limit, limit)

  # one upsert avoids mysql-family deadlocks from mixing ignored inserts and follow-up updates
  with connection.cursor() as cursor:
    cursor.execute(
      f"""
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
      ON DUPLICATE KEY UPDATE
        {expires_at_column} = IF(
          {reconciled_available} > 0,
          %s,
          {expires_at_column}
        ),
        {updated_at_column} = IF(
          {should_touch},
          %s,
          {updated_at_column}
        ),
        {pk_column} = IF(
          {reconciled_available} > 0,
          LAST_INSERT_ID({pk_column}),
          LAST_INSERT_ID(0) + {pk_column}
        ),
        {active_count_column} = %s - IF(
          {reconciled_available} > 0,
          {reconciled_available} - 1,
          {reconciled_available}
        ),
        {value_column} = IF(
          {reconciled_available} > 0,
          {reconciled_available} - 1,
          {reconciled_available}
        ),
        {limit_column} = %s
      """,
      [
        key,
        limit - 1,
        1,
        limit,
        expires_at,
        now,
        now,
        *reconciled_available_params,
        expires_at,
        *reconciled_available_params,
        *reconciled_available_params,
        limit,
        *reconciled_available_params,
        limit,
        now,
        *reconciled_available_params,
        limit,
        *reconciled_available_params,
        *reconciled_available_params,
        *reconciled_available_params,
        *reconciled_available_params,
        *reconciled_available_params,
        *reconciled_available_params,
        limit,
      ],
    )
    return cursor.lastrowid != 0
