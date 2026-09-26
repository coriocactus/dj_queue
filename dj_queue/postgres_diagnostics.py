from contextlib import nullcontext

from django.db import connections, transaction
from django.db.utils import DatabaseError
from django.utils import timezone

from dj_queue.config import load_backend_config
from dj_queue.db import database_capabilities, get_database_alias, queue_cursor
from dj_queue.models import (
  BlockedExecution,
  ClaimedExecution,
  FailedExecution,
  Job,
  Pause,
  Process,
  ReadyExecution,
  RecurringExecution,
  RecurringTask,
  ScheduledExecution,
  Semaphore,
)

POSTGRES_DIAGNOSTIC_TABLE_MODELS = (
  Job,
  ReadyExecution,
  ScheduledExecution,
  ClaimedExecution,
  BlockedExecution,
  FailedExecution,
  Semaphore,
  Process,
  RecurringTask,
  RecurringExecution,
  Pause,
)


POSTGRES_AUTOVACUUM_TABLE_MODELS = (
  Job,
  ReadyExecution,
  ScheduledExecution,
  ClaimedExecution,
  BlockedExecution,
  FailedExecution,
  RecurringExecution,
)


POSTGRES_AUTOVACUUM_STORAGE_PARAMETERS = {
  "autovacuum_vacuum_scale_factor": "0.01",
  "autovacuum_vacuum_threshold": "50",
  "autovacuum_analyze_scale_factor": "0.02",
  "autovacuum_analyze_threshold": "50",
}


def postgres_diagnostics_for_backend(*, backend_alias, max_age=None, now=None):
  alias = get_database_alias(backend_alias)
  if database_capabilities(alias).backend_family != "postgresql":
    return None
  if now is None:
    now = timezone.now()
  if max_age is None:
    max_age = load_backend_config(backend_alias).process_alive_threshold

  try:
    boundary = (
      nullcontext() if connections[alias].get_autocommit() else transaction.atomic(using=alias)
    )
    with boundary:
      return {
        "queue_tables": postgres_queue_table_rows(backend_alias=backend_alias),
        "xmin_activity": postgres_xmin_activity_rows(backend_alias=backend_alias),
        "replication_slots": postgres_replication_slot_rows(backend_alias=backend_alias),
        "prepared_transactions": postgres_prepared_transaction_rows(backend_alias=backend_alias),
        "long_transaction_threshold_seconds": float(max_age),
        "captured_at": now,
      }
  except DatabaseError as error:
    return {"error": str(error), "captured_at": now}


def postgres_xmin_blocker_rows(diagnostics):
  threshold = diagnostics["long_transaction_threshold_seconds"]
  activity_rows = [
    row
    for row in diagnostics["xmin_activity"]
    if (row["transaction_age_seconds"] or 0) >= threshold or row["state"] == "idle in transaction"
  ]
  slot_rows = [
    row
    for row in diagnostics["replication_slots"]
    if row["xmin_age"] is not None or row["catalog_xmin_age"] is not None
  ]
  return (*activity_rows, *slot_rows, *diagnostics["prepared_transactions"])


def postgres_queue_table_rows(*, backend_alias):
  table_names = tuple(
    dict.fromkeys(model._meta.db_table for model in POSTGRES_DIAGNOSTIC_TABLE_MODELS)
  )
  placeholders = ", ".join("%s" for _name in table_names)
  with queue_cursor(backend_alias) as cursor:
    cursor.execute(
      f"""
      SELECT
        relname,
        n_live_tup,
        n_dead_tup,
        CASE
          WHEN n_live_tup + n_dead_tup = 0 THEN 0
          ELSE n_dead_tup::float8 / (n_live_tup + n_dead_tup)
        END AS dead_tuple_ratio,
        last_vacuum,
        last_autovacuum,
        vacuum_count,
        autovacuum_count,
        pg_total_relation_size(relid)
      FROM pg_stat_user_tables
      WHERE relname IN ({placeholders})
      ORDER BY relname
      """,
      table_names,
    )
    return tuple(
      {
        "table_name": row[0],
        "live_tuples": row[1],
        "dead_tuples": row[2],
        "dead_tuple_ratio": row[3],
        "last_vacuum": row[4],
        "last_autovacuum": row[5],
        "vacuum_count": row[6],
        "autovacuum_count": row[7],
        "total_relation_bytes": row[8],
      }
      for row in cursor.fetchall()
    )


def postgres_autovacuum_sql(connection):
  table_names = tuple(
    dict.fromkeys(model._meta.db_table for model in POSTGRES_AUTOVACUUM_TABLE_MODELS)
  )
  settings = ", ".join(
    f"{name} = {value}" for name, value in POSTGRES_AUTOVACUUM_STORAGE_PARAMETERS.items()
  )
  return tuple(
    f"ALTER TABLE {connection.ops.quote_name(table_name)} SET ({settings});"
    for table_name in table_names
  )


def postgres_xmin_activity_rows(*, backend_alias):
  with queue_cursor(backend_alias) as cursor:
    cursor.execute(
      """
      SELECT
        pid,
        usename,
        application_name,
        client_addr::text,
        state,
        wait_event_type,
        wait_event,
        EXTRACT(EPOCH FROM now() - xact_start)::float8,
        age(backend_xmin)
      FROM pg_stat_activity
      WHERE pid <> pg_backend_pid()
        AND (backend_xmin IS NOT NULL OR xact_start IS NOT NULL)
      ORDER BY xact_start NULLS LAST, pid
      LIMIT 20
      """
    )
    return tuple(
      {
        "pid": row[0],
        "user": row[1],
        "application_name": row[2],
        "client_addr": row[3],
        "state": row[4],
        "wait_event_type": row[5],
        "wait_event": row[6],
        "transaction_age_seconds": row[7],
        "backend_xmin_age": row[8],
      }
      for row in cursor.fetchall()
    )


def postgres_replication_slot_rows(*, backend_alias):
  with queue_cursor(backend_alias) as cursor:
    cursor.execute(
      """
      SELECT
        slot_name,
        slot_type,
        active,
        age(xmin),
        age(catalog_xmin)
      FROM pg_replication_slots
      WHERE xmin IS NOT NULL OR catalog_xmin IS NOT NULL
      ORDER BY slot_name
      LIMIT 20
      """
    )
    return tuple(
      {
        "slot_name": row[0],
        "slot_type": row[1],
        "active": row[2],
        "xmin_age": row[3],
        "catalog_xmin_age": row[4],
      }
      for row in cursor.fetchall()
    )


def postgres_prepared_transaction_rows(*, backend_alias):
  with queue_cursor(backend_alias) as cursor:
    cursor.execute(
      """
      SELECT
        gid,
        owner,
        database,
        EXTRACT(EPOCH FROM now() - prepared)::float8
      FROM pg_prepared_xacts
      ORDER BY prepared, gid
      LIMIT 20
      """
    )
    return tuple(
      {
        "gid": row[0],
        "owner": row[1],
        "database": row[2],
        "transaction_age_seconds": row[3],
      }
      for row in cursor.fetchall()
    )
