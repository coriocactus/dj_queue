import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta

from django.conf import settings
from django.db.models import (
  Case,
  Count,
  F,
  IntegerField,
  Max,
  Min,
  OuterRef,
  Q,
  Subquery,
  Value,
  When,
)
from django.db.models.functions import Coalesce
from django.db.utils import DatabaseError
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from dj_queue.config import configured_backend_aliases as configured_dj_queue_backend_aliases
from dj_queue.config import load_backend_config
from dj_queue.cron import next_cron_run
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
from dj_queue.queue_selectors import queue_matches_selectors
from dj_queue.queue_state import (
  empty_queue_state_summary,
  queue_state_summaries_by_queue,
  queue_state_summary,
)

_NOT_PROVIDED = object()
POSTGRES_DEAD_TUPLE_WARNING_COUNT = 10_000
POSTGRES_DEAD_TUPLE_WARNING_RATIO = 0.2
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


@dataclass(frozen=True, slots=True)
class BackendChoice:
  alias: str
  database_alias: str


@dataclass(frozen=True, slots=True)
class BackendSnapshot:
  backend_alias: str
  queue_database_alias: str
  process_alive_threshold: int
  queue_rows: tuple[dict, ...]
  process_rows: tuple[dict, ...]
  recurring_rows: tuple[dict, ...]
  semaphore_rows: tuple[dict, ...]
  runner_metrics: dict
  failed_metrics: dict | None = None
  postgres_diagnostics: dict | None = None

  def stats_row(self):
    row = {
      "backend_alias": self.backend_alias,
      "queue_database_alias": self.queue_database_alias,
      "process_alive_threshold": self.process_alive_threshold,
      "queues": self.queue_rows,
      "runner_metrics": self.runner_metrics,
      "recurring": self.recurring_rows,
      "semaphores": self.semaphore_rows,
      "failed_jobs": self.failed_metrics,
    }
    if self.postgres_diagnostics is not None:
      row["postgres_diagnostics"] = self.postgres_diagnostics
    return row


def configured_backend_aliases():
  return configured_dj_queue_backend_aliases(getattr(settings, "TASKS", {}))


def backend_choices():
  return [
    BackendChoice(alias=alias, database_alias=load_backend_config(alias).database_alias)
    for alias in configured_backend_aliases()
  ]


def backend_snapshot(
  *, backend_alias, now=None, semaphore_rows=None, include_postgres_diagnostics=False
):
  config = load_backend_config(backend_alias)
  queue_database_alias = get_database_alias(backend_alias)
  if now is None:
    now = timezone.now()
  process_cutoff = process_cutoff_for_backend(
    backend_alias,
    now=now,
    max_age=config.process_alive_threshold,
  )
  queue_state_rows = queue_rows(
    backend_alias=backend_alias,
    now=now,
    process_cutoff=process_cutoff,
  )
  backend_process_rows = process_rows(
    backend_alias=backend_alias,
    now=now,
    process_cutoff=process_cutoff,
    scope="backend",
  )
  recurring_rows = recurring_rows_for_backend(backend_alias=backend_alias, now=now)
  if semaphore_rows is None:
    semaphore_rows = semaphore_rows_for_backend(backend_alias=backend_alias)
  runner_metrics = process_counts(backend_process_rows)
  postgres_diagnostics = None
  if include_postgres_diagnostics:
    postgres_diagnostics = postgres_diagnostics_for_backend(
      backend_alias=backend_alias,
      now=now,
      max_age=config.process_alive_threshold,
    )
  failed_metrics = failed_job_metrics(
    backend_alias=backend_alias,
    now=now,
    retention_seconds=config.clear_failed_jobs_after,
  )

  return BackendSnapshot(
    backend_alias=backend_alias,
    queue_database_alias=queue_database_alias,
    process_alive_threshold=config.process_alive_threshold,
    queue_rows=tuple(queue_state_rows),
    process_rows=tuple(backend_process_rows),
    recurring_rows=tuple(recurring_rows),
    semaphore_rows=tuple(semaphore_rows),
    runner_metrics=runner_metrics,
    failed_metrics=failed_metrics,
    postgres_diagnostics=postgres_diagnostics,
  )


def all_backend_snapshots(*, now=None, include_postgres_diagnostics=False):
  if now is None:
    now = timezone.now()
  shared_semaphore_rows = {}
  snapshots = []
  for alias in configured_backend_aliases():
    queue_database_alias = get_database_alias(alias)
    semaphore_rows = shared_semaphore_rows.get(queue_database_alias)
    if semaphore_rows is None:
      semaphore_rows = tuple(semaphore_rows_for_backend(backend_alias=alias))
      shared_semaphore_rows[queue_database_alias] = semaphore_rows
    snapshots.append(
      backend_snapshot(
        backend_alias=alias,
        now=now,
        semaphore_rows=semaphore_rows,
        include_postgres_diagnostics=include_postgres_diagnostics,
      )
    )
  return snapshots


def stats_payload(*, now=None, include_postgres_diagnostics=True):
  snapshots = all_backend_snapshots(
    now=now,
    include_postgres_diagnostics=include_postgres_diagnostics,
  )
  return {"backends": [snapshot.stats_row() for snapshot in snapshots]}


def queue_rows_for_backend(*, backend_alias, now=None):
  if now is None:
    now = timezone.now()
  return queue_rows(
    backend_alias=backend_alias,
    now=now,
    process_cutoff=process_cutoff_for_backend(backend_alias, now=now),
  )


def queue_ready_count(*, backend_alias, queue_name):
  return queue_state_summary(backend_alias=backend_alias, queue_name=queue_name).count("ready")


def failed_job_metrics(*, backend_alias, now=None, retention_seconds=None):
  if now is None:
    now = timezone.now()
  alias = get_database_alias(backend_alias)
  queryset = FailedExecution.objects.using(alias).filter(job__backend_alias=backend_alias)
  metrics = _failed_job_aggregate(queryset, now=now)
  metrics["retention_seconds"] = retention_seconds
  metrics["over_retention_count"] = 0
  metrics["oldest_over_retention_created_at"] = None
  metrics["oldest_over_retention_age_seconds"] = None
  if retention_seconds is None:
    return metrics

  cutoff = now - timedelta(seconds=retention_seconds)
  over_retention = _failed_job_aggregate(queryset.filter(created_at__lt=cutoff), now=now)
  metrics["over_retention_count"] = over_retention["count"]
  metrics["oldest_over_retention_created_at"] = over_retention["oldest_created_at"]
  metrics["oldest_over_retention_age_seconds"] = over_retention["oldest_age_seconds"]
  return metrics


def _failed_job_aggregate(queryset, *, now):
  aggregate = queryset.aggregate(count=Count("id"), oldest_created_at=Min("created_at"))
  oldest_created_at = aggregate["oldest_created_at"]
  return {
    "count": aggregate["count"],
    "oldest_created_at": oldest_created_at,
    "oldest_age_seconds": _age_seconds(now, oldest_created_at),
  }


def _age_seconds(now, timestamp):
  if timestamp is None:
    return None
  return max((now - timestamp).total_seconds(), 0.0)


def process_counts(process_rows):
  counts = {
    "live": 0,
    "stale": 0,
    "by_kind": {},
  }
  by_kind = defaultdict(lambda: {"live": 0, "stale": 0})
  for row in process_rows:
    status = "live" if row["is_live"] else "stale"
    counts[status] += 1
    by_kind[row["kind"]][status] += 1
  counts["by_kind"] = {kind: dict(values) for kind, values in by_kind.items()}
  return counts


def queue_rows(*, backend_alias, now, process_cutoff):
  alias = get_database_alias(backend_alias)
  state_summaries = queue_state_summaries_by_queue(backend_alias=backend_alias)
  queue_names = set(state_summaries)
  paused_queues = set(
    Pause.objects.using(alias)
    .filter(backend_alias=backend_alias)
    .values_list("queue_name", flat=True)
  )
  recurring_queues = set(
    RecurringTask.objects.using(alias)
    .filter(backend_alias=backend_alias)
    .values_list("queue_name", flat=True)
  )

  live_workers = list(
    _live_processes_for_backend(
      alias=alias, backend_alias=backend_alias, kind="Worker", process_cutoff=process_cutoff
    )
  )

  queue_names.update(paused_queues)
  queue_names.update(recurring_queues)

  return [
    queue_snapshot(
      backend_alias=backend_alias,
      queue_name=queue_name,
      now=now,
      process_cutoff=process_cutoff,
      state_summary=state_summaries.get(queue_name) or empty_queue_state_summary(queue_name),
      paused=queue_name in paused_queues,
      live_workers=live_workers,
    )
    for queue_name in sorted(queue_names)
  ]


def queue_snapshot(
  *,
  backend_alias,
  queue_name,
  now,
  process_cutoff,
  state_summary=None,
  paused=_NOT_PROVIDED,
  oldest_ready_at=_NOT_PROVIDED,
  oldest_scheduled_at=_NOT_PROVIDED,
  oldest_blocked_at=_NOT_PROVIDED,
  live_workers=None,
):
  alias = get_database_alias(backend_alias)
  if state_summary is None:
    state_summary = queue_state_summary(backend_alias=backend_alias, queue_name=queue_name)
  if paused is _NOT_PROVIDED:
    paused = queue_is_paused(backend_alias=backend_alias, queue_name=queue_name)
  if oldest_ready_at is _NOT_PROVIDED:
    oldest_ready_at = state_summary.oldest_ready_at
  if oldest_scheduled_at is _NOT_PROVIDED:
    oldest_scheduled_at = state_summary.oldest_scheduled_at
  if oldest_blocked_at is _NOT_PROVIDED:
    oldest_blocked_at = state_summary.oldest_blocked_at
  if live_workers is None:
    live_workers = list(
      _live_processes_for_backend(
        alias=alias,
        backend_alias=backend_alias,
        kind="Worker",
        process_cutoff=process_cutoff,
      )
    )

  latency_seconds = queue_latency_seconds(
    backend_alias=backend_alias,
    queue_name=queue_name,
    now=now,
    paused=paused,
    oldest_ready_at=oldest_ready_at,
  )

  state_count_fields = state_summary.count_fields()

  return {
    "name": queue_name,
    **state_count_fields,
    "paused": paused,
    "latency_seconds": latency_seconds,
    "oldest_scheduled_at": oldest_scheduled_at,
    "oldest_blocked_at": oldest_blocked_at,
    "live_worker_count": sum(
      1 for worker in live_workers if _worker_matches_queue(queue_name, worker)
    ),
  }


def queue_is_paused(*, backend_alias, queue_name):
  alias = get_database_alias(backend_alias)
  return (
    Pause.objects.using(alias)
    .filter(
      backend_alias=backend_alias,
      queue_name=queue_name,
    )
    .exists()
  )


def queue_latency_seconds(
  *, backend_alias, queue_name, now=None, paused=None, oldest_ready_at=_NOT_PROVIDED
):
  if now is None:
    now = timezone.now()
  if paused is None:
    paused = queue_is_paused(backend_alias=backend_alias, queue_name=queue_name)
  if paused:
    return None
  if oldest_ready_at is _NOT_PROVIDED:
    oldest_ready_at = oldest_ready_at_for_queue(
      backend_alias=backend_alias,
      queue_name=queue_name,
    )
  if oldest_ready_at is None:
    return None
  return max((now - oldest_ready_at).total_seconds(), 0.0)


def oldest_ready_at_for_queue(*, backend_alias, queue_name):
  return queue_state_summary(backend_alias=backend_alias, queue_name=queue_name).oldest_ready_at


def process_rows(*, backend_alias, now, process_cutoff, scope):
  alias = get_database_alias(backend_alias)
  queryset = Process.objects.using(alias).select_related("supervisor")
  if scope == "backend":
    queryset = queryset.filter(backend_alias=backend_alias)
  elif scope != "database":
    raise ValueError(f"unknown process scope {scope!r}")
  processes = list(queryset.order_by("name"))
  children = defaultdict(list)
  roots = []

  for process in processes:
    row = process_row(process, now=now, process_cutoff=process_cutoff)
    if process.supervisor_id is not None:
      children[process.supervisor_id].append(row)
      continue
    roots.append(row)

  rows = []
  grouped_roots = sorted(
    roots,
    key=lambda row: (
      0 if row["is_live"] else 1,
      0 if row["kind"] == "Supervisor" else 1,
      row["name"],
    ),
  )
  for root in grouped_roots:
    root["is_group_head"] = bool(children.get(root["id"]))
    root["is_child"] = False
    rows.append(root)
    for child in sorted(
      children.get(root["id"], []),
      key=lambda row: (0 if row["is_live"] else 1, _process_kind_order(row["kind"]), row["name"]),
    ):
      child["is_group_head"] = False
      child["is_child"] = True
      child["group_parent_name"] = root["name"]
      rows.append(child)
  return rows


def process_cutoff_for_backend(backend_alias, *, now=None, max_age=None):
  if now is None:
    now = timezone.now()
  if max_age is None:
    max_age = load_backend_config(backend_alias).process_alive_threshold
  return now - timedelta(seconds=max_age)


def process_live_rank_expression(process_cutoff):
  return Case(
    When(last_heartbeat_at__gte=process_cutoff, then=Value(0)),
    default=Value(1),
    output_field=IntegerField(),
  )


def filter_process_status(queryset, status, *, process_cutoff):
  if status == "live":
    return queryset.filter(last_heartbeat_at__gte=process_cutoff)
  if status == "stale":
    return queryset.filter(last_heartbeat_at__lt=process_cutoff)
  return queryset


def has_live_processes(*, backend_alias, max_age=None, now=None):
  alias = get_database_alias(backend_alias)
  queryset = Process.objects.using(alias).filter(backend_alias=backend_alias)
  return filter_process_status(
    queryset,
    "live",
    process_cutoff=process_cutoff_for_backend(backend_alias, now=now, max_age=max_age),
  ).exists()


def deep_health_problems(*, backend_alias, max_age=None, now=None):
  if now is None:
    now = timezone.now()
  alias = get_database_alias(backend_alias)
  process_cutoff = process_cutoff_for_backend(backend_alias, now=now, max_age=max_age)
  problems = []

  invalid_jobs = (
    Job.objects.using(alias).filter(backend_alias=backend_alias).invalid_execution_state().count()
  )
  if invalid_jobs:
    problems.append(f"{invalid_jobs} jobs have invalid execution state")

  for label, model in _backend_owned_state_models():
    mismatched = _state_backend_mismatch_count(model, alias=alias, backend_alias=backend_alias)
    if mismatched:
      problems.append(f"{mismatched} {label} execution rows have mismatched backend ownership")

  bad_claims = (
    ClaimedExecution.objects.using(alias)
    .filter(job__backend_alias=backend_alias)
    .filter(
      Q(process__isnull=True)
      | Q(process__backend_alias__isnull=True)
      | ~Q(process__backend_alias=backend_alias)
      | Q(process__last_heartbeat_at__lt=process_cutoff)
    )
    .count()
  )
  if bad_claims:
    problems.append(f"{bad_claims} claimed execution rows have missing or stale processes")

  recurring_without_jobs = (
    RecurringExecution.objects.using(alias)
    .filter(backend_alias=backend_alias, job__isnull=True)
    .count()
  )
  if recurring_without_jobs:
    problems.append(f"{recurring_without_jobs} recurring execution reservations have no job")

  recurring_mismatched = (
    RecurringExecution.objects.using(alias)
    .filter(
      Q(backend_alias=backend_alias) | Q(job__backend_alias=backend_alias), job__isnull=False
    )
    .exclude(backend_alias=F("job__backend_alias"))
    .count()
  )
  if recurring_mismatched:
    problems.append(
      f"{recurring_mismatched} recurring execution rows have mismatched backend ownership"
    )

  bad_semaphores = (
    Semaphore.objects.using(alias)
    .filter(Q(limit__lt=1) | Q(value__lt=0) | Q(value__gt=F("limit")))
    .count()
  )
  if bad_semaphores:
    problems.append(f"{bad_semaphores} semaphores have impossible slot counts")

  failed_metrics = failed_job_metrics(
    backend_alias=backend_alias,
    now=now,
    retention_seconds=load_backend_config(backend_alias).clear_failed_jobs_after,
  )
  if failed_metrics["over_retention_count"]:
    problems.append(
      f"{failed_metrics['over_retention_count']} failed execution rows exceed "
      f"configured retention of {failed_metrics['retention_seconds']} seconds"
    )

  problems.extend(postgres_health_problems(backend_alias=backend_alias, max_age=max_age, now=now))

  return tuple(problems)


def postgres_diagnostics_for_backend(*, backend_alias, max_age=None, now=None):
  alias = get_database_alias(backend_alias)
  if database_capabilities(alias).backend_family != "postgresql":
    return None
  if now is None:
    now = timezone.now()
  if max_age is None:
    max_age = load_backend_config(backend_alias).process_alive_threshold

  try:
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


def postgres_health_problems(*, backend_alias, max_age=None, now=None):
  diagnostics = postgres_diagnostics_for_backend(
    backend_alias=backend_alias,
    max_age=max_age,
    now=now,
  )
  if not diagnostics or diagnostics.get("error"):
    return ()

  bloated_tables = [
    row
    for row in diagnostics["queue_tables"]
    if row["dead_tuples"] >= POSTGRES_DEAD_TUPLE_WARNING_COUNT
    and row["dead_tuple_ratio"] >= POSTGRES_DEAD_TUPLE_WARNING_RATIO
  ]
  if not bloated_tables:
    return ()

  table_names = ", ".join(row["table_name"] for row in bloated_tables[:5])
  problems = [
    f"{len(bloated_tables)} PostgreSQL queue tables have high dead tuples: {table_names}"
  ]
  xmin_blockers = postgres_xmin_blocker_rows(diagnostics)
  if xmin_blockers:
    problems.append(f"{len(xmin_blockers)} PostgreSQL sessions or slots may be pinning xmin")
  return tuple(problems)


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


def _backend_owned_state_models():
  return (
    ("ready", ReadyExecution),
    ("scheduled", ScheduledExecution),
    ("blocked", BlockedExecution),
  )


def _state_backend_mismatch_count(model, *, alias, backend_alias):
  return (
    model.objects.using(alias)
    .filter(Q(backend_alias=backend_alias) | Q(job__backend_alias=backend_alias))
    .exclude(backend_alias=F("job__backend_alias"))
    .count()
  )


def process_row(process, *, now, process_cutoff):
  age_seconds = max((now - process.last_heartbeat_at).total_seconds(), 0.0)
  metadata = process.metadata if isinstance(process.metadata, dict) else {}
  shutdown_started_at = metadata.get("shutdown_started_at")
  return {
    "id": process.id,
    "name": process.name,
    "backend_alias": process.backend_alias,
    "kind": process.kind,
    "pid": process.pid,
    "hostname": process.hostname,
    "metadata_json": json.dumps(process.metadata, sort_keys=True),
    "last_heartbeat_at": process.last_heartbeat_at,
    "heartbeat_age_seconds": age_seconds,
    "is_live": process.last_heartbeat_at >= process_cutoff,
    "supervisor_name": process.supervisor.name if process.supervisor_id else None,
    "shutdown_state": metadata.get("shutdown_state"),
    "shutdown_started_at": shutdown_started_at,
    "shutdown_age_seconds": _metadata_age_seconds(now, shutdown_started_at),
    "shutdown_timeout": metadata.get("shutdown_timeout"),
    "active_jobs": metadata.get("active_jobs"),
  }


def _metadata_age_seconds(now, value):
  if not isinstance(value, str):
    return None
  parsed = parse_datetime(value)
  if parsed is None:
    return None
  if timezone.is_naive(parsed):
    parsed = timezone.make_aware(parsed, timezone.get_current_timezone())
  return max((now - parsed).total_seconds(), 0.0)


def recurring_rows_for_backend(*, backend_alias, now):
  alias = get_database_alias(backend_alias)
  last_runs = dict(
    RecurringExecution.objects.using(alias)
    .filter(backend_alias=backend_alias)
    .values_list("task_key")
    .annotate(last_run_at=Max("run_at"))
  )
  return [
    {
      "key": task.key,
      "task_path": task.task_path,
      "queue_name": task.queue_name,
      "schedule": task.schedule,
      "static": task.static,
      "last_run_at": last_runs.get(task.key),
      "next_run_at": task.next_run_at or next_run_at(task.schedule, now),
    }
    for task in RecurringTask.objects.using(alias)
    .filter(backend_alias=backend_alias)
    .order_by("key")
  ]


def semaphore_rows_for_backend(*, backend_alias):
  alias = get_database_alias(backend_alias)
  waiters = _counts_by_value(
    BlockedExecution.objects.using(alias),
    field_name="concurrency_key",
  )
  return [
    {
      "scope": "queue_database",
      "queue_database_alias": alias,
      "key": semaphore.key,
      "available_slots": semaphore.value,
      "limit": semaphore.limit,
      "blocked_waiters": waiters.get(semaphore.key, 0),
      "expires_at": semaphore.expires_at,
    }
    for semaphore in Semaphore.objects.using(alias).order_by("key")
  ]


def semaphore_blocked_waiter_count_expression(alias):
  blocked_waiters = (
    BlockedExecution.objects.using(alias)
    .filter(concurrency_key=OuterRef("key"))
    .values("concurrency_key")
    .annotate(total=Count("id"))
    .values("total")[:1]
  )
  return Coalesce(
    Subquery(blocked_waiters, output_field=IntegerField()),
    Value(0),
  )


def next_run_at(schedule, now):
  return next_cron_run(schedule, now)


def _live_processes_for_backend(*, alias, backend_alias, kind, process_cutoff):
  return [
    process
    for process in Process.objects.using(alias).filter(kind=kind, backend_alias=backend_alias)
    if process.last_heartbeat_at >= process_cutoff
  ]


def _worker_matches_queue(queue_name, worker):
  selectors = _worker_queue_selectors(worker)
  if selectors is None:
    return False
  return queue_matches_selectors(queue_name, selectors)


def _worker_queue_selectors(worker):
  if worker.metadata is not None and not isinstance(worker.metadata, dict):
    return None
  metadata = worker.metadata or {}
  selectors = metadata.get("queues") or ("*",)
  if isinstance(selectors, str):
    return selectors
  if not isinstance(selectors, (list, tuple)):
    return None
  if not all(isinstance(selector, str) for selector in selectors):
    return None
  return tuple(selectors)


def _counts_by_value(queryset, *, field_name):
  return {
    row[field_name]: row["count"]
    for row in queryset.values(field_name).annotate(count=Count("id"))
  }


def _process_kind_order(kind):
  order = {
    "Dispatcher": 0,
    "Scheduler": 1,
    "Worker": 2,
  }
  return order.get(kind, 99)
