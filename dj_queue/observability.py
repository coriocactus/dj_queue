from dataclasses import dataclass

from django.conf import settings
from django.utils import timezone

from dj_queue.config import configured_backend_aliases as configured_dj_queue_backend_aliases
from dj_queue.config import load_backend_config
from dj_queue.db import get_database_alias
from dj_queue.health import (
  POSTGRES_DEAD_TUPLE_WARNING_COUNT,
  POSTGRES_DEAD_TUPLE_WARNING_RATIO,
  deep_health_problems,
  postgres_health_problems,
)
from dj_queue.postgres_diagnostics import (
  POSTGRES_AUTOVACUUM_STORAGE_PARAMETERS,
  POSTGRES_AUTOVACUUM_TABLE_MODELS,
  POSTGRES_DIAGNOSTIC_TABLE_MODELS,
  postgres_autovacuum_sql,
  postgres_diagnostics_for_backend,
  postgres_prepared_transaction_rows,
  postgres_queue_table_rows,
  postgres_replication_slot_rows,
  postgres_xmin_activity_rows,
  postgres_xmin_blocker_rows,
)
from dj_queue.reads.controls import (
  next_run_at,
  recurring_rows_for_backend,
  semaphore_blocked_waiter_count_expression,
  semaphore_rows_for_backend,
)
from dj_queue.reads.jobs import failed_job_metrics
from dj_queue.reads.processes import (
  filter_process_status,
  has_live_processes,
  process_counts,
  process_cutoff_for_backend,
  process_live_rank_expression,
  process_row,
  process_rows,
)
from dj_queue.reads.queues import (
  oldest_ready_at_for_queue,
  queue_is_paused,
  queue_latency_seconds,
  queue_ready_count,
  queue_rows,
  queue_rows_for_backend,
  queue_snapshot,
)

__all__ = [
  "POSTGRES_AUTOVACUUM_STORAGE_PARAMETERS",
  "POSTGRES_AUTOVACUUM_TABLE_MODELS",
  "POSTGRES_DEAD_TUPLE_WARNING_COUNT",
  "POSTGRES_DEAD_TUPLE_WARNING_RATIO",
  "POSTGRES_DIAGNOSTIC_TABLE_MODELS",
  "BackendChoice",
  "BackendSnapshot",
  "all_backend_snapshots",
  "backend_choices",
  "backend_snapshot",
  "configured_backend_aliases",
  "deep_health_problems",
  "failed_job_metrics",
  "filter_process_status",
  "has_live_processes",
  "next_run_at",
  "oldest_ready_at_for_queue",
  "postgres_autovacuum_sql",
  "postgres_diagnostics_for_backend",
  "postgres_health_problems",
  "postgres_prepared_transaction_rows",
  "postgres_queue_table_rows",
  "postgres_replication_slot_rows",
  "postgres_xmin_activity_rows",
  "postgres_xmin_blocker_rows",
  "process_counts",
  "process_cutoff_for_backend",
  "process_live_rank_expression",
  "process_row",
  "process_rows",
  "queue_is_paused",
  "queue_latency_seconds",
  "queue_ready_count",
  "queue_rows",
  "queue_rows_for_backend",
  "queue_snapshot",
  "recurring_rows_for_backend",
  "semaphore_blocked_waiter_count_expression",
  "semaphore_rows_for_backend",
  "stats_payload",
]


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
