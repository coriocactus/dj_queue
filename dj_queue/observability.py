import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta

from django.conf import settings
from django.db.models import Count, Max
from django.utils import timezone

from dj_queue.config import configured_backend_aliases as configured_dj_queue_backend_aliases
from dj_queue.config import load_backend_config
from dj_queue.cron import next_cron_run
from dj_queue.db import get_database_alias
from dj_queue.models import (
  BlockedExecution,
  Pause,
  Process,
  RecurringExecution,
  RecurringTask,
  Semaphore,
)
from dj_queue.queue_selectors import queue_matches_selectors
from dj_queue.queue_state import (
  empty_queue_state_summary,
  queue_state_summaries_by_queue,
  queue_state_summary,
)


_NOT_PROVIDED = object()


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

  def stats_row(self):
    return {
      "backend_alias": self.backend_alias,
      "queue_database_alias": self.queue_database_alias,
      "process_alive_threshold": self.process_alive_threshold,
      "queues": self.queue_rows,
      "runner_metrics": self.runner_metrics,
      "recurring": self.recurring_rows,
      "semaphores": self.semaphore_rows,
    }


def configured_backend_aliases():
  return configured_dj_queue_backend_aliases(getattr(settings, "TASKS", {}))


def backend_choices():
  return [
    BackendChoice(alias=alias, database_alias=load_backend_config(alias).database_alias)
    for alias in configured_backend_aliases()
  ]


def backend_snapshot(*, backend_alias, now=None):
  config = load_backend_config(backend_alias)
  queue_database_alias = get_database_alias(backend_alias)
  if now is None:
    now = timezone.now()
  process_cutoff = now - timedelta(seconds=config.process_alive_threshold)
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
  semaphore_rows = semaphore_rows_for_backend(backend_alias=backend_alias)
  runner_metrics = process_counts(backend_process_rows)

  return BackendSnapshot(
    backend_alias=backend_alias,
    queue_database_alias=queue_database_alias,
    process_alive_threshold=config.process_alive_threshold,
    queue_rows=tuple(queue_state_rows),
    process_rows=tuple(backend_process_rows),
    recurring_rows=tuple(recurring_rows),
    semaphore_rows=tuple(semaphore_rows),
    runner_metrics=runner_metrics,
  )


def all_backend_snapshots(*, now=None):
  if now is None:
    now = timezone.now()
  return [backend_snapshot(backend_alias=alias, now=now) for alias in configured_backend_aliases()]


def stats_payload(*, now=None):
  snapshots = all_backend_snapshots(now=now)
  return {"backends": [snapshot.stats_row() for snapshot in snapshots]}


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
      1
      for worker in live_workers
      if queue_matches_selectors(queue_name, worker.metadata.get("queues") or ("*",))
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


def process_row(process, *, now, process_cutoff):
  age_seconds = max((now - process.last_heartbeat_at).total_seconds(), 0.0)
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
  }


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


def next_run_at(schedule, now):
  return next_cron_run(schedule, now)


def _live_processes_for_backend(*, alias, backend_alias, kind, process_cutoff):
  return [
    process
    for process in Process.objects.using(alias).filter(kind=kind, backend_alias=backend_alias)
    if process.last_heartbeat_at >= process_cutoff
  ]


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
