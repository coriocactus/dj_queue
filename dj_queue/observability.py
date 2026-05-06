import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta

from croniter import croniter
from django.conf import settings
from django.db.models import Count, Max, Min
from django.db.models.functions import Coalesce
from django.utils import timezone

from dj_queue.config import configured_backend_aliases as configured_dj_queue_backend_aliases
from dj_queue.config import load_backend_config
from dj_queue.db import get_database_alias
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


@dataclass(frozen=True, slots=True)
class BackendChoice:
  alias: str
  database_alias: str


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

  return {
    "backend_alias": backend_alias,
    "queue_database_alias": queue_database_alias,
    "process_alive_threshold": config.process_alive_threshold,
    "queue_rows": queue_state_rows,
    "process_rows": backend_process_rows,
    "recurring_rows": recurring_rows,
    "semaphore_rows": semaphore_rows,
    "runner_metrics": runner_metrics,
  }


def all_backend_snapshots(*, now=None):
  if now is None:
    now = timezone.now()
  return [backend_snapshot(backend_alias=alias, now=now) for alias in configured_backend_aliases()]


def stats_payload(*, now=None):
  snapshots = all_backend_snapshots(now=now)
  return {
    "backends": [
      {
        "backend_alias": snapshot["backend_alias"],
        "queue_database_alias": snapshot["queue_database_alias"],
        "process_alive_threshold": snapshot["process_alive_threshold"],
        "queues": snapshot["queue_rows"],
        "runner_metrics": snapshot["runner_metrics"],
        "recurring": snapshot["recurring_rows"],
        "semaphores": snapshot["semaphore_rows"],
      }
      for snapshot in snapshots
    ]
  }


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
  queue_names = set()

  ready_counts = _counts_by_value(
    ReadyExecution.objects.using(alias).filter(job__backend_alias=backend_alias),
    field_name="queue_name",
  )
  claimed_counts = _counts_by_value(
    ClaimedExecution.objects.using(alias).filter(job__backend_alias=backend_alias),
    field_name="job__queue_name",
  )
  scheduled_counts = _counts_by_value(
    ScheduledExecution.objects.using(alias).filter(job__backend_alias=backend_alias),
    field_name="queue_name",
  )
  blocked_counts = _counts_by_value(
    BlockedExecution.objects.using(alias).filter(job__backend_alias=backend_alias),
    field_name="queue_name",
  )
  failed_counts = _counts_by_value(
    FailedExecution.objects.using(alias).filter(job__backend_alias=backend_alias),
    field_name="job__queue_name",
  )
  finished_counts = _counts_by_value(
    Job.objects.using(alias).filter(backend_alias=backend_alias, finished_at__isnull=False),
    field_name="queue_name",
  )
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

  oldest_ready = {
    row["queue_name"]: row["oldest"]
    for row in ReadyExecution.objects.using(alias)
    .filter(job__backend_alias=backend_alias)
    .values("queue_name")
    .annotate(oldest=Min(Coalesce("latency_started_at", "created_at")))
  }
  oldest_scheduled = {
    row["queue_name"]: row["oldest"]
    for row in ScheduledExecution.objects.using(alias)
    .filter(job__backend_alias=backend_alias)
    .values("queue_name")
    .annotate(oldest=Min("scheduled_at"))
  }
  oldest_blocked = {
    row["queue_name"]: row["oldest"]
    for row in BlockedExecution.objects.using(alias)
    .filter(job__backend_alias=backend_alias)
    .values("queue_name")
    .annotate(oldest=Min("expires_at"))
  }

  live_workers = list(
    _live_processes_for_backend(
      alias=alias, backend_alias=backend_alias, kind="Worker", process_cutoff=process_cutoff
    )
  )

  queue_names.update(ready_counts)
  queue_names.update(claimed_counts)
  queue_names.update(scheduled_counts)
  queue_names.update(blocked_counts)
  queue_names.update(failed_counts)
  queue_names.update(finished_counts)
  queue_names.update(paused_queues)
  queue_names.update(recurring_queues)

  return [
    queue_snapshot(
      backend_alias=backend_alias,
      queue_name=queue_name,
      now=now,
      process_cutoff=process_cutoff,
      ready_count=ready_counts.get(queue_name, 0),
      claimed_count=claimed_counts.get(queue_name, 0),
      scheduled_count=scheduled_counts.get(queue_name, 0),
      blocked_count=blocked_counts.get(queue_name, 0),
      failed_count=failed_counts.get(queue_name, 0),
      finished_count=finished_counts.get(queue_name, 0),
      paused=queue_name in paused_queues,
      recurring=queue_name in recurring_queues,
      oldest_ready_at=oldest_ready.get(queue_name),
      oldest_scheduled_at=oldest_scheduled.get(queue_name),
      oldest_blocked_at=oldest_blocked.get(queue_name),
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
  ready_count=None,
  claimed_count=None,
  scheduled_count=None,
  blocked_count=None,
  failed_count=None,
  finished_count=None,
  paused=None,
  recurring=None,
  oldest_ready_at=None,
  oldest_scheduled_at=None,
  oldest_blocked_at=None,
  live_workers=None,
):
  alias = get_database_alias(backend_alias)
  if ready_count is None:
    state_counts = queue_state_counts(backend_alias=backend_alias, queue_name=queue_name)
    ready_count = state_counts["ready"]
    claimed_count = state_counts["claimed"]
    scheduled_count = state_counts["scheduled"]
    blocked_count = state_counts["blocked"]
    failed_count = state_counts["failed"]
    finished_count = state_counts["finished"]
  if paused is None:
    paused = (
      Pause.objects.using(alias)
      .filter(
        backend_alias=backend_alias,
        queue_name=queue_name,
      )
      .exists()
    )
  if recurring is None:
    recurring = (
      RecurringTask.objects.using(alias)
      .filter(
        backend_alias=backend_alias,
        queue_name=queue_name,
      )
      .exists()
    )
  if oldest_ready_at is None:
    oldest_ready_at = (
      ReadyExecution.objects.using(alias)
      .filter(job__backend_alias=backend_alias, queue_name=queue_name)
      .aggregate(oldest=Min(Coalesce("latency_started_at", "created_at")))["oldest"]
    )
  if oldest_scheduled_at is None:
    oldest_scheduled_at = (
      ScheduledExecution.objects.using(alias)
      .filter(job__backend_alias=backend_alias, queue_name=queue_name)
      .aggregate(oldest=Min("scheduled_at"))["oldest"]
    )
  if oldest_blocked_at is None:
    oldest_blocked_at = (
      BlockedExecution.objects.using(alias)
      .filter(job__backend_alias=backend_alias, queue_name=queue_name)
      .aggregate(oldest=Min("expires_at"))["oldest"]
    )
  if live_workers is None:
    live_workers = list(
      _live_processes_for_backend(
        alias=alias,
        backend_alias=backend_alias,
        kind="Worker",
        process_cutoff=process_cutoff,
      )
    )

  latency_seconds = None
  if oldest_ready_at is not None and paused is False:
    latency_seconds = max((now - oldest_ready_at).total_seconds(), 0.0)

  return {
    "name": queue_name,
    "ready_count": ready_count,
    "claimed_count": claimed_count,
    "scheduled_count": scheduled_count,
    "blocked_count": blocked_count,
    "failed_count": failed_count,
    "finished_count": finished_count,
    "paused": paused,
    "latency_seconds": latency_seconds,
    "oldest_scheduled_at": oldest_scheduled_at,
    "oldest_blocked_at": oldest_blocked_at,
    "live_worker_count": sum(
      1
      for worker in live_workers
      if queue_matches_selectors(queue_name, worker.metadata.get("queues", []))
    ),
  }


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
      "key": semaphore.key,
      "available_slots": semaphore.value,
      "limit": semaphore.limit,
      "blocked_waiters": waiters.get(semaphore.key, 0),
      "expires_at": semaphore.expires_at,
    }
    for semaphore in Semaphore.objects.using(alias).order_by("key")
  ]


def queue_state_counts(*, backend_alias, queue_name):
  alias = get_database_alias(backend_alias)
  base_queryset = Job.objects.using(alias).filter(
    backend_alias=backend_alias,
    queue_name=queue_name,
  )
  return {
    "ready": base_queryset.filter(ready_execution__isnull=False).count(),
    "claimed": base_queryset.filter(claimed_execution__isnull=False).count(),
    "scheduled": base_queryset.filter(scheduled_execution__isnull=False).count(),
    "blocked": base_queryset.filter(blocked_execution__isnull=False).count(),
    "failed": base_queryset.filter(failed_execution__isnull=False).count(),
    "finished": base_queryset.filter(finished_at__isnull=False).count(),
  }


def next_run_at(schedule, now):
  return croniter(schedule, now).get_next(type(now))


def queue_matches_selectors(queue_name, selectors):
  normalized = tuple(selectors or ())
  if normalized in ((), ("*",)):
    return True

  for selector in normalized:
    if selector == "*":
      return True
    if selector.endswith("*") and queue_name.startswith(selector[:-1]):
      return True
    if selector == queue_name:
      return True
  return False


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
