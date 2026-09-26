import json
from collections import defaultdict
from datetime import timedelta

from django.db.models import (
  Case,
  IntegerField,
  Value,
  When,
)
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from dj_queue.config import load_backend_config
from dj_queue.db import get_database_alias
from dj_queue.models import (
  Process,
)


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


def _live_processes_for_backend(*, alias, backend_alias, kind, process_cutoff):
  return [
    process
    for process in Process.objects.using(alias).filter(kind=kind, backend_alias=backend_alias)
    if process.last_heartbeat_at >= process_cutoff
  ]


def _process_kind_order(kind):
  order = {
    "Dispatcher": 0,
    "Scheduler": 1,
    "Worker": 2,
  }
  return order.get(kind, 99)
