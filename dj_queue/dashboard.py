import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta
from urllib.parse import urlencode

from croniter import croniter
from django.conf import settings
from django.core.paginator import Paginator
from django.db.models import Count, Max, Min
from django.http import Http404
from django.utils import timezone

from dj_queue.api import QueueInfo
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
from dj_queue.operations.jobs import discard_blocked_jobs, discard_ready_jobs

QUEUE_STATES = (
  ("ready", "ready"),
  ("claimed", "claimed"),
  ("scheduled", "scheduled"),
  ("blocked", "blocked"),
  ("failed", "failed"),
  ("finished", "finished"),
)

QUEUE_STATE_LABELS = dict(QUEUE_STATES)
PAGE_SIZE = 100
OVERVIEW_PAGE_SIZES = {
  "queues": 18,
  "processes": 10,
  "recurring": 12,
  "semaphores": 12,
}


@dataclass(frozen=True, slots=True)
class BackendChoice:
  alias: str
  database_alias: str
  shared_aliases: tuple[str, ...]

  @property
  def shared_database(self):
    return len(self.shared_aliases) > 1


def backend_choices():
  aliases = configured_backend_aliases()
  grouped = defaultdict(list)
  database_aliases = {}
  for alias in aliases:
    database_alias = load_backend_config(alias).database_alias
    database_aliases[alias] = database_alias
    grouped[database_alias].append(alias)

  return [
    BackendChoice(
      alias=alias,
      database_alias=database_aliases[alias],
      shared_aliases=tuple(grouped[database_aliases[alias]]),
    )
    for alias in aliases
  ]


def configured_backend_aliases():
  tasks = getattr(settings, "TASKS", {})
  aliases = tuple(tasks)
  if aliases:
    return aliases
  return ("default",)


def resolve_backend_alias(raw_backend_alias):
  aliases = configured_backend_aliases()
  backend_alias = raw_backend_alias or ("default" if "default" in aliases else aliases[0])
  if backend_alias not in aliases:
    raise Http404(f"unknown dj_queue backend {backend_alias!r}")
  return backend_alias


def shared_aliases_for_backend(backend_alias):
  for choice in backend_choices():
    if choice.alias == backend_alias:
      return choice.shared_aliases
  return (backend_alias,)


def dashboard_context(*, backend_alias, query_params=None):
  queue_database_alias = get_database_alias(backend_alias)
  config = load_backend_config(backend_alias)
  now = timezone.now()
  process_cutoff = now - timedelta(seconds=config.process_alive_threshold)
  if query_params is None:
    query_params = {}

  queue_rows = _queue_rows(backend_alias=backend_alias, now=now, process_cutoff=process_cutoff)
  process_rows = _process_rows(
    backend_alias=backend_alias,
    now=now,
    process_cutoff=process_cutoff,
  )
  recurring_rows = _recurring_rows(backend_alias=backend_alias, now=now)
  semaphore_rows = _semaphore_rows(backend_alias=backend_alias)

  return {
    "backend_alias": backend_alias,
    "backend_choices": backend_choices(),
    "config": config,
    "queue_database_alias": queue_database_alias,
    "shared_aliases": shared_aliases_for_backend(backend_alias),
    "summary_cards": _summary_cards(
      queue_rows=queue_rows,
      process_rows=process_rows,
      recurring_rows=recurring_rows,
      semaphore_rows=semaphore_rows,
    ),
    "backend_facts": _backend_facts(
      config=config,
      queue_database_alias=queue_database_alias,
      recurring_count=len(recurring_rows),
      semaphore_count=len(semaphore_rows),
    ),
    "queue_section": _overview_section(
      rows=queue_rows,
      page_param="queues_page",
      page_size=OVERVIEW_PAGE_SIZES["queues"],
      query_params=query_params,
      anchor="queue-summary",
    ),
    "process_section": _overview_section(
      rows=process_rows,
      page_param="processes_page",
      page_size=OVERVIEW_PAGE_SIZES["processes"],
      query_params=query_params,
      anchor="process-summary",
    ),
    "recurring_section": _overview_section(
      rows=recurring_rows,
      page_param="recurring_page",
      page_size=OVERVIEW_PAGE_SIZES["recurring"],
      query_params=query_params,
      anchor="recurring-summary",
    ),
    "semaphore_section": _overview_section(
      rows=semaphore_rows,
      page_param="semaphores_page",
      page_size=OVERVIEW_PAGE_SIZES["semaphores"],
      query_params=query_params,
      anchor="semaphore-summary",
    ),
  }


def queue_page_context(*, backend_alias, queue_name, state, page_number):
  if state not in QUEUE_STATE_LABELS:
    raise Http404(f"unknown queue state {state!r}")

  alias = get_database_alias(backend_alias)
  config = load_backend_config(backend_alias)
  now = timezone.now()
  process_cutoff = now - timedelta(seconds=config.process_alive_threshold)
  queryset = _jobs_for_queue_state(
    backend_alias=backend_alias,
    queue_name=queue_name,
    state=state,
  )

  paginator = Paginator(queryset, PAGE_SIZE)
  page_obj = paginator.get_page(page_number)
  queue_info = QueueInfo(queue_name, backend_alias=backend_alias)
  state_counts = _queue_state_counts(backend_alias=backend_alias, queue_name=queue_name)
  state_tabs = [
    {
      "name": state_name,
      "label": label,
      "count": state_counts[state_name],
      "selected": state_name == state,
    }
    for state_name, label in QUEUE_STATES
  ]

  return {
    "backend_alias": backend_alias,
    "backend_choices": backend_choices(),
    "config": config,
    "queue_database_alias": alias,
    "shared_aliases": shared_aliases_for_backend(backend_alias),
    "queue_name": queue_name,
    "queue_info": queue_info,
    "queue_paused": queue_info.paused,
    "state": state,
    "state_label": QUEUE_STATE_LABELS[state],
    "state_tabs": state_tabs,
    "page_obj": page_obj,
    "jobs": list(page_obj.object_list),
    "process_cutoff": process_cutoff,
  }


def apply_queue_action(*, backend_alias, queue_name, action):
  queue_info = QueueInfo(queue_name, backend_alias=backend_alias)
  if action == "pause":
    queue_info.pause()
    return f"paused queue {queue_name}"
  if action == "resume":
    queue_info.resume()
    return f"resumed queue {queue_name}"
  if action == "clear":
    deleted = queue_info.clear()
    return f"cleared {deleted} ready jobs from {queue_name}"
  raise ValueError(f"unsupported queue action {action!r}")


def apply_job_action(*, backend_alias, queue_name, state, action, job_ids):
  if not job_ids:
    raise ValueError("select at least one job")

  if state == "ready" and action == "discard":
    deleted = discard_ready_jobs(
      job_ids=job_ids,
      batch_size=max(len(job_ids), 1),
      backend_alias=backend_alias,
    )
    return f"discarded {deleted} ready jobs from {queue_name}"

  if state == "blocked" and action == "discard":
    deleted = discard_blocked_jobs(
      job_ids=job_ids,
      batch_size=max(len(job_ids), 1),
      backend_alias=backend_alias,
    )
    return f"discarded {deleted} blocked jobs from {queue_name}"

  if state == "failed" and action == "retry":
    alias = get_database_alias(backend_alias)
    queryset = FailedExecution.objects.using(alias).filter(
      job_id__in=job_ids,
      job__backend_name=backend_alias,
      job__queue_name=queue_name,
    )
    retried = FailedExecution.retry_all(queryset.select_related("job"))
    return f"retried {retried} failed jobs from {queue_name}"

  if state == "failed" and action == "discard":
    alias = get_database_alias(backend_alias)
    discarded = 0
    executions = list(
      FailedExecution.objects.using(alias)
      .select_related("job")
      .filter(
        job_id__in=job_ids,
        job__backend_name=backend_alias,
        job__queue_name=queue_name,
      )
    )
    for execution in executions:
      discarded += execution.discard()
    return f"discarded {discarded} failed jobs from {queue_name}"

  raise ValueError(f"unsupported {state!r} job action {action!r}")


def job_actions_for_state(state):
  if state == "ready":
    return ({"name": "discard", "label": "discard selected"},)
  if state == "blocked":
    return ({"name": "discard", "label": "discard selected"},)
  if state == "failed":
    return (
      {"name": "retry", "label": "retry selected"},
      {"name": "discard", "label": "discard selected"},
    )
  return ()


def _summary_cards(*, queue_rows, process_rows, recurring_rows, semaphore_rows):
  paused_count = sum(1 for row in queue_rows if row["paused"])
  ready_count = sum(row["ready_count"] for row in queue_rows)
  scheduled_count = sum(row["scheduled_count"] for row in queue_rows)
  failed_count = sum(row["failed_count"] for row in queue_rows)
  blocked_count = sum(row["blocked_count"] for row in queue_rows)
  live_processes = sum(1 for row in process_rows if row["is_live"])
  stale_processes = len(process_rows) - live_processes

  return (
    {
      "label": "queues",
      "value": len(queue_rows),
      "detail": f"{paused_count} paused",
    },
    {
      "label": "backlog",
      "value": ready_count + scheduled_count,
      "detail": f"{ready_count} ready and {scheduled_count} scheduled",
    },
    {
      "label": "attention",
      "value": failed_count + blocked_count,
      "detail": f"{failed_count} failed and {blocked_count} blocked",
    },
    {
      "label": "runtime",
      "value": live_processes,
      "detail": f"{live_processes} live, {stale_processes} stale",
    },
    {
      "label": "control plane",
      "value": len(recurring_rows) + len(semaphore_rows),
      "detail": f"{len(recurring_rows)} recurring and {len(semaphore_rows)} semaphores",
    },
  )


def _backend_facts(*, config, queue_database_alias, recurring_count, semaphore_count):
  retention = "disabled"
  if config.clear_finished_jobs_after is not None:
    retention = f"{config.clear_finished_jobs_after}s"

  return (
    {"label": "mode", "value": config.mode},
    {"label": "queue db", "value": queue_database_alias},
    {"label": "scheduler", "value": "enabled" if config.has_scheduler_work else "disabled"},
    {"label": "notify", "value": "on" if config.listen_notify else "off"},
    {"label": "skip locked", "value": "on" if config.use_skip_locked else "off"},
    {"label": "heartbeat", "value": f"{config.process_alive_threshold}s"},
    {"label": "retention", "value": retention},
    {"label": "recurring", "value": str(recurring_count)},
    {"label": "semaphores", "value": str(semaphore_count)},
  )


def _overview_section(*, rows, page_param, page_size, query_params, anchor):
  paginator = Paginator(rows, page_size)
  page_obj = paginator.get_page(query_params.get(page_param, 1))
  start_index = 0
  end_index = 0
  if paginator.count:
    start_index = (page_obj.number - 1) * page_size + 1
    end_index = start_index + len(page_obj.object_list) - 1

  previous_query = None
  if page_obj.has_previous():
    previous_query = _overview_query(
      query_params=query_params,
      page_param=page_param,
      page_number=page_obj.previous_page_number(),
    )

  next_query = None
  if page_obj.has_next():
    next_query = _overview_query(
      query_params=query_params,
      page_param=page_param,
      page_number=page_obj.next_page_number(),
    )

  return {
    "rows": list(page_obj.object_list),
    "page_obj": page_obj,
    "total_count": paginator.count,
    "start_index": start_index,
    "end_index": end_index,
    "previous_query": previous_query,
    "next_query": next_query,
    "anchor": anchor,
  }


def _overview_query(*, query_params, page_param, page_number):
  params = query_params.copy()
  params[page_param] = page_number
  if hasattr(params, "urlencode"):
    return params.urlencode()
  return urlencode(params, doseq=True)


def _queue_rows(*, backend_alias, now, process_cutoff):
  alias = get_database_alias(backend_alias)
  queue_names = set()

  ready_counts = _counts_by_value(
    ReadyExecution.objects.using(alias).filter(job__backend_name=backend_alias),
    field_name="queue_name",
  )
  claimed_counts = _counts_by_value(
    ClaimedExecution.objects.using(alias).filter(job__backend_name=backend_alias),
    field_name="job__queue_name",
  )
  scheduled_counts = _counts_by_value(
    ScheduledExecution.objects.using(alias).filter(job__backend_name=backend_alias),
    field_name="queue_name",
  )
  blocked_counts = _counts_by_value(
    BlockedExecution.objects.using(alias).filter(job__backend_name=backend_alias),
    field_name="queue_name",
  )
  failed_counts = _counts_by_value(
    FailedExecution.objects.using(alias).filter(job__backend_name=backend_alias),
    field_name="job__queue_name",
  )
  finished_counts = _counts_by_value(
    Job.objects.using(alias).filter(backend_name=backend_alias, finished_at__isnull=False),
    field_name="queue_name",
  )
  paused_queues = set(Pause.objects.using(alias).values_list("queue_name", flat=True))
  recurring_queues = set(RecurringTask.objects.using(alias).values_list("queue_name", flat=True))

  oldest_ready = {
    row["queue_name"]: row["oldest"]
    for row in ReadyExecution.objects.using(alias)
    .filter(job__backend_name=backend_alias)
    .values("queue_name")
    .annotate(oldest=Min("job__created_at"))
  }
  oldest_scheduled = {
    row["queue_name"]: row["oldest"]
    for row in ScheduledExecution.objects.using(alias)
    .filter(job__backend_name=backend_alias)
    .values("queue_name")
    .annotate(oldest=Min("scheduled_at"))
  }
  oldest_blocked = {
    row["queue_name"]: row["oldest"]
    for row in BlockedExecution.objects.using(alias)
    .filter(job__backend_name=backend_alias)
    .values("queue_name")
    .annotate(oldest=Min("expires_at"))
  }

  live_workers = [
    process
    for process in Process.objects.using(alias).filter(kind="Worker")
    if process.last_heartbeat_at >= process_cutoff
  ]

  queue_names.update(ready_counts)
  queue_names.update(claimed_counts)
  queue_names.update(scheduled_counts)
  queue_names.update(blocked_counts)
  queue_names.update(failed_counts)
  queue_names.update(finished_counts)
  queue_names.update(paused_queues)
  queue_names.update(recurring_queues)

  rows = []
  for queue_name in sorted(queue_names):
    oldest_ready_at = oldest_ready.get(queue_name)
    latency_seconds = None
    if oldest_ready_at is not None:
      latency_seconds = max((now - oldest_ready_at).total_seconds(), 0.0)

    rows.append(
      {
        "name": queue_name,
        "ready_count": ready_counts.get(queue_name, 0),
        "claimed_count": claimed_counts.get(queue_name, 0),
        "scheduled_count": scheduled_counts.get(queue_name, 0),
        "blocked_count": blocked_counts.get(queue_name, 0),
        "failed_count": failed_counts.get(queue_name, 0),
        "finished_count": finished_counts.get(queue_name, 0),
        "paused": queue_name in paused_queues,
        "latency_seconds": latency_seconds,
        "oldest_scheduled_at": oldest_scheduled.get(queue_name),
        "oldest_blocked_at": oldest_blocked.get(queue_name),
        "live_worker_count": sum(
          1
          for worker in live_workers
          if _queue_matches_selectors(queue_name, worker.metadata.get("queues", []))
        ),
      }
    )
  return rows


def _process_rows(*, backend_alias, now, process_cutoff):
  alias = get_database_alias(backend_alias)
  processes = list(Process.objects.using(alias).select_related("supervisor").order_by("name"))
  children = defaultdict(list)
  roots = []

  for process in processes:
    row = _process_row(process, now=now, process_cutoff=process_cutoff)
    if process.supervisor_id is not None:
      children[process.supervisor_id].append(row)
      continue
    roots.append(row)

  rows = []
  grouped_roots = sorted(
    roots,
    key=lambda row: (
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
      key=lambda row: (_process_kind_order(row["kind"]), row["name"]),
    ):
      child["is_group_head"] = False
      child["is_child"] = True
      child["group_parent_name"] = root["name"]
      rows.append(child)
  return rows


def _process_row(process, *, now, process_cutoff):
  age_seconds = max((now - process.last_heartbeat_at).total_seconds(), 0.0)
  return {
    "id": process.id,
    "name": process.name,
    "kind": process.kind,
    "pid": process.pid,
    "hostname": process.hostname,
    "metadata_json": json.dumps(process.metadata, sort_keys=True),
    "last_heartbeat_at": process.last_heartbeat_at,
    "heartbeat_age_seconds": age_seconds,
    "is_live": process.last_heartbeat_at >= process_cutoff,
    "supervisor_name": process.supervisor.name if process.supervisor_id else None,
  }


def _process_kind_order(kind):
  order = {
    "Dispatcher": 0,
    "Scheduler": 1,
    "Worker": 2,
  }
  return order.get(kind, 99)


def _recurring_rows(*, backend_alias, now):
  alias = get_database_alias(backend_alias)
  last_runs = dict(
    RecurringExecution.objects.using(alias)
    .values_list("task_key")
    .annotate(last_run_at=Max("run_at"))
  )

  rows = []
  for task in RecurringTask.objects.using(alias).order_by("key"):
    rows.append(
      {
        "key": task.key,
        "task_path": task.task_path,
        "queue_name": task.queue_name,
        "schedule": task.schedule,
        "static": task.static,
        "last_run_at": last_runs.get(task.key),
        "next_run_at": _next_run_at(task.schedule, now),
      }
    )
  return rows


def _semaphore_rows(*, backend_alias):
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


def _queue_state_counts(*, backend_alias, queue_name):
  alias = get_database_alias(backend_alias)
  base_queryset = Job.objects.using(alias).filter(
    backend_name=backend_alias, queue_name=queue_name
  )
  return {
    "ready": base_queryset.filter(ready_execution__isnull=False).count(),
    "claimed": base_queryset.filter(claimed_execution__isnull=False).count(),
    "scheduled": base_queryset.filter(scheduled_execution__isnull=False).count(),
    "blocked": base_queryset.filter(blocked_execution__isnull=False).count(),
    "failed": base_queryset.filter(failed_execution__isnull=False).count(),
    "finished": base_queryset.filter(finished_at__isnull=False).count(),
  }


def _jobs_for_queue_state(*, backend_alias, queue_name, state):
  alias = get_database_alias(backend_alias)
  queryset = (
    Job.objects.using(alias)
    .filter(backend_name=backend_alias, queue_name=queue_name)
    .select_related(
      "ready_execution",
      "scheduled_execution",
      "claimed_execution__process",
      "blocked_execution",
      "failed_execution",
    )
  )
  if state == "ready":
    return queryset.filter(ready_execution__isnull=False).order_by(
      "-priority", "ready_execution__id"
    )
  if state == "claimed":
    return queryset.filter(claimed_execution__isnull=False).order_by(
      "claimed_execution__created_at", "id"
    )
  if state == "scheduled":
    return queryset.filter(scheduled_execution__isnull=False).order_by(
      "scheduled_execution__scheduled_at",
      "-priority",
      "scheduled_execution__id",
    )
  if state == "blocked":
    return queryset.filter(blocked_execution__isnull=False).order_by(
      "blocked_execution__expires_at",
      "-priority",
      "blocked_execution__id",
    )
  if state == "failed":
    return queryset.filter(failed_execution__isnull=False).order_by(
      "-failed_execution__created_at", "id"
    )
  return queryset.filter(finished_at__isnull=False).order_by("-finished_at", "id")


def _counts_by_value(queryset, *, field_name):
  return {
    row[field_name]: row["count"]
    for row in queryset.values(field_name).annotate(count=Count("id"))
  }


def _next_run_at(schedule, now):
  return croniter(schedule, now).get_next(type(now))


def _queue_matches_selectors(queue_name, selectors):
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
