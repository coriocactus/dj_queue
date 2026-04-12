import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta
from urllib.parse import urlencode
from uuid import UUID

from croniter import croniter
from django.conf import settings
from django.core.paginator import Paginator
from django.db.models import Count, Max, Min
from django.http import Http404
from django.urls import reverse
from django.utils import timezone

from dj_queue.api import QueueInfo
from dj_queue.config import load_backend_config
from dj_queue.db import database_capabilities, get_database_alias
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
from dj_queue.operations.jobs import (
  discard_blocked_jobs,
  discard_ready_jobs,
  discard_scheduled_jobs,
  enqueue_job_again,
)

QUEUE_STATE_CONFIG = {
  "ready": {
    "label": "ready",
    "job_actions": ({"name": "discard", "label": "discard selected"},),
    "query_filter": {"ready_execution__isnull": False},
    "query_order": ("-priority", "ready_execution__id"),
  },
  "claimed": {
    "label": "claimed",
    "job_actions": (),
    "query_filter": {"claimed_execution__isnull": False},
    "query_order": ("claimed_execution__created_at", "id"),
  },
  "scheduled": {
    "label": "scheduled",
    "job_actions": ({"name": "discard", "label": "discard selected"},),
    "query_filter": {"scheduled_execution__isnull": False},
    "query_order": ("scheduled_execution__scheduled_at", "-priority", "scheduled_execution__id"),
  },
  "blocked": {
    "label": "blocked",
    "job_actions": ({"name": "discard", "label": "discard selected"},),
    "query_filter": {"blocked_execution__isnull": False},
    "query_order": ("blocked_execution__expires_at", "-priority", "blocked_execution__id"),
  },
  "failed": {
    "label": "failed",
    "job_actions": (
      {"name": "retry", "label": "retry selected"},
      {"name": "discard", "label": "discard selected"},
    ),
    "query_filter": {"failed_execution__isnull": False},
    "query_order": ("-failed_execution__created_at", "id"),
  },
  "finished": {
    "label": "finished",
    "job_actions": ({"name": "enqueue", "label": "enqueue selected again"},),
    "query_filter": {"finished_at__isnull": False},
    "query_order": ("-finished_at", "id"),
  },
}

QUEUE_STATES = tuple((state, config["label"]) for state, config in QUEUE_STATE_CONFIG.items())
QUEUE_STATE_LABELS = {state: config["label"] for state, config in QUEUE_STATE_CONFIG.items()}
PAGE_SIZE = 100
OVERVIEW_PAGE_SIZES = {
  "queues": 18,
  "shared_queues": 5,
  "processes": 10,
  "recurring": 12,
  "semaphores": 12,
}
OVERVIEW_COUNT_LABELS = {
  "queues": ("queue", "queues"),
  "shared_queues": ("shared queue", "shared queues"),
  "processes": ("process", "processes"),
  "recurring": ("recurring task", "recurring tasks"),
  "semaphores": ("semaphore", "semaphores"),
}
OVERVIEW_SORTS = {
  "queues": {
    "default": "name",
    "fields": {
      "name": {"label": "name", "key": "name", "default_desc": False, "css_class": "djq-col-name"},
      "ready": {"label": "ready", "key": "ready_count", "default_desc": True},
      "claimed": {"label": "claimed", "key": "claimed_count", "default_desc": True},
      "scheduled": {"label": "scheduled", "key": "scheduled_count", "default_desc": True},
      "blocked": {"label": "blocked", "key": "blocked_count", "default_desc": True},
      "failed": {"label": "failed", "key": "failed_count", "default_desc": True},
      "finished": {"label": "finished", "key": "finished_count", "default_desc": True},
      "paused": {"label": "paused", "key": "paused", "default_desc": True},
      "latency": {"label": "latency", "key": "latency_seconds", "default_desc": True},
      "workers": {"label": "workers", "key": "live_worker_count", "default_desc": True},
      "oldest_scheduled": {
        "label": "oldest scheduled",
        "key": "oldest_scheduled_at",
        "default_desc": True,
      },
      "blocked_until": {
        "label": "blocked until",
        "key": "oldest_blocked_at",
        "default_desc": True,
      },
    },
  },
  "shared_queues": {
    "default": "name",
    "fields": {
      "name": {"label": "name", "key": "name", "default_desc": False, "css_class": "djq-col-name"},
      "shared_via": {
        "label": "shared via",
        "key": "shared_source_labels",
        "default_desc": False,
        "css_class": "djq-col-shared-via",
      },
      "paused": {"label": "paused", "key": "paused", "default_desc": True},
    },
  },
  "processes": {
    "default": "status",
    "fields": {
      "name": {"label": "name", "key": "name", "default_desc": False, "css_class": "djq-col-name"},
      "kind": {"label": "kind", "key": "kind", "default_desc": False},
      "status": {"label": "status", "key": "is_live", "default_desc": True},
      "heartbeat": {
        "label": "heartbeat",
        "key": "last_heartbeat_at",
        "default_desc": True,
      },
      "hostname": {"label": "hostname", "key": "hostname", "default_desc": False},
      "pid": {"label": "pid", "key": "pid", "default_desc": True},
      "metadata": {
        "label": "metadata",
        "key": "metadata_json",
        "default_desc": False,
        "css_class": "djq-col-metadata",
      },
    },
  },
  "recurring": {
    "default": "key",
    "fields": {
      "key": {"label": "key", "key": "key", "default_desc": False, "css_class": "djq-col-name"},
      "task": {"label": "task", "key": "task_path", "default_desc": False},
      "queue": {"label": "queue", "key": "queue_name", "default_desc": False},
      "schedule": {"label": "schedule", "key": "schedule", "default_desc": False},
      "type": {"label": "type", "key": "static", "default_desc": True},
      "last_run": {"label": "last run", "key": "last_run_at", "default_desc": True},
      "next_run": {"label": "next run", "key": "next_run_at", "default_desc": False},
    },
  },
  "semaphores": {
    "default": "key",
    "fields": {
      "key": {"label": "key", "key": "key", "default_desc": False, "css_class": "djq-col-name"},
      "available": {"label": "available", "key": "available_slots", "default_desc": True},
      "limit": {"label": "limit", "key": "limit", "default_desc": True},
      "blocked_waiters": {
        "label": "blocked waiters",
        "key": "blocked_waiters",
        "default_desc": True,
      },
      "expires_at": {"label": "expires at", "key": "expires_at", "default_desc": True},
    },
  },
}
QUEUE_PAGE_SORTS = {
  "ready": {
    "fields": {
      "id": {"label": "id", "key": "id", "default_desc": False},
      "task": {"label": "task", "key": "task_path", "default_desc": False},
      "priority": {"label": "priority", "key": "priority", "default_desc": True},
      "created": {"label": "created", "key": "created_at", "default_desc": True},
    }
  },
  "claimed": {
    "fields": {
      "id": {"label": "id", "key": "id", "default_desc": False},
      "task": {"label": "task", "key": "task_path", "default_desc": False},
      "priority": {"label": "priority", "key": "priority", "default_desc": True},
      "created": {"label": "created", "key": "created_at", "default_desc": True},
      "process": {
        "label": "process",
        "key": "claimed_execution__process__name",
        "default_desc": False,
      },
      "started": {
        "label": "started",
        "key": "claimed_execution__created_at",
        "default_desc": False,
      },
    }
  },
  "scheduled": {
    "fields": {
      "id": {"label": "id", "key": "id", "default_desc": False},
      "task": {"label": "task", "key": "task_path", "default_desc": False},
      "priority": {"label": "priority", "key": "priority", "default_desc": True},
      "created": {"label": "created", "key": "created_at", "default_desc": True},
      "scheduled_at": {
        "label": "scheduled at",
        "key": "scheduled_execution__scheduled_at",
        "default_desc": False,
      },
    }
  },
  "blocked": {
    "fields": {
      "id": {"label": "id", "key": "id", "default_desc": False},
      "task": {"label": "task", "key": "task_path", "default_desc": False},
      "priority": {"label": "priority", "key": "priority", "default_desc": True},
      "created": {"label": "created", "key": "created_at", "default_desc": True},
      "concurrency_key": {
        "label": "concurrency key",
        "key": "blocked_execution__concurrency_key",
        "default_desc": False,
      },
      "expires_at": {
        "label": "expires at",
        "key": "blocked_execution__expires_at",
        "default_desc": False,
      },
    }
  },
  "failed": {
    "fields": {
      "id": {"label": "id", "key": "id", "default_desc": False},
      "task": {"label": "task", "key": "task_path", "default_desc": False},
      "priority": {"label": "priority", "key": "priority", "default_desc": True},
      "created": {"label": "created", "key": "created_at", "default_desc": True},
      "exception": {
        "label": "exception",
        "key": "failed_execution__exception_class",
        "default_desc": False,
      },
      "message": {
        "label": "message",
        "key": "failed_execution__message",
        "default_desc": False,
      },
    }
  },
  "finished": {
    "fields": {
      "id": {"label": "id", "key": "id", "default_desc": False},
      "task": {"label": "task", "key": "task_path", "default_desc": False},
      "priority": {"label": "priority", "key": "priority", "default_desc": True},
      "created": {"label": "created", "key": "created_at", "default_desc": True},
      "finished_at": {
        "label": "finished at",
        "key": "finished_at",
        "default_desc": True,
      },
      "return_value": {
        "label": "return value",
        "key": "return_value",
        "default_desc": False,
      },
    }
  },
}


@dataclass(frozen=True, slots=True)
class BackendChoice:
  alias: str
  database_alias: str


def backend_choices():
  aliases = configured_backend_aliases()
  database_aliases = {}
  for alias in aliases:
    database_alias = load_backend_config(alias).database_alias
    database_aliases[alias] = database_alias

  return [
    BackendChoice(
      alias=alias,
      database_alias=database_aliases[alias],
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


def dashboard_context(*, backend_alias, query_params=None):
  queue_database_alias = get_database_alias(backend_alias)
  config = load_backend_config(backend_alias)
  now = timezone.now()
  process_cutoff = now - timedelta(seconds=config.process_alive_threshold)
  if query_params is None:
    query_params = {}

  queue_rows = _queue_rows(backend_alias=backend_alias, now=now, process_cutoff=process_cutoff)
  backend_queue_rows, shared_queue_rows = _split_queue_rows(queue_rows)
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
    "summary_cards": _summary_cards(
      backend_alias=backend_alias,
      queue_rows=backend_queue_rows,
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
      section="queues",
      rows=backend_queue_rows,
      page_param="queues_page",
      page_size=OVERVIEW_PAGE_SIZES["queues"],
      sort_param="queues_sort",
      query_params=query_params,
      anchor="queue-summary",
    ),
    "shared_queue_section": _overview_section(
      section="shared_queues",
      rows=shared_queue_rows,
      page_param="shared_queues_page",
      page_size=OVERVIEW_PAGE_SIZES["shared_queues"],
      sort_param="shared_queues_sort",
      query_params=query_params,
      anchor="shared-queue-summary",
    ),
    "process_section": _overview_section(
      section="processes",
      rows=process_rows,
      page_param="processes_page",
      page_size=OVERVIEW_PAGE_SIZES["processes"],
      sort_param="processes_sort",
      query_params=query_params,
      anchor="process-summary",
    ),
    "recurring_section": _overview_section(
      section="recurring",
      rows=recurring_rows,
      page_param="recurring_page",
      page_size=OVERVIEW_PAGE_SIZES["recurring"],
      sort_param="recurring_sort",
      query_params=query_params,
      anchor="recurring-summary",
    ),
    "semaphore_section": _overview_section(
      section="semaphores",
      rows=semaphore_rows,
      page_param="semaphores_page",
      page_size=OVERVIEW_PAGE_SIZES["semaphores"],
      sort_param="semaphores_sort",
      query_params=query_params,
      anchor="semaphore-summary",
    ),
  }


def queue_page_context(*, backend_alias, queue_name, state, page_number, query_params=None):
  if state not in QUEUE_STATE_LABELS:
    raise Http404(f"unknown queue state {state!r}")
  if query_params is None:
    query_params = {}

  alias = get_database_alias(backend_alias)
  config = load_backend_config(backend_alias)
  now = timezone.now()
  process_cutoff = now - timedelta(seconds=config.process_alive_threshold)
  queryset = _jobs_for_queue_state(
    backend_alias=backend_alias,
    queue_name=queue_name,
    state=state,
  )
  sort, explicit_sort = _resolve_queue_sort(state=state, raw_sort=query_params.get("sort"))
  jobs = list(queryset)
  if explicit_sort:
    jobs = _sort_queue_jobs(jobs=jobs, state=state, sort=sort)

  paginator = Paginator(jobs, PAGE_SIZE)
  page_obj = paginator.get_page(query_params.get("page", page_number))
  queue_row = _queue_snapshot(
    backend_alias=backend_alias,
    queue_name=queue_name,
    now=now,
    process_cutoff=process_cutoff,
  )
  queue_info = QueueInfo(queue_name, backend_alias=backend_alias)
  state_counts = {
    state_name: queue_row[f"{state_name}_count"] for state_name, _label in QUEUE_STATES
  }
  state_tabs = [
    {
      "name": state_name,
      "label": label,
      "count": state_counts[state_name],
      "selected": state_name == state,
    }
    for state_name, label in QUEUE_STATES
  ]
  raw_links = []
  if sum(state_counts.values()):
    raw_links.append(
      {
        "label": "Raw jobs",
        "url": _job_changelist_url(
          backend_alias,
          queue_name=queue_name,
          status=state,
        ),
      }
    )
  if state_counts["failed"]:
    raw_links.append(
      {
        "label": "Failed executions",
        "url": _failed_execution_changelist_url(
          backend_alias,
          job__queue_name=queue_name,
        ),
      }
    )

  return {
    "backend_alias": backend_alias,
    "backend_choices": backend_choices(),
    "config": config,
    "queue_database_alias": alias,
    "queue_name": queue_name,
    "queue_info": queue_info,
    "queue_paused": queue_row["paused"],
    "queue_latency_seconds": queue_row["latency_seconds"],
    "queue_worker_count": queue_row["live_worker_count"],
    "state": state,
    "state_label": QUEUE_STATE_LABELS[state],
    "state_tabs": state_tabs,
    "table_headers": _queue_page_headers(
      state=state,
      query_params=query_params,
      sort=sort,
      explicit_sort=explicit_sort,
      page_param="page",
      anchor="result_list",
    ),
    "queue_num_sorted_fields": len(_parse_sort_fields(sort)) if explicit_sort else 0,
    "raw_links": tuple(raw_links),
    "page_obj": page_obj,
    "jobs": list(page_obj.object_list),
    "page_links": (
      _page_links_for_total_pages(
        total_pages=paginator.num_pages,
        current_page=page_obj.number,
        query_params=query_params,
        page_param="page",
        sort_param="sort",
        sort=sort if explicit_sort else None,
        anchor="result_list",
      )
      if paginator.num_pages > 1
      else ()
    ),
    "result_count_text": _queue_result_count_text(page_obj=page_obj, total_count=paginator.count),
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
  if not action:
    raise ValueError("No action selected.")

  if not job_ids:
    raise ValueError("select at least one job")

  if state == "ready" and action == "discard":
    deleted = discard_ready_jobs(
      job_ids=job_ids,
      batch_size=max(len(job_ids), 1),
      backend_alias=backend_alias,
    )
    return f"discarded {deleted} ready jobs from {queue_name}"

  if state == "scheduled" and action == "discard":
    deleted = discard_scheduled_jobs(
      job_ids=job_ids,
      batch_size=max(len(job_ids), 1),
      backend_alias=backend_alias,
    )
    return f"discarded {deleted} scheduled jobs from {queue_name}"

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

  if state == "finished" and action == "enqueue":
    alias = get_database_alias(backend_alias)
    jobs = list(
      Job.objects.using(alias).filter(
        pk__in=job_ids,
        backend_name=backend_alias,
        queue_name=queue_name,
        finished_at__isnull=False,
      )
    )
    for job in jobs:
      enqueue_job_again(job.pk, backend_alias=backend_alias)
    return f"enqueued {len(jobs)} finished jobs again from {queue_name}"

  raise ValueError(f"unsupported {state!r} job action {action!r}")


def job_actions_for_state(state):
  return QUEUE_STATE_CONFIG[state]["job_actions"]


def _summary_cards(*, backend_alias, queue_rows, process_rows, recurring_rows, semaphore_rows):
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
      "detail_parts": (
        {
          "label": f"{failed_count} failed",
          "url": _job_changelist_url(backend_alias=backend_alias, status="failed"),
        },
        {"label": "and"},
        {
          "label": f"{blocked_count} blocked",
          "url": _job_changelist_url(backend_alias=backend_alias, status="blocked"),
        },
      ),
    },
    {
      "label": "runtime",
      "value": live_processes,
      "detail": f"{live_processes} live, {stale_processes} stale",
    },
    {
      "label": "control-plane",
      "value": len(recurring_rows) + len(semaphore_rows),
      "detail": f"{len(recurring_rows)} recurring and {len(semaphore_rows)} semaphores",
    },
  )


def _split_queue_rows(queue_rows):
  backend_queue_rows = []
  shared_queue_rows = []
  for row in queue_rows:
    if row["has_backend_jobs"]:
      backend_queue_rows.append(row)
      continue
    shared_queue_rows.append(row)
  return backend_queue_rows, shared_queue_rows


def _backend_facts(*, config, queue_database_alias, recurring_count, semaphore_count):
  retention = "disabled"
  if config.clear_finished_jobs_after is not None:
    retention = f"{config.clear_finished_jobs_after}s"

  capabilities = database_capabilities(queue_database_alias)

  return (
    {"label": "mode", "value": config.mode},
    {"label": "queue db", "value": queue_database_alias},
    {"label": "scheduler", "value": "enabled" if config.has_scheduler_work else "disabled"},
    {
      "label": "notify",
      "value": _capability_fact_value(
        enabled=config.listen_notify,
        supported=capabilities.supports_listen_notify,
      ),
    },
    {
      "label": "skip locked",
      "value": _capability_fact_value(
        enabled=config.use_skip_locked,
        supported=capabilities.supports_skip_locked,
      ),
    },
    {"label": "heartbeat", "value": f"{config.process_alive_threshold}s"},
    {"label": "retention", "value": retention},
    {"label": "recurring", "value": str(recurring_count)},
    {"label": "semaphores", "value": str(semaphore_count)},
  )


def _capability_fact_value(*, enabled, supported):
  if not supported:
    return "unsupported"
  if enabled:
    return "on"
  return "off"


def _overview_section(*, section, rows, page_param, page_size, sort_param, query_params, anchor):
  raw_sort = query_params.get(sort_param)
  sort, explicit_sort = _resolve_overview_sort(section=section, raw_sort=raw_sort)
  rows = _sort_overview_rows(rows=rows, section=section, sort=sort)
  sort_value = sort if explicit_sort else None

  if section == "processes":
    page = _paginate_process_rows(
      rows=rows,
      page_size=page_size,
      page_number=query_params.get(page_param, 1),
    )
  else:
    page = _paginate_standard_rows(
      rows=rows,
      page_size=page_size,
      page_number=query_params.get(page_param, 1),
    )

  return _section_payload(
    section=section,
    rows=page["rows"],
    total_count=page["total_count"],
    total_pages=page["total_pages"],
    current_page=page["number"],
    start_index=page["start_index"],
    end_index=page["end_index"],
    query_params=query_params,
    page_param=page_param,
    sort_param=sort_param,
    sort=sort,
    sort_value=sort_value,
    explicit_sort=explicit_sort,
    anchor=anchor,
  )


def _section_payload(
  *,
  section,
  rows,
  total_count,
  total_pages,
  current_page,
  start_index,
  end_index,
  query_params,
  page_param,
  sort_param,
  sort,
  sort_value,
  explicit_sort,
  anchor,
):
  return {
    "headers": _overview_headers(
      section=section,
      query_params=query_params,
      sort_param=sort_param,
      sort=sort,
      explicit_sort=explicit_sort,
      page_param=page_param,
      anchor=anchor,
    ),
    "rows": rows,
    "total_count": total_count,
    "pagination_required": total_pages > 1,
    "page_links": _page_links_for_total_pages(
      total_pages=total_pages,
      current_page=current_page,
      query_params=query_params,
      page_param=page_param,
      sort_param=sort_param,
      sort=sort_value,
      anchor=anchor,
    ),
    "result_count_text": _result_count_text(
      section=section,
      total_count=total_count,
      start=start_index,
      end=end_index,
    ),
    "sort": sort,
    "num_sorted_fields": len(_parse_sort_fields(sort)) if explicit_sort else 0,
    "anchor": anchor,
  }


def _paginate_standard_rows(*, rows, page_size, page_number):
  paginator = Paginator(rows, page_size)
  page_obj = paginator.get_page(page_number)
  total_count = paginator.count
  return {
    "rows": list(page_obj.object_list),
    "number": page_obj.number,
    "total_pages": paginator.num_pages,
    "total_count": total_count,
    "start_index": page_obj.start_index() if total_count else 0,
    "end_index": page_obj.end_index() if total_count else 0,
  }


def _overview_query(*, query_params, page_param, page_number, sort_param=None, sort=None):
  params = query_params.copy()
  if str(page_number) == "1":
    params.pop(page_param, None)
  else:
    params[page_param] = page_number
  if sort_param and sort:
    params[sort_param] = sort
  if hasattr(params, "urlencode"):
    return params.urlencode()
  return urlencode(params, doseq=True)


def _page_links_for_total_pages(
  *, total_pages, current_page, query_params, page_param, sort_param, sort, anchor
):
  if total_pages <= 1:
    return ()

  paginator = Paginator(range(total_pages), 1)
  links = []
  for page_number in paginator.get_elided_page_range(current_page):
    if page_number == paginator.ELLIPSIS:
      links.append({"is_ellipsis": True, "label": paginator.ELLIPSIS})
      continue

    query = _overview_query(
      query_params=query_params,
      page_param=page_param,
      page_number=page_number,
      sort_param=sort_param,
      sort=sort,
    )
    url = f"?{query}#{anchor}" if query else f"?#{anchor}"
    links.append(
      {
        "is_current": page_number == current_page,
        "is_ellipsis": False,
        "number": page_number,
        "url": url,
      }
    )
  return tuple(links)


def _result_count_text(*, section, total_count, start, end):
  singular, plural = OVERVIEW_COUNT_LABELS[section]
  label = singular if total_count == 1 else plural
  if total_count == 0:
    return f"0 {plural}"
  return f"{start}-{end} of {total_count} {label}"


def _resolve_overview_sort(*, section, raw_sort):
  config = OVERVIEW_SORTS[section]
  default_field = config["default"]
  field = config["fields"][default_field]
  default_sort = f"-{default_field}" if field["default_desc"] else default_field
  return _resolve_sort(fields=config["fields"], raw_sort=raw_sort, default_sort=default_sort)


def _resolve_queue_sort(*, state, raw_sort):
  return _resolve_sort(fields=QUEUE_PAGE_SORTS[state]["fields"], raw_sort=raw_sort)


def _resolve_sort(*, fields, raw_sort, default_sort=None):
  if not raw_sort:
    return default_sort, False

  parts = raw_sort.split(".")
  valid = []
  seen = set()
  for part in parts:
    field_name = part.removeprefix("-")
    if field_name in fields and field_name not in seen:
      valid.append(part)
      seen.add(field_name)
  if not valid:
    return default_sort, False
  return ".".join(valid), True


def _parse_sort_fields(sort):
  if not sort:
    return ()
  return tuple(sort.split("."))


def _sort_overview_rows(*, rows, section, sort):
  if section == "processes":
    return _sort_process_overview_rows(rows=rows, sort=sort)

  config = OVERVIEW_SORTS[section]
  sort_fields = _parse_sort_fields(sort)
  sort_specs = []
  for part in sort_fields:
    field_name = part.removeprefix("-")
    key_name = config["fields"][field_name]["key"]
    reverse = part.startswith("-")
    sort_specs.append((key_name, reverse))
  return _sort_rows_by_keys(rows=rows, sort_specs=sort_specs)


def _sort_process_overview_rows(*, rows, sort):
  config = OVERVIEW_SORTS["processes"]
  sort_fields = _parse_sort_fields(sort)
  sort_specs = []
  for part in sort_fields:
    field_name = part.removeprefix("-")
    key_name = config["fields"][field_name]["key"]
    reverse = part.startswith("-")
    sort_specs.append((key_name, reverse))

  primary_key, primary_reverse = sort_specs[0]

  groups = []
  current_group = None
  for row in rows:
    if row.get("is_child"):
      current_group["children"].append(row)
      continue
    current_group = {"root": row, "children": []}
    groups.append(current_group)

  groups = _sort_rows_by_keys(
    rows=groups,
    sort_specs=sort_specs,
    getter=lambda group, key: group["root"].get(key),
  )

  sorted_rows = []
  for group in groups:
    sorted_rows.append(group["root"])
    sorted_rows.extend(_sort_rows_by_keys(rows=group["children"], sort_specs=sort_specs))
  return sorted_rows


def _paginate_process_rows(*, rows, page_size, page_number):
  groups = _group_process_rows(rows)
  pages = []
  current_page = []
  current_size = 0

  for group in groups:
    group_size = len(group)
    if current_page and current_size + group_size > page_size:
      pages.append(current_page)
      current_page = []
      current_size = 0
    current_page.append(group)
    current_size += group_size

  if current_page or not pages:
    pages.append(current_page)

  total_pages = len(pages)
  number = _coerce_page_number(page_number, total_pages)
  page_groups = pages[number - 1]
  page_rows = [row for group in page_groups for row in group]
  rows_before_page = sum(len(group) for page in pages[: number - 1] for group in page)
  total_count = len(rows)

  return {
    "rows": page_rows,
    "number": number,
    "total_pages": total_pages,
    "total_count": total_count,
    "start_index": rows_before_page + 1 if total_count else 0,
    "end_index": rows_before_page + len(page_rows) if total_count else 0,
  }


def _group_process_rows(rows):
  groups = []
  current_group = None
  for row in rows:
    if row.get("is_child"):
      current_group.append(row)
      continue
    current_group = [row]
    groups.append(current_group)
  return groups


def _coerce_page_number(page_number, total_pages):
  try:
    number = int(page_number)
  except (TypeError, ValueError):
    number = 1
  if number < 1:
    return 1
  if number > total_pages:
    return total_pages
  return number


def _sort_queue_jobs(*, jobs, state, sort):
  fields = QUEUE_PAGE_SORTS[state]["fields"]
  sort_specs = []
  for part in _parse_sort_fields(sort):
    field_name = part.removeprefix("-")
    key_name = fields[field_name]["key"]
    reverse = part.startswith("-")
    sort_specs.append((key_name, reverse))
  return _sort_rows_by_keys(rows=jobs, sort_specs=sort_specs, getter=_job_sort_value)


def _sort_rows_by_keys(*, rows, sort_specs, getter=None):
  if getter is None:
    getter = lambda row, key: row.get(key)  # noqa: E731

  def sort_key(row):
    parts = []
    for key_name, rev in sort_specs:
      value = getter(row, key_name)
      sv = _sortable_value(value)
      # none values sort last regardless of direction
      is_none = value is None
      parts.append((is_none, _Reversible(sv) if rev else sv))
    return tuple(parts)

  return sorted(rows, key=sort_key)


class _Reversible:
  __slots__ = ("value",)

  def __init__(self, value):
    self.value = value

  def __lt__(self, other):
    return other.value < self.value

  def __eq__(self, other):
    return self.value == other.value

  def __le__(self, other):
    return other.value <= self.value

  def __gt__(self, other):
    return other.value > self.value

  def __ge__(self, other):
    return other.value >= self.value


def _job_sort_value(job, key):
  return _lookup_value(job, key)


def _lookup_value(value, lookup):
  current = value
  for part in lookup.split("__"):
    if current is None:
      return None
    if isinstance(current, dict):
      current = current.get(part)
      continue
    current = getattr(current, part, None)
  return current


def _sortable_value(value):
  if isinstance(value, bool):
    return int(value)
  if isinstance(value, UUID):
    return str(value)
  if isinstance(value, (dict, list, tuple)):
    return json.dumps(value, sort_keys=True)
  if isinstance(value, str):
    return value.lower()
  return value


def _overview_headers(
  *, section, query_params, sort_param, sort, explicit_sort, page_param, anchor
):
  return _sortable_headers(
    fields=OVERVIEW_SORTS[section]["fields"],
    query_params=query_params,
    sort_param=sort_param,
    sort=sort,
    explicit_sort=explicit_sort,
    page_param=page_param,
    anchor=anchor,
    preserve_anchor=True,
  )


def _queue_page_headers(*, state, query_params, sort, explicit_sort, page_param, anchor):
  return _sortable_headers(
    fields=QUEUE_PAGE_SORTS[state]["fields"],
    query_params=query_params,
    sort_param="sort",
    sort=sort,
    explicit_sort=explicit_sort,
    page_param=page_param,
    anchor=anchor,
    preserve_anchor=False,
  )


def _sortable_headers(
  *, fields, query_params, sort_param, sort, explicit_sort, page_param, anchor, preserve_anchor
):
  sort_fields = _parse_sort_fields(sort) if explicit_sort else ()

  sort_index = {}
  for i, part in enumerate(sort_fields):
    fname = part.removeprefix("-")
    sort_index[fname] = (i + 1, not part.startswith("-"))

  multi_sort = len(sort_fields) > 1
  headers = []

  for field_name, field in fields.items():
    position, ascending = sort_index.get(field_name, (None, None))
    is_sorted = position is not None

    if is_sorted:
      toggled = field_name if not ascending else f"-{field_name}"
      toggled_fields = list(sort_fields)
      toggled_fields[position - 1] = toggled
      toggle_sort = ".".join(toggled_fields)

      removed_fields = [part for part in sort_fields if part.removeprefix("-") != field_name]
      remove_sort = ".".join(removed_fields) if removed_fields else None
    else:
      toggle_sort = None

    if is_sorted:
      toggled = field_name if not ascending else f"-{field_name}"
      primary_fields = [toggled] + [
        part for part in sort_fields if part.removeprefix("-") != field_name
      ]
      primary_sort = ".".join(primary_fields)
    else:
      new_field = f"-{field_name}" if field["default_desc"] else field_name
      primary_sort = ".".join([new_field] + list(sort_fields)) if sort_fields else new_field

    primary_url = _overview_sort_url(
      query_params=query_params,
      sort_param=sort_param,
      sort_value=primary_sort,
      page_param=page_param,
      anchor=anchor,
      preserve_anchor=preserve_anchor,
    )
    toggle_url = (
      _overview_sort_url(
        query_params=query_params,
        sort_param=sort_param,
        sort_value=toggle_sort,
        page_param=page_param,
        anchor=anchor,
        preserve_anchor=preserve_anchor,
      )
      if is_sorted
      else primary_url
    )
    remove_url = (
      _overview_sort_url(
        query_params=query_params,
        sort_param=sort_param,
        sort_value=remove_sort,
        page_param=page_param,
        anchor=anchor,
        preserve_anchor=preserve_anchor,
      )
      if is_sorted
      else None
    )

    classes = [f"column-{field_name}", "sortable"]
    if field.get("css_class"):
      classes.append(field["css_class"])
    if is_sorted:
      classes.extend(("sorted", "ascending" if ascending else "descending"))

    headers.append(
      {
        "text": field["label"],
        "url_primary": primary_url,
        "url_toggle": toggle_url,
        "url_remove": remove_url or primary_url,
        "class_attrib": f' class="{" ".join(classes)}"',
        "sortable": True,
        "sorted": is_sorted,
        "ascending": ascending if is_sorted else None,
        "sort_priority": position if multi_sort else None,
      }
    )
  return tuple(headers)


def _queue_result_count_text(*, page_obj, total_count):
  if total_count == 0:
    return "0 jobs"
  return f"{page_obj.start_index()}-{page_obj.end_index()} of {total_count} jobs"


def _overview_sort_url(
  *, query_params, sort_param, sort_value, page_param, anchor, preserve_anchor
):
  params = query_params.copy()
  if sort_value:
    params[sort_param] = sort_value
  else:
    params.pop(sort_param, None)
  params.pop(page_param, None)
  url = params.urlencode() if hasattr(params, "urlencode") else urlencode(params, doseq=True)
  if not url:
    return f"?#{anchor}" if preserve_anchor else "?"
  return f"?{url}#{anchor}" if preserve_anchor else f"?{url}"


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
    rows.append(
      _queue_snapshot(
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
    )
  return rows


def _queue_snapshot(
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
    state_counts = _queue_state_counts(backend_alias=backend_alias, queue_name=queue_name)
    ready_count = state_counts["ready"]
    claimed_count = state_counts["claimed"]
    scheduled_count = state_counts["scheduled"]
    blocked_count = state_counts["blocked"]
    failed_count = state_counts["failed"]
    finished_count = state_counts["finished"]
  if paused is None:
    paused = Pause.objects.using(alias).filter(queue_name=queue_name).exists()
  if recurring is None:
    recurring = RecurringTask.objects.using(alias).filter(queue_name=queue_name).exists()
  if oldest_ready_at is None:
    oldest_ready_at = (
      ReadyExecution.objects.using(alias)
      .filter(job__backend_name=backend_alias, queue_name=queue_name)
      .aggregate(oldest=Min("job__created_at"))["oldest"]
    )
  if oldest_scheduled_at is None:
    oldest_scheduled_at = (
      ScheduledExecution.objects.using(alias)
      .filter(job__backend_name=backend_alias, queue_name=queue_name)
      .aggregate(oldest=Min("scheduled_at"))["oldest"]
    )
  if oldest_blocked_at is None:
    oldest_blocked_at = (
      BlockedExecution.objects.using(alias)
      .filter(job__backend_name=backend_alias, queue_name=queue_name)
      .aggregate(oldest=Min("expires_at"))["oldest"]
    )
  if live_workers is None:
    live_workers = [
      process
      for process in Process.objects.using(alias).filter(kind="Worker")
      if process.last_heartbeat_at >= process_cutoff
    ]

  latency_seconds = None
  if oldest_ready_at is not None and paused is False:
    latency_seconds = max((now - oldest_ready_at).total_seconds(), 0.0)

  shared_sources = []
  if paused:
    shared_sources.append("pause")
  if recurring:
    shared_sources.append("recurring task")

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
    "has_backend_jobs": any(
      (
        ready_count,
        claimed_count,
        scheduled_count,
        blocked_count,
        failed_count,
        finished_count,
      )
    ),
    "shared_sources": tuple(shared_sources),
    "shared_source_labels": ", ".join(shared_sources),
    "live_worker_count": sum(
      1
      for worker in live_workers
      if _queue_matches_selectors(queue_name, worker.metadata.get("queues", []))
    ),
  }


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
        "jobs_url": _job_changelist_url(
          backend_alias=backend_alias,
          recurring_task_key=task.key,
        ),
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
      "jobs_url": _job_changelist_url(
        backend_alias=backend_alias,
        concurrency_key=semaphore.key,
      ),
    }
    for semaphore in Semaphore.objects.using(alias).order_by("key")
  ]


def _job_changelist_url(backend_alias, **filters):
  params = {
    "backend": backend_alias,
    **filters,
  }
  return f"{reverse('admin:dj_queue_job_changelist')}?{urlencode(params)}"


def _failed_execution_changelist_url(backend_alias, **filters):
  params = {
    "backend": backend_alias,
    **filters,
  }
  return f"{reverse('admin:dj_queue_failedexecution_changelist')}?{urlencode(params)}"


def _queue_state_counts(*, backend_alias, queue_name):
  alias = get_database_alias(backend_alias)
  base_queryset = Job.objects.using(alias).filter(
    backend_name=backend_alias, queue_name=queue_name
  )
  return {
    state: base_queryset.filter(**config["query_filter"]).count()
    for state, config in QUEUE_STATE_CONFIG.items()
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
  state_config = QUEUE_STATE_CONFIG[state]
  return queryset.filter(**state_config["query_filter"]).order_by(*state_config["query_order"])


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
