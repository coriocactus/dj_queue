from urllib.parse import urlencode

from django.core.paginator import Paginator
from django.http import Http404
from django.urls import reverse
from django.utils import timezone

from dj_queue import dashboard_tables as tables
from dj_queue.api import QueueInfo
from dj_queue.config import load_backend_config
from dj_queue.dashboard_tables import OVERVIEW_COUNT_LABELS, OVERVIEW_SORTS, QUEUE_PAGE_SORTS
from dj_queue.db import database_capabilities, get_database_alias
from dj_queue.observability import backend_choices, configured_backend_aliases
from dj_queue.queue_state import (
  QUEUE_STATE_DEFINITIONS,
  QUEUE_STATE_LABELS,
  QUEUE_STATES,
  queue_state_count_key,
  queue_state_queryset,
)
from dj_queue.reads import controls, processes, queues

PAGE_SIZE = 100
OVERVIEW_PAGE_SIZES = {
  "queues": 18,
  "shared_queues": 5,
  "processes": 10,
  "recurring": 12,
  "semaphores": 12,
}


def resolve_backend_alias(raw_backend_alias):
  aliases = configured_backend_aliases()
  if not aliases:
    raise Http404("no dj_queue backends are configured")
  backend_alias = raw_backend_alias or ("default" if "default" in aliases else aliases[0])
  if backend_alias not in aliases:
    raise Http404(f"unknown dj_queue backend {backend_alias!r}")
  return backend_alias


def dashboard_context(*, backend_alias, query_params=None):
  config = load_backend_config(backend_alias)
  if query_params is None:
    query_params = {}

  now = timezone.now()
  queue_database_alias = get_database_alias(backend_alias)
  process_cutoff = processes.process_cutoff_for_backend(
    backend_alias,
    now=now,
    max_age=config.process_alive_threshold,
  )
  queue_rows = queues.queue_rows(
    backend_alias=backend_alias,
    now=now,
    process_cutoff=process_cutoff,
  )
  process_rows = processes.process_rows(
    backend_alias=backend_alias,
    now=now,
    process_cutoff=process_cutoff,
    scope="backend",
  )
  queue_section = tables.overview_section(
    section="queues",
    rows=queue_rows,
    page_param="queues_page",
    page_size=OVERVIEW_PAGE_SIZES["queues"],
    sort_param="queues_sort",
    query_params=query_params,
    anchor="queue-summary",
  )
  process_section = tables.overview_section(
    section="processes",
    rows=process_rows,
    page_param="processes_page",
    page_size=OVERVIEW_PAGE_SIZES["processes"],
    sort_param="processes_sort",
    query_params=query_params,
    anchor="process-summary",
  )
  recurring_section = _control_overview_section(
    section="recurring",
    backend_alias=backend_alias,
    now=now,
    query_params=query_params,
  )
  semaphore_section = _control_overview_section(
    section="semaphores",
    now=now,
    backend_alias=backend_alias,
    query_params=query_params,
  )

  return {
    "backend_alias": backend_alias,
    "backend_choices": backend_choices(),
    "config": config,
    "queue_database_alias": queue_database_alias,
    "summary_cards": _summary_cards(
      backend_alias=backend_alias,
      queue_rows=queue_rows,
      process_rows=process_rows,
      recurring_count=recurring_section["total_count"],
      semaphore_count=semaphore_section["total_count"],
    ),
    "backend_facts": _backend_facts(
      config=config,
      queue_database_alias=queue_database_alias,
      recurring_count=recurring_section["total_count"],
      semaphore_count=semaphore_section["total_count"],
    ),
    "queue_section": queue_section,
    "process_section": process_section,
    "recurring_section": recurring_section,
    "semaphore_section": semaphore_section,
  }


def queue_page_context(*, backend_alias, queue_name, state, page_number, query_params=None):
  if state not in QUEUE_STATE_LABELS:
    raise Http404(f"unknown queue state {state!r}")
  if query_params is None:
    query_params = {}

  alias = get_database_alias(backend_alias)
  config = load_backend_config(backend_alias)
  now = timezone.now()
  process_cutoff = processes.process_cutoff_for_backend(
    backend_alias,
    now=now,
    max_age=config.process_alive_threshold,
  )
  queryset = queue_state_queryset(
    backend_alias=backend_alias,
    queue_name=queue_name,
    state=state,
  )
  sort, explicit_sort = tables.resolve_queue_sort(state=state, raw_sort=query_params.get("sort"))
  if explicit_sort:
    jobs = tables.sorted_queue_jobs(queryset=queryset, state=state, sort=sort)
  else:
    jobs = queryset

  paginator = Paginator(jobs, PAGE_SIZE)
  page_obj = paginator.get_page(query_params.get("page", page_number))
  queue_row = queues.queue_snapshot(
    backend_alias=backend_alias,
    queue_name=queue_name,
    now=now,
    process_cutoff=process_cutoff,
  )
  queue_info = QueueInfo(queue_name, backend_alias=backend_alias)
  state_counts = {
    definition.name: queue_row[definition.count_key] for definition in QUEUE_STATE_DEFINITIONS
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
    "table_headers": tables.queue_page_headers(
      state=state,
      query_params=query_params,
      sort=sort,
      explicit_sort=explicit_sort,
      page_param="page",
      anchor="result_list",
    ),
    "queue_num_sorted_fields": len(tables.parse_sort_fields(sort)) if explicit_sort else 0,
    "raw_links": tuple(raw_links),
    "page_obj": page_obj,
    "jobs": list(page_obj.object_list),
    "page_links": (
      tables.page_links_for_total_pages(
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
    "result_count_text": tables.queue_result_count_text(
      page_obj=page_obj, total_count=paginator.count
    ),
    "process_cutoff": process_cutoff,
  }


def _summary_cards(*, backend_alias, queue_rows, process_rows, recurring_count, semaphore_count):
  paused_count = sum(1 for row in queue_rows if row["paused"])
  ready_count = sum(row[queue_state_count_key("ready")] for row in queue_rows)
  scheduled_count = sum(row[queue_state_count_key("scheduled")] for row in queue_rows)
  failed_count = sum(row[queue_state_count_key("failed")] for row in queue_rows)
  blocked_count = sum(row[queue_state_count_key("blocked")] for row in queue_rows)
  invalid_count = sum(row[queue_state_count_key("invalid")] for row in queue_rows)
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
      "value": failed_count + blocked_count + invalid_count,
      "detail_parts": (
        {
          "label": f"{failed_count} failed",
          "url": _job_changelist_url(backend_alias=backend_alias, status="failed"),
        },
        {"label": ","},
        {
          "label": f"{blocked_count} blocked",
          "url": _job_changelist_url(backend_alias=backend_alias, status="blocked"),
        },
        {"label": "and"},
        {
          "label": f"{invalid_count} invalid",
          "url": _job_changelist_url(backend_alias=backend_alias, status="invalid"),
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
      "value": recurring_count + semaphore_count,
      "detail": f"{recurring_count} recurring and {semaphore_count} semaphores",
    },
  )


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


def _control_overview_section(*, section, backend_alias, now, query_params):
  page_param = f"{section}_page"
  sort_param = f"{section}_sort"
  sort, explicit_sort = tables.resolve_overview_sort(
    section=section, raw_sort=query_params.get(sort_param)
  )
  page_options = {
    "backend_alias": backend_alias,
    "page_size": OVERVIEW_PAGE_SIZES[section],
    "page_number": query_params.get(page_param, 1),
    "sort": sort,
  }
  if section == "recurring":
    page = _recurring_overview_page(now=now, **page_options)
    anchor = "recurring-summary"
  else:
    page = _semaphore_overview_page(**page_options)
    anchor = "semaphore-summary"
  return tables.section_payload(
    section=section,
    page=page,
    query_params=query_params,
    page_param=page_param,
    sort_param=sort_param,
    sort=sort,
    explicit_sort=explicit_sort,
    anchor=anchor,
  )


def _recurring_overview_page(*, backend_alias, now, page_size, page_number, sort):
  if _recurring_sort_requires_python(sort):
    rows = controls.recurring_rows_for_backend(backend_alias=backend_alias, now=now)
    rows = tables.sort_overview_rows(rows=rows, section="recurring", sort=sort)
    page = tables.paginate_standard_rows(rows=rows, page_size=page_size, page_number=page_number)
  else:
    queryset = controls.recurring_tasks_with_last_run(backend_alias=backend_alias).order_by(
      *tables.overview_queryset_ordering(section="recurring", sort=sort, tie_breaker="key")
    )
    page = tables.paginate_standard_rows(
      rows=queryset, page_size=page_size, page_number=page_number
    )
    page["rows"] = [
      controls.recurring_row(task, now=now, last_run_at=task.last_run_at) for task in page["rows"]
    ]
  page["rows"] = [
    _recurring_row_with_jobs_url(row, backend_alias=backend_alias) for row in page["rows"]
  ]
  return page


def _semaphore_overview_page(*, backend_alias, page_size, page_number, sort):
  alias = get_database_alias(backend_alias)
  if _semaphore_sort_requires_python(sort):
    rows = controls.semaphore_rows_for_backend(backend_alias=backend_alias)
    rows = tables.sort_overview_rows(
      rows=rows, section="semaphores", sort=tables.sort_with_tie_breaker(sort, "key")
    )
    page = tables.paginate_standard_rows(rows=rows, page_size=page_size, page_number=page_number)
  else:
    queryset = controls.semaphores_with_waiter_counts(backend_alias=backend_alias).order_by(
      *tables.overview_queryset_ordering(
        section="semaphores", sort=sort, field_map={"available_slots": "value"}, tie_breaker="key"
      )
    )
    page = tables.paginate_standard_rows(
      rows=queryset, page_size=page_size, page_number=page_number
    )
    page["rows"] = [
      controls.semaphore_row(semaphore, alias=alias, blocked_waiters=semaphore.blocked_waiters)
      for semaphore in page["rows"]
    ]
  page["rows"] = [
    _semaphore_row_with_jobs_url(row, backend_alias=backend_alias) for row in page["rows"]
  ]
  return page


def _recurring_sort_requires_python(sort):
  return any(part.removeprefix("-") == "next_run" for part in tables.parse_sort_fields(sort))


def _semaphore_sort_requires_python(sort):
  return any(
    part.removeprefix("-") in {"active", "blocked_waiters"}
    for part in tables.parse_sort_fields(sort)
  )


def _recurring_row_with_jobs_url(row, *, backend_alias):
  return {
    **row,
    "jobs_url": _job_changelist_url(
      backend_alias=backend_alias,
      recurring_task_key=row["key"],
    ),
  }


def _semaphore_row_with_jobs_url(row, *, backend_alias):
  return {
    **row,
    "jobs_url": _job_changelist_url(
      backend_alias=backend_alias,
      concurrency_key=row["key"],
    ),
  }


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


__all__ = [
  "OVERVIEW_COUNT_LABELS",
  "OVERVIEW_PAGE_SIZES",
  "OVERVIEW_SORTS",
  "PAGE_SIZE",
  "QUEUE_PAGE_SORTS",
  "backend_choices",
  "configured_backend_aliases",
  "dashboard_context",
  "queue_page_context",
  "resolve_backend_alias",
]
