import json
from functools import partial
from urllib.parse import urlencode
from uuid import UUID

from django.core.paginator import Paginator
from django.db.models import F

from dj_queue.queue_state import (
  QUEUE_STATE_DEFINITIONS,
)

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
      **{
        definition.name: {
          "label": definition.label,
          "key": definition.count_key,
          "default_desc": True,
        }
        for definition in QUEUE_STATE_DEFINITIONS
      },
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
      "backend": {"label": "backend", "key": "backend_alias", "default_desc": False},
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
      "active": {"label": "active", "key": "active_count", "default_desc": True},
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


_JOB_COLUMNS = {
  "id": {"label": "id", "key": "id", "default_desc": False},
  "task": {"label": "task", "key": "task_path", "default_desc": False},
  "priority": {"label": "priority", "key": "priority", "default_desc": True},
  "created": {"label": "created", "key": "created_at", "default_desc": True},
}

QUEUE_PAGE_SORTS = {
  state: {"fields": {**{name: field.copy() for name, field in _JOB_COLUMNS.items()}, **fields}}
  for state, fields in {
    "ready": {},
    "claimed": {
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
    },
    "scheduled": {
      "scheduled_at": {
        "label": "scheduled at",
        "key": "scheduled_execution__scheduled_at",
        "default_desc": False,
      }
    },
    "blocked": {
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
    },
    "failed": {
      "exception": {
        "label": "exception",
        "key": "failed_execution__exception_class",
        "default_desc": False,
      },
      "message": {"label": "message", "key": "failed_execution__message", "default_desc": False},
    },
    "finished": {
      "finished_at": {"label": "finished at", "key": "finished_at", "default_desc": True},
      "return_value": {
        "label": "return value",
        "key": "return_value",
        "default_desc": False,
        "sortable": False,
      },
    },
    "invalid": {},
  }.items()
}


def overview_section(*, section, rows, page_param, page_size, sort_param, query_params, anchor):
  raw_sort = query_params.get(sort_param)
  sort, explicit_sort = resolve_overview_sort(section=section, raw_sort=raw_sort)
  rows = sort_overview_rows(rows=rows, section=section, sort=sort)

  if section == "processes":
    page = _paginate_process_rows(
      rows=rows,
      page_size=page_size,
      page_number=query_params.get(page_param, 1),
    )
  else:
    page = paginate_standard_rows(
      rows=rows,
      page_size=page_size,
      page_number=query_params.get(page_param, 1),
    )

  return section_payload(
    section=section,
    page=page,
    query_params=query_params,
    page_param=page_param,
    sort_param=sort_param,
    sort=sort,
    explicit_sort=explicit_sort,
    anchor=anchor,
  )


def section_payload(
  *, section, page, query_params, page_param, sort_param, sort, explicit_sort, anchor
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
    "rows": page["rows"],
    "total_count": page["total_count"],
    "pagination_required": page["total_pages"] > 1,
    "page_links": page_links_for_total_pages(
      total_pages=page["total_pages"],
      current_page=page["number"],
      query_params=query_params,
      page_param=page_param,
      sort_param=sort_param,
      sort=sort if explicit_sort else None,
      anchor=anchor,
    ),
    "result_count_text": _result_count_text(
      section=section,
      total_count=page["total_count"],
      start=page["start_index"],
      end=page["end_index"],
    ),
    "sort": sort,
    "num_sorted_fields": len(parse_sort_fields(sort)) if explicit_sort else 0,
    "anchor": anchor,
  }


def paginate_standard_rows(*, rows, page_size, page_number):
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


def sort_with_tie_breaker(sort, tie_breaker):
  field_names = {part.removeprefix("-") for part in parse_sort_fields(sort)}
  if tie_breaker in field_names:
    return sort
  return f"{sort}.{tie_breaker}"


def overview_queryset_ordering(*, section, sort, field_map=None, tie_breaker=None):
  return _queryset_ordering(
    fields=OVERVIEW_SORTS[section]["fields"],
    sort=sort,
    field_map=field_map,
    tie_breaker=tie_breaker,
  )


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


def page_links_for_total_pages(
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


def resolve_overview_sort(*, section, raw_sort):
  config = OVERVIEW_SORTS[section]
  default_field = config["default"]
  field = config["fields"][default_field]
  default_sort = f"-{default_field}" if field["default_desc"] else default_field
  return _resolve_sort(fields=config["fields"], raw_sort=raw_sort, default_sort=default_sort)


def resolve_queue_sort(*, state, raw_sort):
  return _resolve_sort(fields=QUEUE_PAGE_SORTS[state]["fields"], raw_sort=raw_sort)


def _resolve_sort(*, fields, raw_sort, default_sort=None):
  if not raw_sort:
    return default_sort, False

  parts = raw_sort.split(".")
  valid = []
  seen = set()
  for part in parts:
    field_name = part.removeprefix("-")
    field = fields.get(field_name)
    if field is not None and field.get("sortable", True) and field_name not in seen:
      valid.append(part)
      seen.add(field_name)
  if not valid:
    return default_sort, False
  return ".".join(valid), True


def parse_sort_fields(sort):
  if not sort:
    return ()
  return tuple(sort.split("."))


def sort_overview_rows(*, rows, section, sort):
  if section == "processes":
    return _sort_process_overview_rows(rows=rows, sort=sort)
  return _sort_rows_by_keys(
    rows=rows, sort_specs=_sort_specs(fields=OVERVIEW_SORTS[section]["fields"], sort=sort)
  )


def _sort_process_overview_rows(*, rows, sort):
  sort_specs = _sort_specs(fields=OVERVIEW_SORTS["processes"]["fields"], sort=sort)
  groups = _sort_rows_by_keys(
    rows=_group_process_rows(rows),
    sort_specs=sort_specs,
    getter=lambda group, key: group[0].get(key),
  )
  sorted_rows = []
  for group in groups:
    sorted_rows.append(group[0])
    sorted_rows.extend(_sort_rows_by_keys(rows=group[1:], sort_specs=sort_specs))
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


def sorted_queue_jobs(*, queryset, state, sort):
  return queryset.order_by(
    *_queryset_ordering(fields=QUEUE_PAGE_SORTS[state]["fields"], sort=sort, tie_breaker="id")
  )


def _queryset_ordering(*, fields, sort, field_map=None, tie_breaker=None):
  field_map = field_map or {}
  order_by = []
  ordered_fields = set()
  for key_name, descending in _sort_specs(fields=fields, sort=sort):
    query_field = field_map.get(key_name, key_name)
    expression = F(query_field)
    order_by.append(
      expression.desc(nulls_last=True) if descending else expression.asc(nulls_last=True)
    )
    ordered_fields.add(query_field)
  if tie_breaker and tie_breaker not in ordered_fields:
    order_by.append(F(tie_breaker).asc())
  return order_by


def _sort_specs(*, fields, sort):
  return [
    (fields[part.removeprefix("-")]["key"], part.startswith("-"))
    for part in parse_sort_fields(sort)
  ]


def _sort_rows_by_keys(*, rows, sort_specs, getter=None):
  if getter is None:

    def getter(row, key):
      return row.get(key)

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


def queue_page_headers(*, state, query_params, sort, explicit_sort, page_param, anchor):
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
  sort_fields = parse_sort_fields(sort) if explicit_sort else ()
  sort_index = {
    part.removeprefix("-"): (index + 1, not part.startswith("-"))
    for index, part in enumerate(sort_fields)
  }
  sort_url = partial(
    _overview_sort_url,
    query_params=query_params,
    sort_param=sort_param,
    page_param=page_param,
    anchor=anchor,
    preserve_anchor=preserve_anchor,
  )
  headers = []
  for field_name, field in fields.items():
    sortable = field.get("sortable", True)
    position, ascending = sort_index.get(field_name, (None, None)) if sortable else (None, None)
    is_sorted = position is not None
    classes = [f"column-{field_name}"]
    primary_url = toggle_url = remove_url = None
    if sortable:
      classes.append("sortable")
      primary, toggle, remove = _column_sort_values(
        field_name,
        field,
        sort_fields=sort_fields,
        position=position,
        ascending=ascending,
      )
      primary_url = sort_url(sort_value=primary)
      toggle_url = sort_url(sort_value=toggle) if is_sorted else primary_url
      remove_url = sort_url(sort_value=remove) if is_sorted else None
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
        "sortable": sortable,
        "sorted": is_sorted,
        "ascending": ascending if is_sorted else None,
        "sort_priority": position if len(sort_fields) > 1 else None,
      }
    )
  return tuple(headers)


def _column_sort_values(field_name, field, *, sort_fields, position, ascending):
  others = [part for part in sort_fields if part.removeprefix("-") != field_name]
  if position is None:
    first = f"-{field_name}" if field["default_desc"] else field_name
    return ".".join([first, *others]), None, None
  toggled = field_name if not ascending else f"-{field_name}"
  toggled_fields = list(sort_fields)
  toggled_fields[position - 1] = toggled
  return ".".join([toggled, *others]), ".".join(toggled_fields), ".".join(others) or None


def queue_result_count_text(*, page_obj, total_count):
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
