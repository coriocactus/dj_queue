from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from django.db import transaction
from django.db.models import Case, IntegerField, Value, When
from django.utils import timezone

from dj_queue.config import load_backend_config
from dj_queue.db import (
  database_capabilities,
  get_database_alias,
  locked_queryset,
  retry_transient_database_errors,
)
from dj_queue.exceptions import EnqueueError
from dj_queue.log import event_logging_enabled, log_event
from dj_queue.models import ClaimedExecution, Job, Process, ReadyExecution
from dj_queue.operations._helpers import (
  _bulk_create,
  _consume_selected_rows,
  _exclude_active_pauses,
  _job_ids_with_other_execution_state,
  _lock_active_pauses,
)
from dj_queue.queue_selectors import (
  filter_by_queue_selectors,
  normalize_queue_selectors,
  queue_selector_condition,
  selectors_match_all,
)
from dj_queue.sql import backend_sql


@dataclass(frozen=True)
class ClaimedJob:
  job: Job
  claimed_at: datetime
  worker_ids: tuple[str, ...]
  process_id: int | None = None


def claim_ready_jobs(
  *,
  limit: int,
  queues: str | Sequence[str] | None = None,
  process: Process | None = None,
  backend_alias: str = "default",
  use_skip_locked: bool | None = None,
) -> list[ClaimedJob]:
  if limit <= 0:
    return []

  alias = get_database_alias(backend_alias)
  if use_skip_locked is None:
    use_skip_locked = load_backend_config(backend_alias).use_skip_locked

  claimed_jobs = retry_transient_database_errors(
    lambda: _claim_ready_jobs_once(
      limit=limit,
      queues=queues,
      process=process,
      backend_alias=backend_alias,
      use_skip_locked=use_skip_locked,
      alias=alias,
    )
  )

  if event_logging_enabled(backend_alias=backend_alias):
    for claimed_job in claimed_jobs:
      log_event(
        "job.claimed",
        backend_alias=backend_alias,
        job_id=str(claimed_job.job.id),
        queue_name=claimed_job.job.queue_name,
        priority=claimed_job.job.priority,
      )
  return claimed_jobs


def _claim_ready_jobs_once(
  *,
  limit,
  queues,
  process,
  backend_alias,
  use_skip_locked,
  alias,
):
  with transaction.atomic(using=alias):
    if process is not None and process.backend_alias != backend_alias:
      raise EnqueueError(f"process {process.name!r} belongs to backend {process.backend_alias!r}")

    claimed_insert_checks_conflicts = _claimed_insert_checks_conflicts(alias)
    queryset = (
      ReadyExecution.objects.using(alias).select_related("job").filter(backend_alias=backend_alias)
    )
    queryset = _exclude_active_pauses(queryset, alias, backend_alias)
    ready_rows = _select_ready_rows(
      queryset,
      limit=limit,
      queues=queues,
      use_skip_locked=use_skip_locked,
      alias=alias,
      backend_alias=backend_alias,
    )
    if not ready_rows:
      return []

    mismatched_row = next(
      (row for row in ready_rows if row.job.backend_alias != backend_alias), None
    )
    if mismatched_row is not None:
      raise EnqueueError(
        f"job {mismatched_row.job_id} belongs to backend {mismatched_row.job.backend_alias!r}"
      )

    if not claimed_insert_checks_conflicts:
      conflicting_job_ids = _job_ids_with_other_execution_state(
        alias,
        [row.job_id for row in ready_rows],
        ignored_models=(ReadyExecution,),
      )
      if conflicting_job_ids:
        conflicting_job_id = next(iter(conflicting_job_ids))
        raise EnqueueError(f"job {conflicting_job_id} already has an execution-state row")

    paused_queue_names = _lock_active_pauses(
      alias,
      backend_alias,
      {row.queue_name for row in ready_rows},
    )
    if paused_queue_names:
      ready_rows = [row for row in ready_rows if row.queue_name not in paused_queue_names]
      if not ready_rows:
        return []

    jobs = [row.job for row in ready_rows]

    claimed_at = timezone.now()
    worker_ids = (process.name,) if process is not None else ()
    if claimed_insert_checks_conflicts:
      created_job_ids = backend_sql(alias).consume_ready_and_create_claimed_executions(
        alias,
        ready_rows,
        process=process,
        claimed_at=claimed_at,
      )
      if len(created_job_ids) != len(ready_rows):
        job_ids = [row.job_id for row in ready_rows]
        conflicting_job_ids = _job_ids_with_other_execution_state(alias, job_ids)
        if conflicting_job_ids:
          conflicting_job_id = next(iter(conflicting_job_ids))
          raise EnqueueError(f"job {conflicting_job_id} already has an execution-state row")
        raise EnqueueError("could not claim selected jobs")
    else:
      ready_rows = _consume_selected_rows(alias, ReadyExecution, ready_rows)
      if not ready_rows:
        return []
      jobs = [row.job for row in ready_rows]
      _create_claimed_executions(
        alias,
        jobs,
        process=process,
        claimed_at=claimed_at,
      )

  process_id = process.pk if process is not None else None
  return [
    ClaimedJob(job=job, claimed_at=claimed_at, worker_ids=worker_ids, process_id=process_id)
    for job in jobs
  ]


def _filter_queue_selectors(queryset, queues):
  return filter_by_queue_selectors(queryset, queues)


def _select_ready_rows(queryset, *, limit, queues, use_skip_locked, alias, backend_alias):
  if selectors_match_all(queues):
    ordered = queryset.order_by("-priority", "id")
    return list(locked_queryset(ordered, use_skip_locked=use_skip_locked)[:limit])

  selectors = normalize_queue_selectors(queues)
  selected_rows = []
  selected_ids = set()

  star_index = selectors.index("*") if "*" in selectors else None
  ordered_selectors = selectors if star_index is None else selectors[:star_index]

  if ordered_selectors:
    if _selectors_are_exact_queues(ordered_selectors):
      rows = _select_exact_queue_rows(
        queryset,
        ordered_selectors,
        limit=limit,
        use_skip_locked=use_skip_locked,
        selected_ids=selected_ids,
        alias=alias,
        backend_alias=backend_alias,
      )
    else:
      ordered = _ordered_selector_rows_queryset(
        queryset.exclude(pk__in=selected_ids),
        ordered_selectors,
      )
      rows = list(locked_queryset(ordered, use_skip_locked=use_skip_locked)[:limit])
    selected_rows.extend(rows)
    selected_ids.update(row.pk for row in rows)

  remaining = limit - len(selected_rows)
  if remaining <= 0 or star_index is None:
    return selected_rows

  ordered = queryset.exclude(pk__in=selected_ids).order_by("-priority", "id")
  rows = list(locked_queryset(ordered, use_skip_locked=use_skip_locked)[:remaining])
  selected_rows.extend(rows)
  return selected_rows


def _selectors_are_exact_queues(selectors):
  return all(not selector.endswith("*") for selector in selectors)


def _select_exact_queue_rows(
  queryset,
  selectors,
  *,
  limit,
  use_skip_locked,
  selected_ids,
  alias,
  backend_alias,
):
  if not selected_ids:
    select_ready_rows = getattr(backend_sql(alias), "select_ready_rows_by_exact_queues", None)
    if select_ready_rows is not None:
      return select_ready_rows(
        alias,
        backend_alias=backend_alias,
        selectors=selectors,
        limit=limit,
        use_skip_locked=use_skip_locked,
      )

  selected_rows = []
  for selector in selectors:
    remaining = limit - len(selected_rows)
    if remaining <= 0:
      break
    ordered = (
      queryset.exclude(pk__in=selected_ids).filter(queue_name=selector).order_by("-priority", "id")
    )
    rows = list(locked_queryset(ordered, use_skip_locked=use_skip_locked)[:remaining])
    selected_rows.extend(rows)
    selected_ids.update(row.pk for row in rows)
  return selected_rows


def _claimed_insert_checks_conflicts(alias):
  return database_capabilities(alias).backend_family == "postgresql"


def _create_claimed_executions(alias, jobs, *, process, claimed_at):
  process_id = process.id if process is not None else None
  return _bulk_create(
    alias,
    ClaimedExecution,
    [
      ClaimedExecution(job_id=job.id, process_id=process_id, created_at=claimed_at) for job in jobs
    ],
  )


def _ordered_selector_rows_queryset(queryset, selectors):
  filtered = _filter_queue_selectors(queryset, selectors)
  selector_rank = Case(
    *[
      When(queue_selector_condition((selector,)), then=Value(index))
      for index, selector in enumerate(selectors)
    ],
    default=Value(len(selectors)),
    output_field=IntegerField(),
  )
  return filtered.annotate(selector_rank=selector_rank).order_by(
    "selector_rank", "-priority", "id"
  )
