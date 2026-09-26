from django.utils import timezone

from dj_queue.db import get_database_alias
from dj_queue.models import (
  Pause,
  RecurringTask,
)
from dj_queue.queue_selectors import queue_matches_selectors
from dj_queue.queue_state import (
  empty_queue_state_summary,
  queue_state_summaries_by_queue,
  queue_state_summary,
)
from dj_queue.reads.processes import _live_processes_for_backend, process_cutoff_for_backend

_NOT_PROVIDED = object()


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
