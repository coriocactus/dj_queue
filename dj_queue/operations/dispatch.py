from dataclasses import dataclass, field
from datetime import timedelta
from enum import StrEnum

from django.utils import timezone

from dj_queue.db import get_database_alias
from dj_queue.models import BlockedExecution, Job, ReadyExecution, ScheduledExecution
from dj_queue.operations._helpers import (
  _create_blocked_execution,
  _create_ready_execution_locked,
  _create_scheduled_execution,
  _finish_job_if_no_execution_state,
  _ready_execution_row,
  _scheduled_execution_row,
)
from dj_queue.operations.concurrency import (
  concurrency_settings_for_job,
  semaphore_acquire,
  semaphore_acquire_many,
)


class DispatchOutcome(StrEnum):
  READY = "ready"
  SCHEDULED = "scheduled"
  BLOCKED = "blocked"
  DISCARDED = "discarded"

  @property
  def should_notify(self):
    return self is DispatchOutcome.READY


@dataclass(frozen=True, slots=True)
class DispatchDecision:
  outcome: DispatchOutcome | None
  concurrency_key: str | None = None
  limit: int | None = None
  duration_seconds: int | None = None
  on_conflict: str | None = None


@dataclass(slots=True)
class DispatchEntry:
  job: Job
  decision: DispatchDecision
  outcome: DispatchOutcome | None = None


@dataclass(slots=True)
class DispatchRows:
  ready: list[ReadyExecution] = field(default_factory=list)
  scheduled: list[ScheduledExecution] = field(default_factory=list)
  blocked: list[BlockedExecution] = field(default_factory=list)
  discarded: list[Job] = field(default_factory=list)

  @property
  def ready_queue_names(self):
    return tuple(dict.fromkeys(row.queue_name for row in self.ready))


def build_dispatch_rows(entries, *, backend_alias, now, config=None):
  """Acquire slots and prepare rows inside the caller's transaction."""
  rows = DispatchRows()
  groups = {}
  for entry in entries:
    if entry.decision.outcome is None:
      groups.setdefault(entry.decision, []).append(entry)
      continue
    entry.outcome = entry.decision.outcome
    _append_dispatch_row(rows, entry, backend_alias=backend_alias, now=now)

  for decision, group in groups.items():
    acquired_count = semaphore_acquire_many(
      decision.concurrency_key,
      count=len(group),
      limit=decision.limit,
      duration_seconds=decision.duration_seconds,
      backend_alias=backend_alias,
      config=config,
    )
    for index, entry in enumerate(group):
      entry.outcome = _concurrency_dispatch_outcome(decision, acquired=index < acquired_count)
      _append_dispatch_row(rows, entry, backend_alias=backend_alias, now=now)
  return rows


def _append_dispatch_row(rows, entry, *, backend_alias, now):
  job = entry.job
  if entry.outcome is DispatchOutcome.SCHEDULED:
    rows.scheduled.append(
      _scheduled_execution_row(
        job, backend_alias=backend_alias, scheduled_at=job.scheduled_at, created_at=now
      )
    )
  elif entry.outcome is DispatchOutcome.READY:
    rows.ready.append(
      _ready_execution_row(job, backend_alias=backend_alias, ready_at=now, created_at=now)
    )
  elif entry.outcome is DispatchOutcome.BLOCKED:
    rows.blocked.append(
      BlockedExecution(
        job=job,
        backend_alias=backend_alias,
        queue_name=job.queue_name,
        priority=job.priority,
        concurrency_key=entry.decision.concurrency_key,
        expires_at=now + timedelta(seconds=entry.decision.duration_seconds),
        created_at=now,
      )
    )
  elif entry.outcome is DispatchOutcome.DISCARDED:
    job.finished_at = now
    job.return_value = None
    job.updated_at = now
    rows.discarded.append(job)
  else:
    raise ValueError(f"unexpected dispatch outcome: {entry.outcome}")


def dispatch_decision(job, *, now, task=None, config=None):
  if job.scheduled_at is not None and job.scheduled_at > now:
    return DispatchDecision(DispatchOutcome.SCHEDULED)
  if not job.concurrency_key:
    return DispatchDecision(DispatchOutcome.READY)
  limit, duration_seconds, on_conflict = concurrency_settings_for_job(
    job, task=task, config=config
  )
  return DispatchDecision(
    None,
    concurrency_key=job.concurrency_key,
    limit=limit,
    duration_seconds=duration_seconds,
    on_conflict=on_conflict,
  )


def _concurrency_dispatch_outcome(decision, *, acquired):
  if acquired:
    return DispatchOutcome.READY
  if decision.on_conflict == "discard":
    return DispatchOutcome.DISCARDED
  return DispatchOutcome.BLOCKED


def dispatch_job(job, *, backend_alias, now=None, check_conflicts=True, task=None, config=None):
  alias = get_database_alias(backend_alias, config=config)
  if now is None:
    now = timezone.now()
  decision = dispatch_decision(job, task=task, now=now, config=config)
  outcome = decision.outcome
  if outcome is None:
    outcome = _concurrency_dispatch_outcome(
      decision,
      acquired=semaphore_acquire(
        decision.concurrency_key,
        limit=decision.limit,
        duration_seconds=decision.duration_seconds,
        backend_alias=backend_alias,
        config=config,
      ),
    )

  if outcome is DispatchOutcome.SCHEDULED:
    _create_scheduled_execution(
      alias,
      job=job,
      backend_alias=backend_alias,
      scheduled_at=job.scheduled_at,
      check_conflicts=check_conflicts,
    )
  elif outcome is DispatchOutcome.READY:
    _create_ready_execution_locked(
      alias,
      job=job,
      backend_alias=backend_alias,
      queue_name=job.queue_name,
      ready_at=now,
      check_conflicts=check_conflicts,
    )
  elif outcome is DispatchOutcome.BLOCKED:
    _create_blocked_execution(
      alias,
      job=job,
      backend_alias=backend_alias,
      concurrency_key=decision.concurrency_key,
      expires_at=now + timedelta(seconds=decision.duration_seconds),
      check_conflicts=check_conflicts,
    )
  elif outcome is DispatchOutcome.DISCARDED:
    if check_conflicts:
      _finish_job_if_no_execution_state(alias, job, None, finished_at=now, include_claimed=True)
    else:
      job.finished_at = now
      job.return_value = None
      job.save(using=alias, update_fields=["finished_at", "return_value", "updated_at"])
  else:
    raise ValueError(f"unexpected dispatch outcome: {outcome}")
  return outcome
