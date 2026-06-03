import inspect
import time
import traceback
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from functools import lru_cache

from django.db import transaction
from django.db.models import Case, IntegerField, Value, When
from django.db.utils import OperationalError
from django.tasks import TaskContext
from django.utils import timezone
from django.utils.module_loading import import_string

from dj_queue.config import load_allowed_queues, load_backend_config
from dj_queue.db import database_capabilities, get_database_alias, locked_queryset
from dj_queue.exceptions import EnqueueError
from dj_queue.log import event_logging_enabled, log_event
from dj_queue.models import (
  BlockedExecution,
  ClaimedExecution,
  FailedExecution,
  Job,
  Process,
  ReadyExecution,
  Semaphore,
  ScheduledExecution,
)
from dj_queue.operations._helpers import (
  _bulk_create,
  _bulk_create_ready_executions_locked,
  _consume_selected_rows,
  _create_blocked_execution,
  _create_ready_execution_locked,
  _create_scheduled_execution,
  _ensure_job_ids_have_no_other_execution_state,
  _ensure_no_other_execution_state,
  _ensure_state_rows_belong_to_backend,
  _exclude_active_pauses,
  _job_ids_with_other_execution_state,
  _lock_active_pauses,
  _normalize_json_round_trip,
  _normalize_payload,
  _ready_execution_rows,
  _ready_execution_row,
  _scheduled_execution_row,
  _task_option,
)
from dj_queue.operations.concurrency import (
  SlotHandoffMode,
  claim_next_blocked_job,
  concurrency_settings,
  release_recovered_concurrency_slots,
  semaphore_acquire,
  semaphore_acquire_many,
  semaphore_release,
  unblock_next_blocked_job,
)
from dj_queue.queue_selectors import (
  filter_by_queue_selectors,
  normalize_queue_selectors,
  queue_selector_condition,
  selectors_match_all,
)
from dj_queue.sql import backend_sql
from dj_queue.sql import common as sql_common
from dj_queue.task_results import task_result_for_claimed_job
from dj_queue.wakeup import notify_ready_queues_on_commit


CLAIM_READY_JOBS_RETRY_ATTEMPTS = 3
CLAIM_READY_JOBS_RETRY_SLEEP_BASE = 0.01
TRANSIENT_CLAIM_ERROR_SQLSTATES = {
  "40001",  # serialization failure
  "40P01",  # deadlock detected
  "55P03",  # lock not available
}
TRANSIENT_CLAIM_ERROR_ERRNOS = {
  1205,  # mysql lock wait timeout
  1213,  # mysql deadlock
}
TRANSIENT_CLAIM_SQLITE_CODES = {
  5,  # SQLITE_BUSY
  6,  # SQLITE_LOCKED
  261,  # SQLITE_BUSY_RECOVERY
  262,  # SQLITE_LOCKED_SHAREDCACHE
  517,  # SQLITE_BUSY_SNAPSHOT
}
TRANSIENT_CLAIM_ERROR_MESSAGES = (
  "deadlock",
  "lock wait timeout",
  "try restarting transaction",
  "could not serialize access",
  "database is locked",
)


class DispatchOutcome(StrEnum):
  READY = "ready"
  SCHEDULED = "scheduled"
  BLOCKED = "blocked"
  DISCARDED = "discarded"

  @property
  def should_notify(self):
    return self is DispatchOutcome.READY


@dataclass(frozen=True)
class ClaimedJob:
  job: Job
  claimed_at: object
  worker_ids: tuple[str, ...]
  process_id: object = None


@dataclass(frozen=True)
class ExecutionOutcome:
  job: Job
  next_claimed_job: ClaimedJob | None = None


def enqueue_job(task, args, kwargs, *, backend_alias="default"):
  job, _ = enqueue_job_with_dispatch(task, args, kwargs, backend_alias=backend_alias)
  return job


def enqueue_job_with_dispatch(task, args, kwargs, *, backend_alias="default", validate=True):
  if validate:
    validate_queue_allowed(task.queue_name, backend_alias=backend_alias)
    validate_priority(task.priority)
  alias = get_database_alias(backend_alias)
  payload = _normalize_payload(args, kwargs)
  concurrency_key = _resolve_concurrency_key(task, args, kwargs)

  with transaction.atomic(using=alias):
    job = Job.objects.using(alias).create(
      task_path=task.module_path,
      queue_name=task.queue_name,
      priority=task.priority,
      payload=payload,
      backend_alias=backend_alias,
      scheduled_at=task.run_after,
      concurrency_key=concurrency_key,
    )
    dispatch_outcome = _dispatch_job(
      job,
      task=task,
      backend_alias=backend_alias,
      check_conflicts=False,
    )

  if dispatch_outcome.should_notify:
    notify_ready_queues_on_commit((job.queue_name,), backend_alias=backend_alias)

  if event_logging_enabled(backend_alias=backend_alias):
    log_event(
      "job.enqueued",
      backend_alias=backend_alias,
      job_id=str(job.id),
      task_path=job.task_path,
      queue_name=job.queue_name,
      priority=job.priority,
    )
  return job, dispatch_outcome


def enqueue_jobs_bulk(task_calls, *, backend_alias="default", validate=True):
  alias = get_database_alias(backend_alias)
  now = timezone.now()
  prepared = []

  for task, args, kwargs in task_calls:
    if validate:
      validate_queue_allowed(task.queue_name, backend_alias=backend_alias)
      validate_priority(task.priority)
    payload = _normalize_payload(args, kwargs)
    concurrency_key = _resolve_concurrency_key(task, args, kwargs)
    prepared.append(
      {
        "task": task,
        "job": Job(
          task_path=task.module_path,
          queue_name=task.queue_name,
          priority=task.priority,
          payload=payload,
          backend_alias=backend_alias,
          scheduled_at=task.run_after,
          concurrency_key=concurrency_key,
          created_at=now,
          updated_at=now,
        ),
      }
    )

  if not prepared:
    return []

  if all(
    entry["job"].scheduled_at is None and not entry["job"].concurrency_key for entry in prepared
  ):
    with transaction.atomic(using=alias):
      jobs = [entry["job"] for entry in prepared]
      _bulk_create(alias, Job, jobs)
      _bulk_create_ready_executions_locked(
        alias,
        [
          _ready_execution_row(
            job=job,
            backend_alias=backend_alias,
            created_at=job.created_at,
            ready_at=job.created_at,
          )
          for job in jobs
        ],
        backend_alias=backend_alias,
        check_conflicts=False,
      )

    ready_queue_names = tuple(dict.fromkeys(job.queue_name for job in jobs))
    if ready_queue_names:
      notify_ready_queues_on_commit(ready_queue_names, backend_alias=backend_alias)

    _log_bulk_enqueued(
      (DispatchOutcome.READY for _entry in prepared),
      backend_alias=backend_alias,
    )

    return [(entry["job"], entry["task"], DispatchOutcome.READY) for entry in prepared]

  ready_rows = []
  scheduled_rows = []
  blocked_rows = []
  discarded_jobs = []
  concurrency_entries = []
  ready_queue_names = []

  with transaction.atomic(using=alias):
    jobs = [entry["job"] for entry in prepared]
    _bulk_create(alias, Job, jobs)

    for entry in prepared:
      job = entry["job"]
      if job.scheduled_at is not None and job.scheduled_at > now:
        scheduled_rows.append(
          _scheduled_execution_row(
            job=job,
            backend_alias=backend_alias,
            scheduled_at=job.scheduled_at,
            created_at=job.created_at,
          )
        )
        entry["dispatch_outcome"] = DispatchOutcome.SCHEDULED
        continue

      if not job.concurrency_key:
        ready_rows.append(
          _ready_execution_row(
            job=job,
            backend_alias=backend_alias,
            created_at=job.created_at,
            ready_at=job.created_at,
          )
        )
        ready_queue_names.append(job.queue_name)
        entry["dispatch_outcome"] = DispatchOutcome.READY
        continue

      concurrency_entries.append(entry)

    _dispatch_bulk_concurrency_entries(
      concurrency_entries,
      ready_rows=ready_rows,
      blocked_rows=blocked_rows,
      discarded_jobs=discarded_jobs,
      ready_queue_names=ready_queue_names,
      backend_alias=backend_alias,
      now=now,
    )

    _bulk_create_ready_executions_locked(
      alias,
      ready_rows,
      backend_alias=backend_alias,
      check_conflicts=False,
    )
    _bulk_create(alias, ScheduledExecution, scheduled_rows)
    _bulk_create(alias, BlockedExecution, blocked_rows)
    if discarded_jobs:
      Job.objects.using(alias).bulk_update(
        discarded_jobs,
        ["finished_at", "return_value", "updated_at"],
      )

  if ready_queue_names:
    notify_ready_queues_on_commit(
      tuple(dict.fromkeys(ready_queue_names)),
      backend_alias=backend_alias,
    )

  _log_bulk_enqueued(
    (entry["dispatch_outcome"] for entry in prepared),
    backend_alias=backend_alias,
  )

  return [(entry["job"], entry["task"], entry["dispatch_outcome"]) for entry in prepared]


def _log_bulk_enqueued(outcomes, *, backend_alias):
  if not event_logging_enabled(backend_alias=backend_alias):
    return
  counts = {outcome: 0 for outcome in DispatchOutcome}
  job_count = 0
  for outcome in outcomes:
    counts[outcome] += 1
    job_count += 1
  log_event(
    "jobs.enqueued",
    backend_alias=backend_alias,
    job_count=job_count,
    ready_count=counts[DispatchOutcome.READY],
    scheduled_count=counts[DispatchOutcome.SCHEDULED],
    blocked_count=counts[DispatchOutcome.BLOCKED],
    discarded_count=counts[DispatchOutcome.DISCARDED],
  )


def _dispatch_bulk_concurrency_entries(
  entries,
  *,
  ready_rows,
  blocked_rows,
  discarded_jobs,
  ready_queue_names,
  backend_alias,
  now,
):
  groups = {}
  for entry in entries:
    job = entry["job"]
    limit, duration_seconds, on_conflict = concurrency_settings(
      entry["task"], backend_alias=backend_alias
    )
    groups.setdefault(
      (job.concurrency_key, limit, duration_seconds, on_conflict),
      [],
    ).append(entry)

  for (concurrency_key, limit, duration_seconds, on_conflict), group in groups.items():
    acquired_count = semaphore_acquire_many(
      concurrency_key,
      count=len(group),
      limit=limit,
      duration_seconds=duration_seconds,
      backend_alias=backend_alias,
    )
    for index, entry in enumerate(group):
      job = entry["job"]
      if index < acquired_count:
        ready_rows.append(
          _ready_execution_row(
            job=job,
            backend_alias=backend_alias,
            created_at=job.created_at,
            ready_at=now,
          )
        )
        ready_queue_names.append(job.queue_name)
        entry["dispatch_outcome"] = DispatchOutcome.READY
        continue

      if on_conflict == "discard":
        job.finished_at = now
        job.return_value = None
        job.updated_at = now
        discarded_jobs.append(job)
        entry["dispatch_outcome"] = DispatchOutcome.DISCARDED
        continue

      blocked_rows.append(
        BlockedExecution(
          job=job,
          backend_alias=backend_alias,
          queue_name=job.queue_name,
          priority=job.priority,
          concurrency_key=concurrency_key,
          expires_at=now + timedelta(seconds=duration_seconds),
        )
      )
      entry["dispatch_outcome"] = DispatchOutcome.BLOCKED


def claim_ready_jobs(
  *,
  limit,
  queues=None,
  process=None,
  backend_alias="default",
  use_skip_locked=None,
):
  if limit <= 0:
    return []

  alias = get_database_alias(backend_alias)
  if use_skip_locked is None:
    use_skip_locked = load_backend_config(backend_alias).use_skip_locked

  for attempt in range(CLAIM_READY_JOBS_RETRY_ATTEMPTS):
    try:
      claimed_jobs = _claim_ready_jobs_once(
        limit=limit,
        queues=queues,
        process=process,
        backend_alias=backend_alias,
        use_skip_locked=use_skip_locked,
        alias=alias,
      )
      break
    except OperationalError as error:
      if attempt == CLAIM_READY_JOBS_RETRY_ATTEMPTS - 1 or not _is_transient_claim_error(error):
        raise
      time.sleep(CLAIM_READY_JOBS_RETRY_SLEEP_BASE * (attempt + 1))

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


def execute_claimed_job(job, *, backend_alias="default"):
  while True:
    outcome = _execute_claimed_job_once(job, backend_alias=backend_alias)
    if not isinstance(outcome, ExecutionOutcome):
      return outcome
    if outcome.next_claimed_job is None:
      return outcome.job
    job = outcome.next_claimed_job
    backend_alias = job.job.backend_alias


def _execute_claimed_job_once(job, *, backend_alias="default"):
  claimed_job = None
  if isinstance(job, ClaimedJob):
    claimed_job = job
    job = claimed_job.job
  elif not isinstance(job, Job):
    claimed_job = _load_claimed_job(job, backend_alias=backend_alias)
    job = claimed_job.job

  task = None
  try:
    task = import_string(job.task_path)
    args = list(job.payload.get("args", []))
    kwargs = dict(job.payload.get("kwargs", {}))
  except Exception as exc:
    return _execution_failure_outcome(job, exc, task=task, failure_kind="task_import")

  try:
    return_value = _call_task(task, claimed_job, job, args, kwargs)
  except Exception as exc:
    return _execution_failure_outcome(job, exc, task=task, failure_kind="task_execution")

  try:
    return_value = _normalize_return_value(return_value)
  except ValueError as exc:
    return _execution_failure_outcome(
      job,
      exc,
      task=task,
      failure_kind="result_serialization",
    )

  completion_job = (
    claimed_job if claimed_job is not None and claimed_job.process_id is not None else job
  )
  return _complete_claimed_job(
    completion_job,
    return_value,
    backend_alias=job.backend_alias,
    task=task,
  )


def _call_task(task, claimed_job, job, args, kwargs):
  if task.takes_context:
    if claimed_job is None:
      claimed_job = _load_claimed_job(job.id, backend_alias=job.backend_alias)
    if not isinstance(claimed_job, ClaimedJob):
      raise RuntimeError("ClaimedJob is required for task context execution")
    context = TaskContext(task_result=task_result_for_claimed_job(task, claimed_job))
    return task.call(context, *args, **kwargs)
  return task.call(*args, **kwargs)


def _execution_failure_outcome(job, error, *, task, failure_kind):
  failed_job = _fail_claimed_job(
    job,
    error,
    traceback_text=traceback.format_exc(),
    backend_alias=job.backend_alias,
    task=task,
    failure_kind=failure_kind,
  )
  return ExecutionOutcome(job=failed_job)


def complete_claimed_job(job, return_value, *, backend_alias="default"):
  return _complete_claimed_job(job, return_value, backend_alias=backend_alias).job


def _complete_claimed_job(job, return_value, *, backend_alias="default", task=None):
  alias = get_database_alias(backend_alias)
  claimed_job = job if isinstance(job, ClaimedJob) else None
  process_id = claimed_job.process_id if claimed_job is not None else None
  worker_ids = claimed_job.worker_ids if claimed_job is not None else ()
  if isinstance(job, ClaimedJob):
    job = job.job
  job = _resolve_claimed_job(job, alias=alias, backend_alias=backend_alias)

  with transaction.atomic(using=alias):
    now = timezone.now()
    config = load_backend_config(job.backend_alias)

    if config.preserve_finished_jobs:
      if database_capabilities(alias).backend_family == "postgresql":
        _delete_claimed_and_finish_job_if_no_execution_state(
          alias,
          job,
          return_value,
          finished_at=now,
        )
      else:
        _delete_claimed_execution(alias, job.id)
        _finish_job_if_no_execution_state(alias, job, return_value, finished_at=now)
    else:
      _delete_claimed_execution(alias, job.id)
      _ensure_no_other_execution_state(alias, job, ignored_models=(ClaimedExecution,))
      job.delete(using=alias)

    next_claimed_job = _release_concurrency_slot(
      job,
      task=task,
      process_id=process_id,
      worker_ids=worker_ids,
    )
  if event_logging_enabled(backend_alias=backend_alias):
    log_event(
      "job.executed",
      backend_alias=backend_alias,
      job_id=str(job.id),
      status="success",
    )
  return ExecutionOutcome(job=job, next_claimed_job=next_claimed_job)


def fail_claimed_job(job, error, *, traceback_text="", backend_alias="default"):
  return _fail_claimed_job(
    job,
    error,
    traceback_text=traceback_text,
    backend_alias=backend_alias,
  )


def _fail_claimed_job(
  job,
  error,
  *,
  traceback_text="",
  backend_alias="default",
  task=None,
  failure_kind="runtime",
):
  alias = get_database_alias(backend_alias)
  if isinstance(job, ClaimedJob):
    job = job.job
  job = _resolve_claimed_job(job, alias=alias, backend_alias=backend_alias)

  with transaction.atomic(using=alias):
    _delete_claimed_execution(alias, job.id)
    _ensure_no_other_execution_state(alias, job, ignored_models=(ClaimedExecution,))
    FailedExecution.objects.using(alias).create(
      job_id=job.id,
      exception_class=_exception_path(error),
      message=str(error),
      traceback=traceback_text,
    )

    _release_concurrency_slot(job, task=task)
  if event_logging_enabled(backend_alias=backend_alias):
    log_event(
      "job.failed",
      backend_alias=backend_alias,
      job_id=str(job.id),
      failure_kind=failure_kind,
      exception_class=_exception_path(error),
      message=str(error),
    )
  return job


def fail_orphaned_claimed_jobs(error, *, traceback_text="", backend_alias="default"):
  alias = get_database_alias(backend_alias)
  jobs = [
    claimed.job
    for claimed in (
      ClaimedExecution.objects.using(alias)
      .select_related("job")
      .filter(process__isnull=True, job__backend_alias=backend_alias)
    )
  ]
  return _fail_claimed_jobs(
    jobs,
    error,
    traceback_text=traceback_text,
    backend_alias=backend_alias,
  )


def fail_claimed_jobs_for_process(
  process,
  error,
  *,
  traceback_text="",
  backend_alias="default",
  delete_process=False,
):
  if process is None:
    return []

  alias = get_database_alias(backend_alias)
  jobs = [
    claimed.job
    for claimed in (
      ClaimedExecution.objects.using(alias).select_related("job").filter(process_id=process.id)
    )
  ]
  failed_jobs = _fail_claimed_jobs(
    jobs,
    error,
    traceback_text=traceback_text,
    backend_alias=backend_alias,
  )
  if delete_process:
    process.delete(using=alias)
  return failed_jobs


def fail_claimed_jobs_for_pid(pid, error, *, traceback_text="", backend_alias="default"):
  alias = get_database_alias(backend_alias)
  processes = list(Process.objects.using(alias).filter(pid=pid, backend_alias=backend_alias)[:2])
  if len(processes) != 1:
    return []
  process = processes[0]
  return fail_claimed_jobs_for_process(
    process,
    error,
    traceback_text=traceback_text,
    backend_alias=backend_alias,
    delete_process=True,
  )


def fail_claimed_jobs_for_child(
  *,
  pid,
  name,
  supervisor_id,
  error,
  traceback_text="",
  backend_alias="default",
):
  alias = get_database_alias(backend_alias)
  process = (
    Process.objects.using(alias)
    .filter(
      pid=pid,
      name=name,
      supervisor_id=supervisor_id,
      backend_alias=backend_alias,
    )
    .first()
  )
  return fail_claimed_jobs_for_process(
    process,
    error,
    traceback_text=traceback_text,
    backend_alias=backend_alias,
    delete_process=True,
  )


def prune_stale_processes(
  *,
  cutoff,
  error,
  traceback_text="",
  backend_alias="default",
  exclude_process=None,
):
  alias = get_database_alias(backend_alias)
  config = load_backend_config(backend_alias)
  pruned_processes = []

  with transaction.atomic(using=alias):
    queryset = Process.objects.using(alias).filter(
      backend_alias=backend_alias,
      last_heartbeat_at__lt=cutoff,
    )
    if exclude_process is not None:
      queryset = queryset.exclude(pk=exclude_process.pk)

    stale_processes = list(
      locked_queryset(
        queryset.order_by("last_heartbeat_at", "id"),
        use_skip_locked=config.use_skip_locked,
      )
    )
    if not stale_processes:
      return []

    for process in stale_processes:
      jobs = [
        claimed.job
        for claimed in (
          ClaimedExecution.objects.using(alias).select_related("job").filter(process_id=process.id)
        )
      ]
      deleted, _ = (
        Process.objects.using(alias)
        .filter(
          pk=process.pk,
          backend_alias=backend_alias,
          last_heartbeat_at__lt=cutoff,
        )
        .delete()
      )
      if not deleted:
        continue

      _fail_claimed_jobs(
        jobs,
        error,
        traceback_text=traceback_text,
        backend_alias=backend_alias,
      )
      pruned_processes.append(process)
    return pruned_processes


def promote_scheduled_jobs(*, batch_size, backend_alias="default", use_skip_locked=None):
  alias = get_database_alias(backend_alias)
  if use_skip_locked is None:
    use_skip_locked = load_backend_config(backend_alias).use_skip_locked
  now = timezone.now()
  ready_queue_names = []

  with transaction.atomic(using=alias):
    queryset = (
      ScheduledExecution.objects.using(alias)
      .select_related("job")
      .filter(backend_alias=backend_alias, scheduled_at__lte=now)
      .order_by("scheduled_at", "-priority", "id")
    )
    scheduled_rows = list(locked_queryset(queryset, use_skip_locked=use_skip_locked)[:batch_size])
    if not scheduled_rows:
      return []
    _ensure_state_rows_belong_to_backend(scheduled_rows, backend_alias)

    scheduled_rows = _consume_selected_rows(alias, ScheduledExecution, scheduled_rows)
    if not scheduled_rows:
      return []

    jobs = [row.job for row in scheduled_rows]

    direct_jobs = [job for job in jobs if not job.concurrency_key]
    if direct_jobs:
      _bulk_create_ready_executions_locked(
        alias,
        _ready_execution_rows(
          direct_jobs,
          backend_alias=backend_alias,
          ready_at=now,
          created_at=now,
        ),
        backend_alias=backend_alias,
        check_conflicts=True,
      )
      ready_queue_names.extend(job.queue_name for job in direct_jobs)

    direct_job_ids = {job.pk for job in direct_jobs}
    for job in jobs:
      if job.pk in direct_job_ids:
        continue
      dispatch_outcome = _dispatch_existing_job(job)
      if dispatch_outcome.should_notify:
        ready_queue_names.append(job.queue_name)

  if ready_queue_names:
    notify_ready_queues_on_commit(
      tuple(dict.fromkeys(ready_queue_names)), backend_alias=backend_alias
    )
  return jobs


def dispatch_scheduled_job_now(job_id, *, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  config = load_backend_config(backend_alias)

  with transaction.atomic(using=alias):
    scheduled = locked_queryset(
      ScheduledExecution.objects.using(alias)
      .select_related("job")
      .filter(job_id=job_id, backend_alias=backend_alias),
      use_skip_locked=config.use_skip_locked,
    ).first()
    if scheduled is None:
      raise EnqueueError("job is not scheduled")
    _ensure_state_rows_belong_to_backend([scheduled], backend_alias)
    scheduled_rows = _consume_selected_rows(alias, ScheduledExecution, [scheduled])
    if not scheduled_rows:
      raise EnqueueError("job is not scheduled")

    job = scheduled.job
    job.scheduled_at = None
    job.save(using=alias, update_fields=["scheduled_at", "updated_at"])
    dispatch_outcome = _dispatch_existing_job(job)

  if dispatch_outcome.should_notify:
    notify_ready_queues_on_commit((job.queue_name,), backend_alias=backend_alias)

  if event_logging_enabled(backend_alias=backend_alias):
    log_event(
      "job.dispatched_now",
      backend_alias=backend_alias,
      job_id=str(job.id),
      queue_name=job.queue_name,
      priority=job.priority,
      dispatched_as=dispatch_outcome.value,
    )
  return job, dispatch_outcome


def retry_failed_job(job_id, *, backend_alias="default"):
  alias = get_database_alias(backend_alias)

  with transaction.atomic(using=alias):
    failed = (
      FailedExecution.objects.using(alias)
      .select_for_update()
      .select_related("job")
      .get(job_id=job_id, job__backend_alias=backend_alias)
    )
    failed_rows = _consume_selected_rows(alias, FailedExecution, [failed])
    if not failed_rows:
      raise EnqueueError("job is not failed")
    job = failed.job
    job.return_value = None
    job.finished_at = None
    job.save(using=alias, update_fields=["return_value", "finished_at", "updated_at"])
    dispatch_outcome = _dispatch_existing_job(job)

  if dispatch_outcome.should_notify:
    notify_ready_queues_on_commit((job.queue_name,), backend_alias=backend_alias)

  if event_logging_enabled(backend_alias=backend_alias):
    log_event(
      "job.retried",
      backend_alias=backend_alias,
      job_id=str(job.id),
      queue_name=job.queue_name,
      priority=job.priority,
    )
  return job


def retry_failed_jobs(*, job_ids=None, batch_size=500, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  config = load_backend_config(backend_alias)

  with transaction.atomic(using=alias):
    queryset = (
      FailedExecution.objects.using(alias).filter(job__backend_alias=backend_alias).order_by("id")
    )
    if job_ids is not None:
      queryset = queryset.filter(job_id__in=job_ids)
    failed_rows = list(
      locked_queryset(
        queryset.select_related("job"),
        use_skip_locked=config.use_skip_locked,
      )[:batch_size]
    )
    if not failed_rows:
      return 0

    failed_rows = _consume_selected_rows(alias, FailedExecution, failed_rows)
    if not failed_rows:
      return 0

    jobs = []
    ready_queue_names = []
    for failed in failed_rows:
      job = failed.job
      job.return_value = None
      job.finished_at = None
      job.save(using=alias, update_fields=["return_value", "finished_at", "updated_at"])
      dispatch_outcome = _dispatch_existing_job(job)
      jobs.append(job)
      if dispatch_outcome.should_notify:
        ready_queue_names.append(job.queue_name)

  if ready_queue_names:
    notify_ready_queues_on_commit(
      tuple(dict.fromkeys(ready_queue_names)),
      backend_alias=backend_alias,
    )

  if event_logging_enabled(backend_alias=backend_alias):
    for job in jobs:
      log_event(
        "job.retried",
        backend_alias=backend_alias,
        job_id=str(job.id),
        queue_name=job.queue_name,
        priority=job.priority,
      )
  return len(jobs)


_KEEP_RUN_AFTER = object()


def enqueue_job_again(job_id, *, backend_alias="default", run_after=_KEEP_RUN_AFTER):
  alias = get_database_alias(backend_alias)
  source_job = Job.objects.using(alias).get(pk=job_id, backend_alias=backend_alias)
  task = import_string(source_job.task_path)
  source_run_after = source_job.scheduled_at if run_after is _KEEP_RUN_AFTER else run_after
  if hasattr(task, "using"):
    task = task.using(
      priority=source_job.priority,
      queue_name=source_job.queue_name,
      run_after=source_run_after,
      backend=source_job.backend_alias,
    )
  args = list(source_job.payload.get("args", []))
  kwargs = dict(source_job.payload.get("kwargs", {}))
  job, _ = enqueue_job_with_dispatch(task, args, kwargs, backend_alias=source_job.backend_alias)
  return job


def _discard_state_jobs(
  model,
  reason,
  *,
  job_ids=None,
  batch_size=500,
  backend_alias="default",
  release_concurrency=False,
  queue_name=None,
):
  alias = get_database_alias(backend_alias)
  config = load_backend_config(backend_alias)

  with transaction.atomic(using=alias):
    if model is FailedExecution:
      queryset = (
        model.objects.using(alias)
        .select_related("job")
        .filter(job__backend_alias=backend_alias)
        .order_by("id")
      )
    else:
      queryset = (
        model.objects.using(alias)
        .select_related("job")
        .filter(backend_alias=backend_alias)
        .order_by("id")
      )

    if job_ids is not None:
      queryset = queryset.filter(job_id__in=job_ids)
    if queue_name is not None:
      queryset = queryset.filter(queue_name=queue_name)
    rows = list(locked_queryset(queryset, use_skip_locked=config.use_skip_locked)[:batch_size])
    if not rows:
      return 0
    _ensure_state_rows_belong_to_backend(rows, backend_alias)

    rows = _consume_selected_rows(alias, model, rows)
    if not rows:
      return 0

    row_job_ids = [row.job_id for row in rows]
    _ensure_job_ids_have_no_other_execution_state(alias, row_job_ids)
    jobs_by_id = {job.id: job for job in Job.objects.using(alias).filter(pk__in=row_job_ids)}
    jobs = [jobs_by_id[job_id] for job_id in row_job_ids]
    Job.objects.using(alias).filter(pk__in=row_job_ids).delete()

    if release_concurrency:
      for job in jobs:
        _release_concurrency_slot(job)

  should_log = event_logging_enabled(backend_alias=backend_alias)
  for job in jobs:
    if should_log:
      log_event(
        "job.discarded",
        backend_alias=backend_alias,
        job_id=str(job.id),
        reason=reason,
      )
  return len(jobs)


def discard_failed_jobs(*, job_ids=None, batch_size=500, backend_alias="default"):
  return _discard_state_jobs(
    FailedExecution,
    "failed",
    job_ids=job_ids,
    batch_size=batch_size,
    backend_alias=backend_alias,
  )


def discard_failed_job(job_id, *, backend_alias="default"):
  return discard_failed_jobs(job_ids=[job_id], batch_size=1, backend_alias=backend_alias)


def discard_ready_jobs(*, job_ids=None, batch_size=500, backend_alias="default"):
  return _discard_state_jobs(
    ReadyExecution,
    "ready",
    job_ids=job_ids,
    batch_size=batch_size,
    backend_alias=backend_alias,
    release_concurrency=True,
  )


def discard_ready_jobs_for_queue(queue_name, *, batch_size=500, backend_alias="default"):
  return _discard_state_jobs(
    ReadyExecution,
    "ready",
    batch_size=batch_size,
    backend_alias=backend_alias,
    release_concurrency=True,
    queue_name=queue_name,
  )


def discard_scheduled_jobs(*, job_ids=None, batch_size=500, backend_alias="default"):
  return _discard_state_jobs(
    ScheduledExecution,
    "scheduled",
    job_ids=job_ids,
    batch_size=batch_size,
    backend_alias=backend_alias,
  )


def discard_blocked_jobs(*, job_ids=None, batch_size=500, backend_alias="default"):
  return _discard_state_jobs(
    BlockedExecution,
    "blocked",
    job_ids=job_ids,
    batch_size=batch_size,
    backend_alias=backend_alias,
  )


def _dispatch_existing_job(job, *, check_conflicts=True):
  task = import_string(job.task_path)
  return _dispatch_job(
    job, task=task, backend_alias=job.backend_alias, check_conflicts=check_conflicts
  )


def _dispatch_job(job, *, task, backend_alias, now=None, check_conflicts=True):
  alias = get_database_alias(backend_alias)
  if now is None:
    now = timezone.now()

  if job.scheduled_at is not None and job.scheduled_at > now:
    _create_scheduled_execution(
      alias,
      job=job,
      backend_alias=backend_alias,
      scheduled_at=job.scheduled_at,
      check_conflicts=check_conflicts,
    )
    return DispatchOutcome.SCHEDULED

  if not job.concurrency_key:
    _create_ready_execution_locked(
      alias,
      job=job,
      backend_alias=backend_alias,
      queue_name=job.queue_name,
      ready_at=now,
      check_conflicts=check_conflicts,
    )
    return DispatchOutcome.READY

  limit, duration_seconds, on_conflict = concurrency_settings(task, backend_alias=backend_alias)
  if semaphore_acquire(
    job.concurrency_key,
    limit=limit,
    duration_seconds=duration_seconds,
    backend_alias=backend_alias,
  ):
    _create_ready_execution_locked(
      alias,
      job=job,
      backend_alias=backend_alias,
      queue_name=job.queue_name,
      ready_at=now,
      check_conflicts=check_conflicts,
    )
    return DispatchOutcome.READY

  if on_conflict == "discard":
    if check_conflicts:
      _finish_job_if_no_execution_state(alias, job, None, finished_at=now, include_claimed=True)
    else:
      job.finished_at = now
      job.return_value = None
      job.save(using=alias, update_fields=["finished_at", "return_value", "updated_at"])
    return DispatchOutcome.DISCARDED

  _create_blocked_execution(
    alias,
    job=job,
    backend_alias=backend_alias,
    concurrency_key=job.concurrency_key,
    expires_at=now + timedelta(seconds=duration_seconds),
    check_conflicts=check_conflicts,
  )
  return DispatchOutcome.BLOCKED


def _release_concurrency_slot(job, *, task=None, process_id=None, worker_ids=()):
  if not job.concurrency_key:
    return None

  alias = get_database_alias(job.backend_alias)
  config = load_backend_config(job.backend_alias)
  try:
    if task is None:
      task = import_string(job.task_path)
    limit, duration_seconds, _ = concurrency_settings(task, backend_alias=job.backend_alias)
  except (AttributeError, EnqueueError, ImportError):
    limit = _semaphore_limit(job) or 1
    duration_seconds = config.default_concurrency_duration

  if limit == 1 and process_id is not None:
    handoff = claim_next_blocked_job(
      job.concurrency_key,
      limit=limit,
      duration_seconds=duration_seconds,
      process_id=process_id,
      backend_alias=job.backend_alias,
      use_skip_locked=config.use_skip_locked,
    )
    if handoff is not None:
      handed_job = Job.objects.using(alias).get(pk=handoff.job_id, backend_alias=job.backend_alias)
      return ClaimedJob(
        job=handed_job,
        claimed_at=handoff.claimed_at,
        worker_ids=worker_ids,
        process_id=process_id,
      )

  if (
    unblock_next_blocked_job(
      job.concurrency_key,
      limit=limit,
      duration_seconds=duration_seconds,
      backend_alias=job.backend_alias,
      use_skip_locked=config.use_skip_locked,
      slot_handoff=SlotHandoffMode.RELEASE_CLAIMED,
    )
    is not None
  ):
    return None

  semaphore_release(
    job.concurrency_key,
    limit=limit,
    duration_seconds=duration_seconds,
    backend_alias=job.backend_alias,
  )
  unblock_next_blocked_job(
    job.concurrency_key,
    limit=limit,
    duration_seconds=duration_seconds,
    backend_alias=job.backend_alias,
    use_skip_locked=config.use_skip_locked,
    slot_handoff=SlotHandoffMode.CONSUME_RELEASED,
  )
  return None


def _semaphore_limit(job):
  alias = get_database_alias(job.backend_alias)
  return (
    Semaphore.objects.using(alias)
    .filter(key=job.concurrency_key)
    .values_list("limit", flat=True)
    .first()
  )


def validate_queue_allowed(queue_name, *, backend_alias="default"):
  allowed_queues = load_allowed_queues(backend_alias)
  if allowed_queues and queue_name not in allowed_queues:
    raise EnqueueError(f"queue {queue_name!r} is not allowed for backend {backend_alias!r}")


def validate_priority(priority):
  if type(priority) is not int or priority < -100 or priority > 100:
    raise EnqueueError("priority must be an integer from -100 to 100")


def _resolve_concurrency_key(task, args, kwargs):
  option = _task_option(task, "concurrency_key")
  if option in (None, ""):
    return None

  if callable(option):
    value = option(*args, **kwargs)
  elif isinstance(option, str):
    try:
      value = option.format(**_bound_arguments(task, args, kwargs))
    except (IndexError, KeyError, ValueError) as exc:
      raise EnqueueError("could not resolve concurrency_key") from exc
  else:
    raise EnqueueError("concurrency_key must be a string or callable")

  if not isinstance(value, str) or not value or len(value) > 255:
    raise EnqueueError("concurrency_key must resolve to a non-empty string up to 255 chars")
  return value


def _bound_arguments(task, args, kwargs):
  signature = _task_call_signature(task.func, task.takes_context)
  bound = signature.bind(*args, **kwargs)
  bound.apply_defaults()
  return bound.arguments


@lru_cache(maxsize=1024)
def _task_call_signature(func, takes_context):
  signature = inspect.signature(func)
  parameters = tuple(signature.parameters.values())
  if takes_context and parameters:
    signature = signature.replace(parameters=parameters[1:])
  return signature


def _filter_queue_selectors(queryset, queues):
  return filter_by_queue_selectors(queryset, queues)


def _select_ready_rows(queryset, *, limit, queues, use_skip_locked):
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


def _select_exact_queue_rows(queryset, selectors, *, limit, use_skip_locked, selected_ids):
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


def _is_transient_claim_error(error):
  for candidate in _exception_chain(error):
    if _transient_sqlstate(candidate) or _transient_errno(candidate):
      return True
  message = str(error).lower()
  return any(marker in message for marker in TRANSIENT_CLAIM_ERROR_MESSAGES)


def _exception_chain(error):
  seen = set()
  while error is not None and id(error) not in seen:
    seen.add(id(error))
    yield error
    error = error.__cause__ or error.__context__


def _transient_sqlstate(error):
  for name in ("pgcode", "sqlstate"):
    value = getattr(error, name, None)
    if value in TRANSIENT_CLAIM_ERROR_SQLSTATES:
      return True
  return False


def _transient_errno(error):
  sqlite_code = getattr(error, "sqlite_errorcode", None)
  if sqlite_code in TRANSIENT_CLAIM_SQLITE_CODES:
    return True
  errno = getattr(error, "errno", None)
  if errno in TRANSIENT_CLAIM_ERROR_ERRNOS:
    return True
  if error.args and error.args[0] in TRANSIENT_CLAIM_ERROR_ERRNOS:
    return True
  return False


def _normalize_return_value(return_value):
  return _normalize_json_round_trip(
    return_value,
    exception_class=ValueError,
    message="return value must be JSON round-trippable",
  )


def _load_claimed_job(job_id, *, backend_alias):
  alias = get_database_alias(backend_alias)
  claimed = (
    ClaimedExecution.objects.using(alias)
    .select_related("job", "process")
    .get(job_id=job_id, job__backend_alias=backend_alias)
  )
  return ClaimedJob(
    job=claimed.job,
    claimed_at=claimed.created_at,
    worker_ids=(claimed.process.name,) if claimed.process is not None else (),
    process_id=claimed.process_id,
  )


def _resolve_claimed_job(job, *, alias, backend_alias):
  if isinstance(job, Job):
    if job.backend_alias != backend_alias:
      raise ClaimedExecution.DoesNotExist
    return job

  try:
    return Job.objects.using(alias).get(pk=job, backend_alias=backend_alias)
  except Job.DoesNotExist as exc:
    raise ClaimedExecution.DoesNotExist from exc


def _delete_claimed_execution(alias, job_id):
  deleted, _ = ClaimedExecution.objects.using(alias).filter(job_id=job_id).delete()
  if not deleted:
    raise ClaimedExecution.DoesNotExist


def _delete_claimed_and_finish_job_if_no_execution_state(alias, job, return_value, *, finished_at):
  deleted_count, updated_count = backend_sql(
    alias
  ).delete_claimed_and_finish_job_if_no_execution_state(
    alias,
    job,
    return_value,
    finished_at=finished_at,
  )
  if deleted_count != 1:
    raise ClaimedExecution.DoesNotExist
  if updated_count != 1:
    raise EnqueueError(f"job {job.id} already has an execution-state row")
  job.finished_at = finished_at
  job.return_value = return_value
  job.updated_at = finished_at


def _finish_job_if_no_execution_state(
  alias, job, return_value, *, finished_at, include_claimed=False
):
  updated = sql_common.finish_job_if_no_execution_state(
    alias,
    job,
    return_value,
    finished_at=finished_at,
    include_claimed=include_claimed,
  )
  if updated != 1:
    raise EnqueueError(f"job {job.id} already has an execution-state row")
  job.finished_at = finished_at
  job.return_value = return_value
  job.updated_at = finished_at


def _fail_claimed_jobs(jobs, error, *, traceback_text, backend_alias):
  jobs = list(jobs)
  if not jobs:
    return []
  for job in jobs:
    if job.backend_alias != backend_alias:
      raise ClaimedExecution.DoesNotExist

  alias = get_database_alias(backend_alias)
  job_ids = [job.id for job in jobs]
  exception_class = _exception_path(error)
  message = str(error)

  with transaction.atomic(using=alias):
    deleted, _ = ClaimedExecution.objects.using(alias).filter(job_id__in=job_ids).delete()
    if deleted != len(job_ids):
      raise ClaimedExecution.DoesNotExist
    _ensure_job_ids_have_no_other_execution_state(
      alias,
      job_ids,
      ignored_models=(ClaimedExecution,),
    )
    FailedExecution.objects.using(alias).bulk_create(
      [
        FailedExecution(
          job_id=job.id,
          exception_class=exception_class,
          message=message,
          traceback=traceback_text,
        )
        for job in jobs
      ]
    )

  for job in release_recovered_concurrency_slots(jobs, backend_alias=backend_alias):
    _release_concurrency_slot(job)

  if event_logging_enabled(backend_alias=backend_alias):
    for job in jobs:
      log_event(
        "job.failed",
        backend_alias=backend_alias,
        job_id=str(job.id),
        exception_class=exception_class,
        message=message,
      )
  return jobs


def _exception_path(error):
  return f"{error.__class__.__module__}.{error.__class__.__qualname__}"
