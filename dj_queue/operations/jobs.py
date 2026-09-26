import inspect
import traceback
from collections.abc import Iterable
from copy import copy
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from functools import lru_cache
from uuid import UUID, uuid4

from django.db import transaction
from django.tasks import TaskContext
from django.utils import timezone
from django.utils.module_loading import import_string

from dj_queue.config import load_allowed_queues, resolve_backend_config
from dj_queue.db import (
  database_capabilities,
  get_database_alias,
  locked_queryset,
  retry_transient_database_errors,
)
from dj_queue.exceptions import DispatchPolicyError, EnqueueError
from dj_queue.log import event_logging_enabled, log_event
from dj_queue.models import (
  BlockedExecution,
  ClaimedExecution,
  FailedExecution,
  Job,
  Process,
  ReadyExecution,
  ScheduledExecution,
  Semaphore,
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
  _normalize_json_round_trip,
  _normalize_payload,
  _ready_execution_row,
  _ready_execution_rows,
  _scheduled_execution_row,
  _task_option,
)
from dj_queue.operations.claiming import ClaimedJob, claim_ready_jobs  # noqa: F401
from dj_queue.operations.concurrency import (
  SlotHandoffMode,
  concurrency_settings,
  concurrency_settings_for_job,
  release_recovered_concurrency_slots,
  semaphore_acquire,
  semaphore_acquire_many,
  semaphore_release,
  unblock_next_blocked_job,
)
from dj_queue.sql import backend_sql
from dj_queue.sql import common as sql_common
from dj_queue.task_results import task_result_for_claimed_job
from dj_queue.wakeup import notify_ready_queues_on_commit


class DispatchOutcome(StrEnum):
  READY = "ready"
  SCHEDULED = "scheduled"
  BLOCKED = "blocked"
  DISCARDED = "discarded"

  @property
  def should_notify(self):
    return self is DispatchOutcome.READY


@dataclass(frozen=True, slots=True)
class _DispatchDecision:
  outcome: DispatchOutcome | None
  concurrency_key: str | None = None
  limit: int | None = None
  duration_seconds: int | None = None
  on_conflict: str | None = None


@dataclass(frozen=True, slots=True)
class _JobSubmission:
  task: object
  fields: dict


@dataclass(slots=True)
class _PreparedJob:
  task: object
  job: Job
  dispatch_decision: _DispatchDecision | None = None
  dispatch_outcome: DispatchOutcome | None = None


def enqueue_job(task, args, kwargs, *, backend_alias="default", job_id=None, config=None):
  job, _ = enqueue_job_with_dispatch(
    task,
    args,
    kwargs,
    backend_alias=backend_alias,
    job_id=job_id,
    config=config,
  )
  return job


def enqueue_job_with_dispatch(
  task,
  args,
  kwargs,
  *,
  backend_alias="default",
  validate=True,
  job_id=None,
  config=None,
):
  submission = _prepare_submission(
    task,
    args,
    kwargs,
    backend_alias=backend_alias,
    validate=validate,
    job_id=job_id,
    config=config,
  )
  alias = get_database_alias(backend_alias, config=config)

  def enqueue_transition():
    with transaction.atomic(using=alias):
      job = Job.objects.using(alias).create(**submission.fields)
      dispatch_outcome = _dispatch_job(
        job,
        task=task,
        backend_alias=backend_alias,
        check_conflicts=False,
        config=config,
      )
      return job, dispatch_outcome

  job, dispatch_outcome = retry_transient_database_errors(enqueue_transition)

  if dispatch_outcome.should_notify:
    notify_ready_queues_on_commit((job.queue_name,), backend_alias=backend_alias, config=config)

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


def _prepare_submission(task, args, kwargs, *, backend_alias, validate, job_id=None, config=None):
  if validate:
    validate_queue_allowed(task.queue_name, backend_alias=backend_alias, config=config)
    validate_priority(task.priority)
  payload = _normalize_payload(args, kwargs)
  concurrency_key = _resolve_concurrency_key(task, args, kwargs)
  limit, duration, on_conflict = _concurrency_policy(
    task, concurrency_key, backend_alias=backend_alias, config=config
  )
  return _JobSubmission(
    task,
    {
      "id": job_id if job_id is not None else uuid4(),
      "task_path": task.module_path,
      "queue_name": task.queue_name,
      "priority": task.priority,
      "payload": payload,
      "backend_alias": backend_alias,
      "scheduled_at": task.run_after,
      "concurrency_key": concurrency_key,
      "concurrency_limit": limit,
      "concurrency_duration": duration,
      "concurrency_on_conflict": on_conflict,
    },
  )


def enqueue_jobs_bulk(task_calls, *, backend_alias="default", validate=True, config=None):
  submissions = [
    _prepare_submission(
      task, args, kwargs, backend_alias=backend_alias, validate=validate, config=config
    )
    for task, args, kwargs in task_calls
  ]
  if not submissions:
    return []

  alias = get_database_alias(backend_alias, config=config)
  prepared, ready_queue_names = retry_transient_database_errors(
    lambda: _enqueue_bulk_once(
      submissions, alias=alias, backend_alias=backend_alias, config=config
    )
  )
  notify_ready_queues_on_commit(ready_queue_names, backend_alias=backend_alias, config=config)
  _log_bulk_enqueued((entry.dispatch_outcome for entry in prepared), backend_alias=backend_alias)
  return [(entry.job, entry.task, entry.dispatch_outcome) for entry in prepared]


def _enqueue_bulk_once(submissions, *, alias, backend_alias, config=None):
  now = timezone.now()
  prepared = []
  for submission in submissions:
    job = Job(**submission.fields, created_at=now, updated_at=now)
    prepared.append(
      _PreparedJob(
        task=submission.task,
        job=job,
        dispatch_decision=_dispatch_decision(
          job, backend_alias=backend_alias, now=now, config=config
        ),
      )
    )

  with transaction.atomic(using=alias):
    _bulk_create(alias, Job, [entry.job for entry in prepared])
    ready_rows, scheduled_rows, blocked_rows, discarded_jobs, ready_queue_names = (
      _bulk_dispatch_rows(prepared, backend_alias=backend_alias, now=now, config=config)
    )
    _bulk_create_ready_executions_locked(
      alias, ready_rows, backend_alias=backend_alias, check_conflicts=False
    )
    _bulk_create(alias, ScheduledExecution, scheduled_rows)
    _bulk_create(alias, BlockedExecution, blocked_rows)
    if discarded_jobs:
      Job.objects.using(alias).bulk_update(
        discarded_jobs, ["finished_at", "return_value", "updated_at"]
      )
  return prepared, tuple(dict.fromkeys(ready_queue_names))


def _bulk_dispatch_rows(prepared, *, backend_alias, now, config=None):
  ready_rows = []
  scheduled_rows = []
  blocked_rows = []
  discarded_jobs = []
  ready_queue_names = []
  concurrency_entries = []

  for entry in prepared:
    job = entry.job
    decision = entry.dispatch_decision
    if decision.outcome is DispatchOutcome.SCHEDULED:
      scheduled_rows.append(
        _scheduled_execution_row(
          job,
          backend_alias=backend_alias,
          scheduled_at=job.scheduled_at,
          created_at=now,
        )
      )
      entry.dispatch_outcome = DispatchOutcome.SCHEDULED
      continue
    if decision.outcome is DispatchOutcome.READY:
      ready_rows.append(
        _ready_execution_row(
          job,
          backend_alias=backend_alias,
          ready_at=now,
          created_at=now,
        )
      )
      ready_queue_names.append(job.queue_name)
      entry.dispatch_outcome = DispatchOutcome.READY
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
    config=config,
  )
  return ready_rows, scheduled_rows, blocked_rows, discarded_jobs, ready_queue_names


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
  config=None,
):
  groups = {}
  for entry in entries:
    decision = entry.dispatch_decision
    groups.setdefault(
      (decision.concurrency_key, decision.limit, decision.duration_seconds, decision.on_conflict),
      [],
    ).append(entry)

  for (concurrency_key, limit, duration_seconds, on_conflict), group in groups.items():
    acquired_count = semaphore_acquire_many(
      concurrency_key,
      count=len(group),
      limit=limit,
      duration_seconds=duration_seconds,
      backend_alias=backend_alias,
      config=config,
    )
    for index, entry in enumerate(group):
      job = entry.job
      dispatch_outcome = _concurrency_dispatch_outcome(
        entry.dispatch_decision,
        acquired=index < acquired_count,
      )
      if dispatch_outcome is DispatchOutcome.READY:
        ready_rows.append(
          _ready_execution_row(
            job=job,
            backend_alias=backend_alias,
            created_at=now,
            ready_at=now,
          )
        )
        ready_queue_names.append(job.queue_name)
        entry.dispatch_outcome = dispatch_outcome
        continue

      if dispatch_outcome is DispatchOutcome.DISCARDED:
        job.finished_at = now
        job.return_value = None
        job.updated_at = now
        discarded_jobs.append(job)
        entry.dispatch_outcome = dispatch_outcome
        continue

      blocked_rows.append(
        BlockedExecution(
          job=job,
          backend_alias=backend_alias,
          queue_name=job.queue_name,
          priority=job.priority,
          concurrency_key=concurrency_key,
          expires_at=now + timedelta(seconds=duration_seconds),
          created_at=now,
        )
      )
      entry.dispatch_outcome = dispatch_outcome


def execute_claimed_job(
  job: ClaimedJob | Job | UUID | str,
  *,
  backend_alias: str = "default",
  config=None,
) -> Job:
  claimed_job = None
  if isinstance(job, ClaimedJob):
    claimed_job = job
    job = claimed_job.job
  elif not isinstance(job, Job):
    claimed_job = _load_claimed_job(job, backend_alias=backend_alias, config=config)
    job = claimed_job.job

  config = resolve_backend_config(job.backend_alias, config)
  task = None
  try:
    task = import_string(job.task_path)
    args = list(job.payload.get("args", []))
    kwargs = dict(job.payload.get("kwargs", {}))
  except Exception as exc:
    return _execution_failure_outcome(
      job, exc, task=task, failure_kind="task_import", config=config
    )

  try:
    return_value = _call_task(task, claimed_job, job, args, kwargs, config=config)
  except Exception as exc:
    return _execution_failure_outcome(
      job, exc, task=task, failure_kind="task_execution", config=config
    )

  try:
    return_value = _normalize_return_value(return_value)
  except ValueError as exc:
    return _execution_failure_outcome(
      job,
      exc,
      task=task,
      failure_kind="result_serialization",
      config=config,
    )

  return _complete_claimed_job(
    job,
    return_value,
    backend_alias=job.backend_alias,
    task=task,
    config=config,
  )


def _call_task(task, claimed_job, job, args, kwargs, config=None):
  if task.takes_context:
    if claimed_job is None:
      claimed_job = _load_claimed_job(job.id, backend_alias=job.backend_alias, config=config)
    if not isinstance(claimed_job, ClaimedJob):
      raise RuntimeError("ClaimedJob is required for task context execution")
    context = TaskContext(task_result=task_result_for_claimed_job(task, claimed_job))
    return task.call(context, *args, **kwargs)
  return task.call(*args, **kwargs)


def _execution_failure_outcome(job, error, *, task, failure_kind, config=None):
  return _fail_claimed_job(
    job,
    error,
    traceback_text=traceback.format_exc(),
    backend_alias=job.backend_alias,
    task=task,
    failure_kind=failure_kind,
    config=config,
  )


def complete_claimed_job(job, return_value, *, backend_alias="default", config=None):
  return _complete_claimed_job(job, return_value, backend_alias=backend_alias, config=config)


def _complete_claimed_job(job, return_value, *, backend_alias="default", task=None, config=None):
  alias = get_database_alias(backend_alias, config=config)
  if isinstance(job, ClaimedJob):
    job = job.job
  job = _resolve_claimed_job(job, alias=alias, backend_alias=backend_alias)

  config = resolve_backend_config(job.backend_alias, config)

  def complete_transition():
    completed = copy(job)
    with transaction.atomic(using=alias):
      now = timezone.now()

      if config.preserve_finished_jobs:
        if database_capabilities(alias).backend_family == "postgresql":
          _delete_claimed_and_finish_job_if_no_execution_state(
            alias,
            completed,
            return_value,
            finished_at=now,
          )
        else:
          _delete_claimed_execution(alias, job.id)
          _finish_job_if_no_execution_state(alias, completed, return_value, finished_at=now)
      else:
        _delete_claimed_execution(alias, job.id)
        _ensure_no_other_execution_state(alias, job, ignored_models=(ClaimedExecution,))
        completed.delete(using=alias)

      _release_concurrency_slot(job, task=task, config=config)
    return completed

  completed = retry_transient_database_errors(complete_transition)
  if event_logging_enabled(backend_alias=backend_alias):
    log_event(
      "job.executed",
      backend_alias=backend_alias,
      job_id=str(job.id),
      status="success",
    )
  return completed


def fail_claimed_job(job, error, *, traceback_text="", backend_alias="default", config=None):
  return _fail_claimed_job(
    job,
    error,
    traceback_text=traceback_text,
    backend_alias=backend_alias,
    config=config,
  )


def _fail_claimed_job(
  job,
  error,
  *,
  traceback_text="",
  backend_alias="default",
  task=None,
  failure_kind="runtime",
  config=None,
):
  alias = get_database_alias(backend_alias, config=config)
  if isinstance(job, ClaimedJob):
    job = job.job
  job = _resolve_claimed_job(job, alias=alias, backend_alias=backend_alias)

  def fail_transition():
    with transaction.atomic(using=alias):
      _delete_claimed_execution(alias, job.id)
      _ensure_no_other_execution_state(alias, job, ignored_models=(ClaimedExecution,))
      FailedExecution.objects.using(alias).create(
        job_id=job.id,
        exception_class=_exception_path(error),
        message=str(error),
        traceback=traceback_text,
      )

      _release_concurrency_slot(job, task=task, config=config)

  retry_transient_database_errors(fail_transition)
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


def fail_orphaned_claimed_jobs(
  error,
  *,
  traceback_text="",
  backend_alias="default",
  batch_size=500,
  config=None,
):
  alias = get_database_alias(backend_alias, config=config)
  config = resolve_backend_config(backend_alias, config)
  with transaction.atomic(using=alias):
    claimed_rows = list(
      locked_queryset(
        ClaimedExecution.objects.using(alias)
        .select_related("job")
        .filter(process__isnull=True, job__backend_alias=backend_alias)
        .order_by("id"),
        use_skip_locked=config.use_skip_locked,
      )[:batch_size]
    )
    return _fail_claimed_jobs(
      [claimed.job for claimed in claimed_rows],
      error,
      traceback_text=traceback_text,
      backend_alias=backend_alias,
      config=config,
    )


def fail_claimed_jobs_for_process(
  process,
  error,
  *,
  traceback_text="",
  backend_alias="default",
  delete_process=False,
  config=None,
):
  if process is None:
    return []

  alias = get_database_alias(backend_alias, config=config)
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
    config=config,
  )
  if delete_process:
    process.delete(using=alias)
  return failed_jobs


def fail_claimed_jobs_for_pid(
  pid, error, *, traceback_text="", backend_alias="default", config=None
):
  alias = get_database_alias(backend_alias, config=config)
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
    config=config,
  )


def fail_claimed_jobs_for_child(
  *,
  pid,
  name,
  supervisor_id,
  error,
  traceback_text="",
  backend_alias="default",
  config=None,
):
  alias = get_database_alias(backend_alias, config=config)
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
    config=config,
  )


def prune_stale_processes(
  *,
  cutoff,
  error,
  traceback_text="",
  backend_alias="default",
  exclude_process=None,
  batch_size=500,
  config=None,
):
  alias = get_database_alias(backend_alias, config=config)
  config = resolve_backend_config(backend_alias, config)
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
      )[:batch_size]
    )
    if not stale_processes:
      return []

    remaining = batch_size
    for process in stale_processes:
      claimed_rows = list(
        ClaimedExecution.objects.using(alias)
        .select_related("job")
        .filter(process_id=process.id)
        .order_by("id")[:remaining]
      )
      jobs = [claimed.job for claimed in claimed_rows]
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
        config=config,
      )
      pruned_processes.append(process)
      remaining -= len(jobs)
      if remaining == 0:
        break
    return pruned_processes


def promote_scheduled_jobs(
  *, batch_size, backend_alias="default", use_skip_locked=None, config=None
):
  alias = get_database_alias(backend_alias, config=config)
  if use_skip_locked is None:
    use_skip_locked = resolve_backend_config(backend_alias, config).use_skip_locked

  def promote_transition():
    now = timezone.now()
    ready_queue_names = []
    policy_failures = []
    with transaction.atomic(using=alias):
      queryset = (
        ScheduledExecution.objects.using(alias)
        .select_related("job")
        .filter(backend_alias=backend_alias, scheduled_at__lte=now)
        .order_by("scheduled_at", "-priority", "id")
      )
      scheduled_rows = list(
        locked_queryset(queryset, use_skip_locked=use_skip_locked)[:batch_size]
      )
      if not scheduled_rows:
        return [], [], []
      _ensure_state_rows_belong_to_backend(scheduled_rows, backend_alias)

      scheduled_rows = _consume_selected_rows(alias, ScheduledExecution, scheduled_rows)
      if not scheduled_rows:
        return [], [], []

      jobs = [row.job for row in scheduled_rows]

      direct_jobs = [job for job in jobs if not job.concurrency_key]
      if direct_jobs:
        _bulk_create_ready_executions_locked(
          alias,
          _ready_execution_rows(
            sorted(direct_jobs, key=lambda job: job.pk),
            backend_alias=backend_alias,
            ready_at=now,
            created_at=now,
          ),
          backend_alias=backend_alias,
          check_conflicts=True,
        )
        ready_queue_names.extend(job.queue_name for job in direct_jobs)

      direct_job_ids = {job.pk for job in direct_jobs}
      promoted_jobs = list(direct_jobs)
      for job in jobs:
        if job.pk in direct_job_ids:
          continue
        try:
          dispatch_outcome = _dispatch_existing_job(job, config=config)
        except DispatchPolicyError as error:
          _record_dispatch_policy_failure(alias, job, error)
          policy_failures.append((job, error))
          continue
        promoted_jobs.append(job)
        if dispatch_outcome.should_notify:
          ready_queue_names.append(job.queue_name)
    return promoted_jobs, ready_queue_names, policy_failures

  promoted_jobs, ready_queue_names, policy_failures = retry_transient_database_errors(
    promote_transition
  )

  if ready_queue_names:
    notify_ready_queues_on_commit(
      tuple(dict.fromkeys(ready_queue_names)), backend_alias=backend_alias, config=config
    )
  _log_dispatch_policy_failures(policy_failures, backend_alias=backend_alias)
  return promoted_jobs


def dispatch_scheduled_job_now(job_id, *, backend_alias="default", config=None):
  alias = get_database_alias(backend_alias, config=config)
  config = resolve_backend_config(backend_alias, config)

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
    dispatch_outcome = _dispatch_existing_job(job, config=config)

  if dispatch_outcome.should_notify:
    notify_ready_queues_on_commit((job.queue_name,), backend_alias=backend_alias, config=config)

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


def schedule_failed_job_retry(
  job_id: UUID | str,
  *,
  retry_at: datetime,
  backend_alias: str = "default",
  config=None,
) -> Job:
  if retry_at is None:
    raise EnqueueError("retry_at is required")
  alias = get_database_alias(backend_alias, config=config)

  with transaction.atomic(using=alias):
    failed = (
      FailedExecution.objects.using(alias)
      .select_for_update()
      .select_related("job")
      .get(job_id=job_id, job__backend_alias=backend_alias)
    )
    failed.retry_at = retry_at
    failed.save(using=alias, update_fields=["retry_at"])
    return failed.job


def retry_failed_job(job_id: UUID | str, *, backend_alias: str = "default", config=None) -> Job:
  alias = get_database_alias(backend_alias, config=config)

  def retry_transition():
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
      jobs, ready_queue_names, _policy_failures = _dispatch_consumed_failed_rows(
        alias,
        failed_rows,
        backend_alias=backend_alias,
        config=config,
      )
      return jobs[0], ready_queue_names

  job, ready_queue_names = retry_transient_database_errors(retry_transition)

  if ready_queue_names:
    notify_ready_queues_on_commit(
      tuple(ready_queue_names), backend_alias=backend_alias, config=config
    )

  _log_jobs_retried((job,), backend_alias=backend_alias)
  return job


def retry_failed_jobs(
  *,
  job_ids: Iterable[UUID | str] | None = None,
  batch_size: int = 500,
  backend_alias: str = "default",
  config=None,
) -> int:
  alias = get_database_alias(backend_alias, config=config)
  config = resolve_backend_config(backend_alias, config)
  if job_ids is not None:
    job_ids = tuple(job_ids)

  def retry_transition():
    with transaction.atomic(using=alias):
      queryset = (
        FailedExecution.objects.using(alias)
        .filter(job__backend_alias=backend_alias)
        .order_by("id")
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
        return [], [], []

      failed_rows = _consume_selected_rows(alias, FailedExecution, failed_rows)
      if not failed_rows:
        return [], [], []

      return _dispatch_consumed_failed_rows(
        alias,
        failed_rows,
        backend_alias=backend_alias,
        isolate_policy_errors=True,
        config=config,
      )

  jobs, ready_queue_names, policy_failures = retry_transient_database_errors(retry_transition)

  if ready_queue_names:
    notify_ready_queues_on_commit(
      tuple(dict.fromkeys(ready_queue_names)),
      backend_alias=backend_alias,
      config=config,
    )

  _log_jobs_retried(jobs, backend_alias=backend_alias)
  _log_dispatch_policy_failures(policy_failures, backend_alias=backend_alias)
  return len(jobs)


def promote_failed_job_retries(
  *, batch_size, backend_alias="default", use_skip_locked=None, config=None
):
  alias = get_database_alias(backend_alias, config=config)
  if use_skip_locked is None:
    use_skip_locked = resolve_backend_config(backend_alias, config).use_skip_locked
  now = timezone.now()

  def promote_transition():
    with transaction.atomic(using=alias):
      queryset = (
        FailedExecution.objects.using(alias)
        .filter(job__backend_alias=backend_alias, retry_at__lte=now)
        .order_by("retry_at", "id")
      )
      failed_rows = list(
        locked_queryset(
          queryset.select_related("job"),
          use_skip_locked=use_skip_locked,
        )[:batch_size]
      )
      if not failed_rows:
        return [], [], []

      failed_rows = _consume_selected_rows(alias, FailedExecution, failed_rows)
      if not failed_rows:
        return [], [], []

      return _dispatch_consumed_failed_rows(
        alias,
        failed_rows,
        backend_alias=backend_alias,
        isolate_policy_errors=True,
        config=config,
      )

  jobs, ready_queue_names, policy_failures = retry_transient_database_errors(promote_transition)

  if ready_queue_names:
    notify_ready_queues_on_commit(
      tuple(dict.fromkeys(ready_queue_names)),
      backend_alias=backend_alias,
      config=config,
    )

  _log_jobs_retried(jobs, backend_alias=backend_alias)
  _log_dispatch_policy_failures(policy_failures, backend_alias=backend_alias)
  return jobs


def _dispatch_consumed_failed_rows(
  alias,
  failed_rows,
  *,
  backend_alias,
  isolate_policy_errors=False,
  config=None,
):
  now = timezone.now()
  failed_jobs = [failed.job for failed in failed_rows]
  _ensure_job_ids_have_no_other_execution_state(alias, [job.id for job in failed_jobs])

  prepared = []
  policy_failures = []
  for job in failed_jobs:
    job.return_value = None
    job.finished_at = None
    job.updated_at = now
    entry = _PreparedJob(task=None, job=job)
    try:
      entry.dispatch_decision = _dispatch_decision(
        job,
        backend_alias=backend_alias,
        now=now,
        config=config,
      )
    except DispatchPolicyError as error:
      if not isolate_policy_errors:
        raise
      policy_failures.append((job, error))
      continue
    prepared.append(entry)

  ready_rows, scheduled_rows, blocked_rows, _discarded_jobs, ready_queue_names = (
    _bulk_dispatch_rows(
      prepared,
      backend_alias=backend_alias,
      now=now,
      config=config,
    )
  )

  Job.objects.using(alias).bulk_update(
    failed_jobs,
    ["return_value", "finished_at", "updated_at"],
  )
  _bulk_create_ready_executions_locked(
    alias,
    ready_rows,
    backend_alias=backend_alias,
    check_conflicts=False,
  )
  _bulk_create(alias, ScheduledExecution, scheduled_rows)
  _bulk_create(alias, BlockedExecution, blocked_rows)
  _bulk_create(
    alias,
    FailedExecution,
    [
      FailedExecution(
        job_id=job.id,
        exception_class=_exception_path(error),
        message=str(error),
        traceback="",
      )
      for job, error in policy_failures
    ],
  )

  return [entry.job for entry in prepared], ready_queue_names, policy_failures


def _log_jobs_retried(jobs, *, backend_alias):
  if not event_logging_enabled(backend_alias=backend_alias):
    return
  for job in jobs:
    log_event(
      "job.retried",
      backend_alias=backend_alias,
      job_id=str(job.id),
      queue_name=job.queue_name,
      priority=job.priority,
    )


def _record_dispatch_policy_failure(alias, job, error):
  _ensure_no_other_execution_state(alias, job)
  FailedExecution.objects.using(alias).create(
    job_id=job.id,
    exception_class=_exception_path(error),
    message=str(error),
    traceback="",
  )


def _log_dispatch_policy_failures(failures, *, backend_alias):
  if not event_logging_enabled(backend_alias=backend_alias):
    return
  for job, error in failures:
    log_event(
      "job.failed",
      backend_alias=backend_alias,
      job_id=str(job.id),
      failure_kind="dispatch_policy",
      exception_class=_exception_path(error),
      message=str(error),
    )


_KEEP_RUN_AFTER = object()


def enqueue_job_again(job_id, *, backend_alias="default", run_after=_KEEP_RUN_AFTER, config=None):
  alias = get_database_alias(backend_alias, config=config)
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
  job, _ = enqueue_job_with_dispatch(
    task, args, kwargs, backend_alias=source_job.backend_alias, config=config
  )
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
  config=None,
):
  alias = get_database_alias(backend_alias, config=config)
  config = resolve_backend_config(backend_alias, config)

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
        _release_concurrency_slot(job, config=config)

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


def discard_failed_jobs(
  *,
  job_ids: Iterable[UUID | str] | None = None,
  batch_size: int = 500,
  backend_alias: str = "default",
  config=None,
) -> int:
  return _discard_state_jobs(
    FailedExecution,
    "failed",
    job_ids=job_ids,
    batch_size=batch_size,
    backend_alias=backend_alias,
    config=config,
  )


def discard_failed_job(job_id: UUID | str, *, backend_alias: str = "default", config=None) -> int:
  return discard_failed_jobs(
    job_ids=[job_id], batch_size=1, backend_alias=backend_alias, config=config
  )


def discard_ready_jobs(
  *,
  job_ids: Iterable[UUID | str] | None = None,
  batch_size: int = 500,
  backend_alias: str = "default",
  config=None,
) -> int:
  return _discard_state_jobs(
    ReadyExecution,
    "ready",
    job_ids=job_ids,
    batch_size=batch_size,
    backend_alias=backend_alias,
    release_concurrency=True,
    config=config,
  )


def discard_ready_jobs_for_queue(
  queue_name, *, batch_size=500, backend_alias="default", config=None
):
  return _discard_state_jobs(
    ReadyExecution,
    "ready",
    batch_size=batch_size,
    backend_alias=backend_alias,
    release_concurrency=True,
    queue_name=queue_name,
    config=config,
  )


def discard_scheduled_jobs(
  *,
  job_ids: Iterable[UUID | str] | None = None,
  batch_size: int = 500,
  backend_alias: str = "default",
  config=None,
) -> int:
  return _discard_state_jobs(
    ScheduledExecution,
    "scheduled",
    job_ids=job_ids,
    batch_size=batch_size,
    backend_alias=backend_alias,
    config=config,
  )


def discard_blocked_jobs(
  *,
  job_ids: Iterable[UUID | str] | None = None,
  batch_size: int = 500,
  backend_alias: str = "default",
  config=None,
) -> int:
  return _discard_state_jobs(
    BlockedExecution,
    "blocked",
    job_ids=job_ids,
    batch_size=batch_size,
    backend_alias=backend_alias,
    config=config,
  )


def _dispatch_existing_job(job, *, check_conflicts=True, config=None):
  return _dispatch_job(
    job, backend_alias=job.backend_alias, check_conflicts=check_conflicts, config=config
  )


def _dispatch_decision(job, *, backend_alias, now, task=None, config=None):
  if job.scheduled_at is not None and job.scheduled_at > now:
    return _DispatchDecision(DispatchOutcome.SCHEDULED)

  if not job.concurrency_key:
    return _DispatchDecision(DispatchOutcome.READY)

  limit, duration_seconds, on_conflict = concurrency_settings_for_job(
    job, task=task, config=config
  )
  return _DispatchDecision(
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


def _dispatch_job(job, *, backend_alias, now=None, check_conflicts=True, task=None, config=None):
  alias = get_database_alias(backend_alias, config=config)
  if now is None:
    now = timezone.now()
  decision = _dispatch_decision(
    job, task=task, backend_alias=backend_alias, now=now, config=config
  )

  if decision.outcome is DispatchOutcome.SCHEDULED:
    _create_scheduled_execution(
      alias,
      job=job,
      backend_alias=backend_alias,
      scheduled_at=job.scheduled_at,
      check_conflicts=check_conflicts,
    )
    return DispatchOutcome.SCHEDULED

  if decision.outcome is DispatchOutcome.READY:
    _create_ready_execution_locked(
      alias,
      job=job,
      backend_alias=backend_alias,
      queue_name=job.queue_name,
      ready_at=now,
      check_conflicts=check_conflicts,
    )
    return DispatchOutcome.READY

  dispatch_outcome = _concurrency_dispatch_outcome(
    decision,
    acquired=semaphore_acquire(
      decision.concurrency_key,
      limit=decision.limit,
      duration_seconds=decision.duration_seconds,
      backend_alias=backend_alias,
      config=config,
    ),
  )
  if dispatch_outcome is DispatchOutcome.READY:
    _create_ready_execution_locked(
      alias,
      job=job,
      backend_alias=backend_alias,
      queue_name=job.queue_name,
      ready_at=now,
      check_conflicts=check_conflicts,
    )
    return dispatch_outcome

  if dispatch_outcome is DispatchOutcome.DISCARDED:
    if check_conflicts:
      _finish_job_if_no_execution_state(alias, job, None, finished_at=now, include_claimed=True)
    else:
      job.finished_at = now
      job.return_value = None
      job.save(using=alias, update_fields=["finished_at", "return_value", "updated_at"])
    return dispatch_outcome

  _create_blocked_execution(
    alias,
    job=job,
    backend_alias=backend_alias,
    concurrency_key=decision.concurrency_key,
    expires_at=now + timedelta(seconds=decision.duration_seconds),
    check_conflicts=check_conflicts,
  )
  return dispatch_outcome


def _release_concurrency_slot(job, *, task=None, config=None):
  if not job.concurrency_key:
    return

  config = resolve_backend_config(job.backend_alias, config)
  try:
    limit, duration_seconds, _ = concurrency_settings_for_job(job, task=task, config=config)
  except DispatchPolicyError:
    limit = _semaphore_limit(job, config=config) or 1
    duration_seconds = config.default_concurrency_duration

  if (
    unblock_next_blocked_job(
      job.concurrency_key,
      limit=limit,
      duration_seconds=duration_seconds,
      backend_alias=job.backend_alias,
      use_skip_locked=config.use_skip_locked,
      slot_handoff=SlotHandoffMode.RELEASE_CLAIMED,
      config=config,
    )
    is not None
  ):
    return

  semaphore_release(
    job.concurrency_key,
    limit=limit,
    duration_seconds=duration_seconds,
    backend_alias=job.backend_alias,
    config=config,
  )
  unblock_next_blocked_job(
    job.concurrency_key,
    limit=limit,
    duration_seconds=duration_seconds,
    backend_alias=job.backend_alias,
    use_skip_locked=config.use_skip_locked,
    slot_handoff=SlotHandoffMode.CONSUME_RELEASED,
    config=config,
  )


def _semaphore_limit(job, config=None):
  alias = get_database_alias(job.backend_alias, config=config)
  return (
    Semaphore.objects.using(alias)
    .filter(key=job.concurrency_key)
    .values_list("limit", flat=True)
    .first()
  )


def validate_queue_allowed(queue_name, *, backend_alias="default", config=None):
  allowed_queues = load_allowed_queues(backend_alias) if config is None else config.allowed_queues
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


def _concurrency_policy(task, concurrency_key, *, backend_alias, config=None):
  if concurrency_key is None:
    return None, None, None
  return concurrency_settings(task, backend_alias=backend_alias, config=config)


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


def _normalize_return_value(return_value):
  return _normalize_json_round_trip(
    return_value,
    exception_class=ValueError,
    message="return value must be JSON round-trippable",
  )


def _load_claimed_job(job_id, *, backend_alias, config=None):
  alias = get_database_alias(backend_alias, config=config)
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


def _fail_claimed_jobs(jobs, error, *, traceback_text, backend_alias, config=None):
  jobs = list(jobs)
  if not jobs:
    return []
  for job in jobs:
    if job.backend_alias != backend_alias:
      raise ClaimedExecution.DoesNotExist

  alias = get_database_alias(backend_alias, config=config)
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

    for job in release_recovered_concurrency_slots(
      jobs, backend_alias=backend_alias, config=config
    ):
      _release_concurrency_slot(job, config=config)

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
