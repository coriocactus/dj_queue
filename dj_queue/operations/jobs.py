from collections.abc import Iterable
from datetime import datetime
from uuid import UUID

from django.db import transaction
from django.utils import timezone
from django.utils.module_loading import import_string

from dj_queue.config import resolve_backend_config
from dj_queue.db import (
  get_database_alias,
  locked_queryset,
  retry_transient_database_errors,
)
from dj_queue.exceptions import DispatchPolicyError, EnqueueError, exception_path
from dj_queue.log import event_logging_enabled, log_event
from dj_queue.models import (
  BlockedExecution,
  FailedExecution,
  Job,
  ReadyExecution,
  ScheduledExecution,
)
from dj_queue.operations._helpers import (
  _bulk_create,
  _bulk_create_ready_executions_locked,
  _consume_selected_rows,
  _ensure_job_ids_have_no_other_execution_state,
  _ensure_no_other_execution_state,
  _ensure_state_rows_belong_to_backend,
  _ready_execution_rows,
)
from dj_queue.operations.claiming import ClaimedJob, claim_ready_jobs
from dj_queue.operations.concurrency import release_concurrency_slot
from dj_queue.operations.dispatch import (
  DispatchEntry,
  DispatchOutcome,
  build_dispatch_rows,
  dispatch_decision,
  dispatch_job,
)
from dj_queue.operations.enqueue import (
  enqueue_job,
  enqueue_job_with_dispatch,
  enqueue_jobs_bulk,
  validate_priority,
  validate_queue_allowed,
)
from dj_queue.operations.execution import (
  complete_claimed_job,
  execute_claimed_job,
  fail_claimed_job,
)
from dj_queue.operations.recovery import (
  fail_claimed_jobs_for_child,
  fail_claimed_jobs_for_pid,
  fail_claimed_jobs_for_process,
  fail_orphaned_claimed_jobs,
  prune_stale_processes,
)
from dj_queue.wakeup import notify_ready_queues_on_commit

__all__ = [
  "ClaimedJob",
  "DispatchOutcome",
  "claim_ready_jobs",
  "complete_claimed_job",
  "discard_blocked_jobs",
  "discard_failed_job",
  "discard_failed_jobs",
  "discard_ready_jobs",
  "discard_ready_jobs_for_queue",
  "discard_scheduled_jobs",
  "dispatch_scheduled_job_now",
  "enqueue_job",
  "enqueue_job_again",
  "enqueue_job_with_dispatch",
  "enqueue_jobs_bulk",
  "execute_claimed_job",
  "fail_claimed_job",
  "fail_claimed_jobs_for_child",
  "fail_claimed_jobs_for_pid",
  "fail_claimed_jobs_for_process",
  "fail_orphaned_claimed_jobs",
  "promote_failed_job_retries",
  "promote_scheduled_jobs",
  "prune_stale_processes",
  "retry_failed_job",
  "retry_failed_jobs",
  "schedule_failed_job_retry",
  "validate_priority",
  "validate_queue_allowed",
]


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
          dispatch_outcome = dispatch_job(job, backend_alias=backend_alias, config=config)
        except DispatchPolicyError as error:
          _record_dispatch_policy_failure(alias, job, error)
          policy_failures.append((job, error))
          continue
        promoted_jobs.append(job)
        if dispatch_outcome.should_notify:
          ready_queue_names.append(job.queue_name)
    return promoted_jobs, ready_queue_names, policy_failures

  promoted_jobs, ready_queue_names, policy_failures = retry_transient_database_errors(
    promote_transition, using=alias
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
    dispatch_outcome = dispatch_job(job, backend_alias=backend_alias, config=config)

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

  job, ready_queue_names = retry_transient_database_errors(retry_transition, using=alias)

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

  jobs, ready_queue_names, policy_failures = retry_transient_database_errors(
    retry_transition, using=alias
  )

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

  jobs, ready_queue_names, policy_failures = retry_transient_database_errors(
    promote_transition, using=alias
  )

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
    try:
      decision = dispatch_decision(job, now=now, config=config)
    except DispatchPolicyError as error:
      if not isolate_policy_errors:
        raise
      policy_failures.append((job, error))
      continue
    prepared.append(DispatchEntry(job=job, decision=decision))

  rows = build_dispatch_rows(prepared, backend_alias=backend_alias, now=now, config=config)

  Job.objects.using(alias).bulk_update(
    failed_jobs,
    ["return_value", "finished_at", "updated_at"],
  )
  _bulk_create_ready_executions_locked(
    alias, rows.ready, backend_alias=backend_alias, check_conflicts=False
  )
  _bulk_create(alias, ScheduledExecution, rows.scheduled)
  _bulk_create(alias, BlockedExecution, rows.blocked)
  _bulk_create(
    alias,
    FailedExecution,
    [
      FailedExecution(
        job_id=job.id,
        exception_class=exception_path(error),
        message=str(error),
        traceback="",
      )
      for job, error in policy_failures
    ],
  )

  return [entry.job for entry in prepared], rows.ready_queue_names, policy_failures


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
    exception_class=exception_path(error),
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
      exception_class=exception_path(error),
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
        release_concurrency_slot(job, config=config)

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
