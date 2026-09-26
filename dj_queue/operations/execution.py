import traceback
from copy import copy
from uuid import UUID

from django.db import transaction
from django.tasks import TaskContext
from django.utils import timezone
from django.utils.module_loading import import_string

from dj_queue.config import resolve_backend_config
from dj_queue.db import (
  database_capabilities,
  get_database_alias,
  retry_transient_database_errors,
)
from dj_queue.exceptions import EnqueueError, exception_path
from dj_queue.log import event_logging_enabled, log_event
from dj_queue.models import (
  ClaimedExecution,
  FailedExecution,
  Job,
)
from dj_queue.operations._helpers import (
  _ensure_no_other_execution_state,
  _finish_job_if_no_execution_state,
  _normalize_json_round_trip,
)
from dj_queue.operations.claiming import ClaimedJob
from dj_queue.operations.concurrency import release_concurrency_slot
from dj_queue.sql import backend_sql
from dj_queue.task_results import task_result_for_claimed_job


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

      release_concurrency_slot(job, task=task, config=config)
    return completed

  completed = retry_transient_database_errors(complete_transition, using=alias)
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
        exception_class=exception_path(error),
        message=str(error),
        traceback=traceback_text,
      )

      release_concurrency_slot(job, task=task, config=config)

  retry_transient_database_errors(fail_transition, using=alias)
  if event_logging_enabled(backend_alias=backend_alias):
    log_event(
      "job.failed",
      backend_alias=backend_alias,
      job_id=str(job.id),
      failure_kind=failure_kind,
      exception_class=exception_path(error),
      message=str(error),
    )
  return job


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
