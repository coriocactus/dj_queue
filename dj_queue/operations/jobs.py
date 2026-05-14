import inspect
import json
import time
import traceback
from dataclasses import dataclass
from datetime import timedelta

from django.db import connections, transaction
from django.db.models import Q
from django.db.utils import OperationalError
from django.tasks import TaskContext, TaskResult, TaskResultStatus
from django.utils import timezone
from django.utils.module_loading import import_string

from dj_queue.config import load_backend_config
from dj_queue.db import get_database_alias, locked_queryset
from dj_queue.exceptions import EnqueueError
from dj_queue.log import log_event
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
  _consume_selected_rows,
  _create_blocked_execution,
  _create_ready_execution,
  _create_scheduled_execution,
  _lock_active_pauses,
  _normalize_payload,
  _ready_execution_row,
  _scheduled_execution_row,
  _task_option,
)
from dj_queue.operations.concurrency import (
  concurrency_settings,
  semaphore_acquire,
  semaphore_release,
  unblock_next_blocked_job,
)
from dj_queue.runtime import notify as runtime_notify


CLAIM_READY_JOBS_RETRY_ATTEMPTS = 3
TRANSIENT_CLAIM_ERROR_MESSAGES = (
  "deadlock",
  "lock wait timeout",
  "try restarting transaction",
  "could not serialize access",
  "database is locked",
)


@dataclass(frozen=True)
class ClaimedJob:
  job: Job
  claimed_at: object
  worker_ids: tuple[str, ...]


def enqueue_job(task, args, kwargs, *, backend_alias="default"):
  job, _ = enqueue_job_with_dispatch(task, args, kwargs, backend_alias=backend_alias)
  return job


def enqueue_job_with_dispatch(task, args, kwargs, *, backend_alias="default"):
  validate_queue_allowed(task.queue_name, backend_alias=backend_alias)
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
    dispatched_as = _dispatch_job(job, task=task, backend_alias=backend_alias)

  if dispatched_as == "ready":
    runtime_notify.notify_ready_queues((job.queue_name,), backend_alias=backend_alias)

  log_event(
    "job.enqueued",
    job_id=str(job.id),
    task_path=job.task_path,
    queue_name=job.queue_name,
    priority=job.priority,
  )
  return job, dispatched_as


def enqueue_jobs_bulk(task_calls, *, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  now = timezone.now()
  prepared = []

  for index, (task, args, kwargs) in enumerate(task_calls):
    validate_queue_allowed(task.queue_name, backend_alias=backend_alias)
    payload = _normalize_payload(args, kwargs)
    concurrency_key = _resolve_concurrency_key(task, args, kwargs)
    created_at = now + timedelta(microseconds=index)
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
          created_at=created_at,
          updated_at=created_at,
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
      _lock_active_pauses(alias, backend_alias, {job.queue_name for job in jobs})
      _bulk_create(
        alias,
        ReadyExecution,
        [
          _ready_execution_row(
            job=job,
            backend_alias=backend_alias,
            created_at=job.created_at,
            ready_at=job.created_at,
          )
          for job in jobs
        ],
      )

    ready_queue_names = tuple(dict.fromkeys(job.queue_name for job in jobs))
    if ready_queue_names:
      runtime_notify.notify_ready_queues(ready_queue_names, backend_alias=backend_alias)

    for entry in prepared:
      job = entry["job"]
      log_event(
        "job.enqueued",
        job_id=str(job.id),
        task_path=job.task_path,
        queue_name=job.queue_name,
        priority=job.priority,
      )

    return [(entry["job"], entry["task"], "ready") for entry in prepared]

  ready_rows = []
  scheduled_rows = []
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
        entry["dispatched_as"] = "scheduled"
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
        entry["dispatched_as"] = "ready"
        continue

      dispatched_as = _dispatch_job(job, task=entry["task"], backend_alias=backend_alias, now=now)
      if dispatched_as == "ready":
        ready_queue_names.append(job.queue_name)
      entry["dispatched_as"] = dispatched_as

    _lock_active_pauses(alias, backend_alias, {row.queue_name for row in ready_rows})
    _bulk_create(alias, ReadyExecution, ready_rows)
    _bulk_create(alias, ScheduledExecution, scheduled_rows)

  if ready_queue_names:
    runtime_notify.notify_ready_queues(
      tuple(dict.fromkeys(ready_queue_names)),
      backend_alias=backend_alias,
    )

  for entry in prepared:
    job = entry["job"]
    log_event(
      "job.enqueued",
      job_id=str(job.id),
      task_path=job.task_path,
      queue_name=job.queue_name,
      priority=job.priority,
    )

  return [(entry["job"], entry["task"], entry["dispatched_as"]) for entry in prepared]


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
      time.sleep(0.01 * (attempt + 1))

  for claimed_job in claimed_jobs:
    log_event(
      "job.claimed",
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
    paused_queue_names = _lock_active_pauses(alias, backend_alias)
    queryset = (
      ReadyExecution.objects.using(alias).select_related("job").filter(backend_alias=backend_alias)
    )
    if paused_queue_names:
      queryset = queryset.exclude(queue_name__in=paused_queue_names)
    ready_rows = _select_ready_rows(
      queryset,
      limit=limit,
      queues=queues,
      use_skip_locked=use_skip_locked,
    )
    if not ready_rows:
      return []

    paused_queue_names = _lock_active_pauses(
      alias,
      backend_alias,
      {row.queue_name for row in ready_rows},
    )
    if paused_queue_names:
      ready_rows = [row for row in ready_rows if row.queue_name not in paused_queue_names]
      if not ready_rows:
        return []

    ready_rows = _consume_selected_rows(alias, ReadyExecution, ready_rows)
    if not ready_rows:
      return []

    jobs = [row.job for row in ready_rows]

    claimed_at = timezone.now()
    worker_ids = (process.name,) if process is not None else ()
    _bulk_create(
      alias,
      ClaimedExecution,
      [ClaimedExecution(job=job, process=process, created_at=claimed_at) for job in jobs],
    )

  return [ClaimedJob(job=job, claimed_at=claimed_at, worker_ids=worker_ids) for job in jobs]


def execute_claimed_job(job, *, backend_alias="default"):
  claimed_job = None
  if isinstance(job, ClaimedJob):
    claimed_job = job
    job = claimed_job.job
  elif not isinstance(job, Job):
    claimed_job = _load_claimed_job(job, backend_alias=backend_alias)
    job = claimed_job.job

  try:
    task = import_string(job.task_path)
    args = list(job.payload.get("args", []))
    kwargs = dict(job.payload.get("kwargs", {}))
    if task.takes_context:
      if claimed_job is None:
        claimed_job = _load_claimed_job(job.id, backend_alias=job.backend_alias)
      context = TaskContext(task_result=_task_result_for_claimed_job(task, claimed_job))
      return_value = task.call(context, *args, **kwargs)
    else:
      return_value = task.call(*args, **kwargs)
    return_value = _normalize_return_value(return_value)
  except Exception as exc:
    return fail_claimed_job(
      job,
      exc,
      traceback_text=traceback.format_exc(),
      backend_alias=job.backend_alias,
    )

  return complete_claimed_job(job, return_value, backend_alias=job.backend_alias)


def complete_claimed_job(job, return_value, *, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  if isinstance(job, ClaimedJob):
    job = job.job
  job_id = job.id if isinstance(job, Job) else job

  with transaction.atomic(using=alias):
    queryset = ClaimedExecution.objects.using(alias).select_for_update()
    if isinstance(job, Job):
      claimed = queryset.get(job_id=job_id, job__backend_alias=backend_alias)
    else:
      claimed = queryset.select_related("job").get(job_id=job_id, job__backend_alias=backend_alias)
      job = claimed.job
    now = timezone.now()
    config = load_backend_config(job.backend_alias)

    if config.preserve_finished_jobs:
      job.finished_at = now
      job.return_value = return_value
      job.save(using=alias, update_fields=["finished_at", "return_value", "updated_at"])
      claimed.delete(using=alias)
    else:
      job.delete(using=alias)

  _release_concurrency_slot(job)
  log_event("job.executed", job_id=str(job.id), status="success")
  return job


def fail_claimed_job(job, error, *, traceback_text="", backend_alias="default"):
  alias = get_database_alias(backend_alias)
  if isinstance(job, ClaimedJob):
    job = job.job
  job_id = job.id if isinstance(job, Job) else job

  with transaction.atomic(using=alias):
    queryset = ClaimedExecution.objects.using(alias).select_for_update()
    if isinstance(job, Job):
      claimed = queryset.get(job_id=job_id, job__backend_alias=backend_alias)
    else:
      claimed = queryset.select_related("job").get(job_id=job_id, job__backend_alias=backend_alias)
      job = claimed.job
    claimed.delete(using=alias)
    FailedExecution.objects.using(alias).create(
      job=job,
      exception_class=_exception_path(error),
      message=str(error),
      traceback=traceback_text,
    )

  _release_concurrency_slot(job)
  log_event(
    "job.failed",
    job_id=str(job.id),
    exception_class=_exception_path(error),
    message=str(error),
  )
  return job


def fail_orphaned_claimed_jobs(error, *, traceback_text="", backend_alias="default"):
  alias = get_database_alias(backend_alias)
  job_ids = list(
    ClaimedExecution.objects.using(alias)
    .filter(process__isnull=True, job__backend_alias=backend_alias)
    .values_list("job_id", flat=True)
  )
  return _fail_claimed_job_ids(
    job_ids,
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
  job_ids = list(
    ClaimedExecution.objects.using(alias).filter(process=process).values_list("job_id", flat=True)
  )
  failed_jobs = _fail_claimed_job_ids(
    job_ids,
    error,
    traceback_text=traceback_text,
    backend_alias=backend_alias,
  )
  if delete_process:
    process.delete(using=alias)
  return failed_jobs


def fail_claimed_jobs_for_pid(pid, error, *, traceback_text="", backend_alias="default"):
  alias = get_database_alias(backend_alias)
  process = Process.objects.using(alias).filter(pid=pid, backend_alias=backend_alias).first()
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
  queryset = Process.objects.using(alias).filter(
    backend_alias=backend_alias,
    last_heartbeat_at__lt=cutoff,
  )
  if exclude_process is not None:
    queryset = queryset.exclude(pk=exclude_process.pk)

  stale_processes = list(queryset.order_by("last_heartbeat_at", "id"))
  for process in stale_processes:
    fail_claimed_jobs_for_process(
      process,
      error,
      traceback_text=traceback_text,
      backend_alias=backend_alias,
      delete_process=True,
    )
  return stale_processes


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

    scheduled_rows = _consume_selected_rows(alias, ScheduledExecution, scheduled_rows)
    if not scheduled_rows:
      return []

    jobs = [row.job for row in scheduled_rows]

    direct_jobs = [job for job in jobs if not job.concurrency_key]
    if direct_jobs:
      _lock_active_pauses(alias, backend_alias, {job.queue_name for job in direct_jobs})
      _bulk_create(
        alias,
        ReadyExecution,
        [
          _ready_execution_row(
            job=job,
            backend_alias=backend_alias,
            created_at=now,
            ready_at=now,
          )
          for job in direct_jobs
        ],
      )
      ready_queue_names.extend(job.queue_name for job in direct_jobs)

    direct_job_ids = {job.pk for job in direct_jobs}
    for job in jobs:
      if job.pk in direct_job_ids:
        continue
      dispatched_as = _dispatch_existing_job(job)
      if dispatched_as == "ready":
        ready_queue_names.append(job.queue_name)

  if ready_queue_names:
    runtime_notify.notify_ready_queues(
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

    job = scheduled.job
    scheduled.delete(using=alias)
    job.scheduled_at = None
    job.save(using=alias, update_fields=["scheduled_at", "updated_at"])
    dispatched_as = _dispatch_existing_job(job)

  if dispatched_as == "ready":
    runtime_notify.notify_ready_queues((job.queue_name,), backend_alias=backend_alias)

  log_event(
    "job.dispatched_now",
    job_id=str(job.id),
    queue_name=job.queue_name,
    priority=job.priority,
    dispatched_as=dispatched_as,
  )
  return job, dispatched_as


def retry_failed_job(job_id, *, backend_alias="default"):
  alias = get_database_alias(backend_alias)

  with transaction.atomic(using=alias):
    failed = (
      FailedExecution.objects.using(alias)
      .select_for_update()
      .select_related("job")
      .get(job_id=job_id, job__backend_alias=backend_alias)
    )
    job = failed.job
    failed.delete(using=alias)
    job.return_value = None
    job.finished_at = None
    job.save(using=alias, update_fields=["return_value", "finished_at", "updated_at"])
    dispatched_as = _dispatch_existing_job(job)

  if dispatched_as == "ready":
    runtime_notify.notify_ready_queues((job.queue_name,), backend_alias=backend_alias)

  log_event("job.retried", job_id=str(job.id), queue_name=job.queue_name, priority=job.priority)
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

    jobs = []
    ready_queue_names = []
    for failed in failed_rows:
      job = failed.job
      failed.delete(using=alias)
      job.return_value = None
      job.finished_at = None
      job.save(using=alias, update_fields=["return_value", "finished_at", "updated_at"])
      dispatched_as = _dispatch_existing_job(job)
      jobs.append(job)
      if dispatched_as == "ready":
        ready_queue_names.append(job.queue_name)

  if ready_queue_names:
    runtime_notify.notify_ready_queues(
      tuple(dict.fromkeys(ready_queue_names)),
      backend_alias=backend_alias,
    )

  for job in jobs:
    log_event("job.retried", job_id=str(job.id), queue_name=job.queue_name, priority=job.priority)
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
):
  alias = get_database_alias(backend_alias)
  config = load_backend_config(backend_alias)

  with transaction.atomic(using=alias):
    if model is FailedExecution:
      queryset = model.objects.using(alias).filter(job__backend_alias=backend_alias).order_by("id")
    else:
      queryset = model.objects.using(alias).filter(backend_alias=backend_alias).order_by("id")

    if job_ids is not None:
      queryset = queryset.filter(job_id__in=job_ids)
    rows = list(locked_queryset(queryset, use_skip_locked=config.use_skip_locked)[:batch_size])
    if not rows:
      return 0

    row_job_ids = [row.job_id for row in rows]
    jobs_by_id = {job.id: job for job in Job.objects.using(alias).filter(pk__in=row_job_ids)}
    jobs = [jobs_by_id[job_id] for job_id in row_job_ids]
    Job.objects.using(alias).filter(pk__in=row_job_ids).delete()

  for job in jobs:
    if release_concurrency:
      _release_concurrency_slot(job)
    log_event("job.discarded", job_id=str(job.id), reason=reason)
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


def _dispatch_existing_job(job):
  task = import_string(job.task_path)
  return _dispatch_job(job, task=task, backend_alias=job.backend_alias)


def _dispatch_job(job, *, task, backend_alias, now=None):
  alias = get_database_alias(backend_alias)
  if now is None:
    now = timezone.now()

  if job.scheduled_at is not None and job.scheduled_at > now:
    _create_scheduled_execution(
      alias,
      job=job,
      backend_alias=backend_alias,
      scheduled_at=job.scheduled_at,
    )
    return "scheduled"

  if not job.concurrency_key:
    _create_ready_execution(
      alias,
      job=job,
      backend_alias=backend_alias,
      ready_at=now,
    )
    return "ready"

  limit, duration_seconds, on_conflict = concurrency_settings(task, backend_alias=backend_alias)
  if semaphore_acquire(
    job.concurrency_key,
    limit=limit,
    duration_seconds=duration_seconds,
    backend_alias=backend_alias,
  ):
    _create_ready_execution(
      alias,
      job=job,
      backend_alias=backend_alias,
      ready_at=now,
    )
    return "ready"

  if on_conflict == "discard":
    job.finished_at = now
    job.return_value = None
    job.save(using=alias, update_fields=["finished_at", "return_value", "updated_at"])
    return "discarded"

  _create_blocked_execution(
    alias,
    job=job,
    backend_alias=backend_alias,
    concurrency_key=job.concurrency_key,
    expires_at=now + timedelta(seconds=duration_seconds),
  )
  return "blocked"


def _release_concurrency_slot(job):
  if not job.concurrency_key:
    return

  try:
    task = import_string(job.task_path)
    limit, duration_seconds, _ = concurrency_settings(task, backend_alias=job.backend_alias)
  except ImportError:
    limit = _semaphore_limit(job) or 1
    duration_seconds = load_backend_config(job.backend_alias).default_concurrency_duration

  semaphore_release(
    job.concurrency_key,
    duration_seconds=duration_seconds,
    backend_alias=job.backend_alias,
  )
  unblock_next_blocked_job(
    job.concurrency_key,
    limit=limit,
    duration_seconds=duration_seconds,
    backend_alias=job.backend_alias,
    use_skip_locked=load_backend_config(job.backend_alias).use_skip_locked,
  )


def _semaphore_limit(job):
  alias = get_database_alias(job.backend_alias)
  return (
    Semaphore.objects.using(alias)
    .filter(key=job.concurrency_key)
    .values_list("limit", flat=True)
    .first()
  )


def validate_queue_allowed(queue_name, *, backend_alias="default"):
  allowed_queues = load_backend_config(backend_alias).allowed_queues
  if allowed_queues and queue_name not in allowed_queues:
    raise EnqueueError(f"queue {queue_name!r} is not allowed for backend {backend_alias!r}")


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
  signature = inspect.signature(task.func)
  parameters = tuple(signature.parameters.values())
  if task.takes_context and parameters:
    signature = signature.replace(parameters=parameters[1:])

  bound = signature.bind(*args, **kwargs)
  bound.apply_defaults()
  return bound.arguments


def _filter_queue_selectors(queryset, queues):
  if queues in (None, (), "*", ["*"], ("*",)):
    return queryset

  selectors = (queues,) if isinstance(queues, str) else tuple(queues)
  condition = Q()
  for selector in selectors:
    if selector == "*":
      return queryset
    if selector.endswith("*"):
      condition |= Q(queue_name__startswith=selector[:-1])
    else:
      condition |= Q(queue_name=selector)

  if not condition:
    return queryset.none()
  return queryset.filter(condition)


def _select_ready_rows(queryset, *, limit, queues, use_skip_locked):
  if queues in (None, (), "*", ["*"], ("*",)):
    ordered = queryset.order_by("-priority", "id")
    return list(locked_queryset(ordered, use_skip_locked=use_skip_locked)[:limit])

  selectors = (queues,) if isinstance(queues, str) else tuple(queues)
  selected_rows = []
  selected_ids = set()

  for selector in selectors:
    remaining = limit - len(selected_rows)
    if remaining <= 0:
      break

    ordered = queryset.exclude(pk__in=selected_ids).order_by("-priority", "id")
    filtered = _filter_queue_selectors(ordered, selector)
    rows = list(locked_queryset(filtered, use_skip_locked=use_skip_locked)[:remaining])
    selected_rows.extend(rows)
    selected_ids.update(row.pk for row in rows)

  return selected_rows


def _is_transient_claim_error(error):
  message = str(error).lower()
  return any(marker in message for marker in TRANSIENT_CLAIM_ERROR_MESSAGES)


def _normalize_return_value(return_value):
  try:
    return json.loads(json.dumps(return_value))
  except (TypeError, ValueError) as exc:
    raise ValueError("return value must be JSON round-trippable") from exc


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
  )


def _bulk_create(alias, model, objects):
  if not objects:
    return None

  fields = [field for field in model._meta.concrete_fields if not field.generated]
  batch_size = connections[alias].ops.bulk_batch_size(fields, objects)
  if batch_size is None or batch_size <= 0:
    batch_size = len(objects)
  model.objects.using(alias).bulk_create(objects, batch_size=batch_size)
  return None


def _fail_claimed_job_ids(job_ids, error, *, traceback_text, backend_alias):
  failed_jobs = []
  for job_id in job_ids:
    failed_jobs.append(
      fail_claimed_job(
        job_id,
        error,
        traceback_text=traceback_text,
        backend_alias=backend_alias,
      )
    )
  return failed_jobs


def _exception_path(error):
  return f"{error.__class__.__module__}.{error.__class__.__qualname__}"


def _task_result_for_claimed_job(task, claimed_job):
  if not isinstance(claimed_job, ClaimedJob):
    raise RuntimeError("ClaimedJob is required for task context execution")

  return TaskResult(
    task=task,
    id=str(claimed_job.job.id),
    status=TaskResultStatus.RUNNING,
    enqueued_at=claimed_job.job.created_at,
    started_at=claimed_job.claimed_at,
    finished_at=None,
    last_attempted_at=claimed_job.claimed_at,
    args=claimed_job.job.payload.get("args", []),
    kwargs=claimed_job.job.payload.get("kwargs", {}),
    backend=claimed_job.job.backend_alias,
    errors=[],
    worker_ids=list(claimed_job.worker_ids),
  )
