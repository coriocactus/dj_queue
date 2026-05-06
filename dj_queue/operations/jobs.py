import inspect
import json
import traceback
from datetime import timedelta

from django.db import connections, transaction
from django.db.models import Q
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
  Pause,
  ReadyExecution,
  ScheduledExecution,
)
from dj_queue.operations.concurrency import (
  semaphore_acquire,
  semaphore_release,
  unblock_next_blocked_job,
)
from dj_queue.runtime import notify as runtime_notify


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
          ReadyExecution(
            job=job,
            queue_name=job.queue_name,
            priority=job.priority,
            created_at=job.created_at,
            latency_started_at=job.created_at,
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
          ScheduledExecution(
            job=job,
            queue_name=job.queue_name,
            priority=job.priority,
            scheduled_at=job.scheduled_at,
            created_at=job.created_at,
          )
        )
        entry["dispatched_as"] = "scheduled"
        continue

      if not job.concurrency_key:
        ready_rows.append(
          ReadyExecution(
            job=job,
            queue_name=job.queue_name,
            priority=job.priority,
            created_at=job.created_at,
            latency_started_at=job.created_at,
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

  paused_queue_names = list(
    Pause.objects.using(alias)
    .filter(backend_alias=backend_alias)
    .values_list("queue_name", flat=True)
  )

  with transaction.atomic(using=alias):
    queryset = (
      ReadyExecution.objects.using(alias)
      .select_related("job")
      .filter(job__backend_alias=backend_alias)
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

    jobs = [row.job for row in ready_rows]

    ReadyExecution.objects.using(alias).filter(pk__in=[row.pk for row in ready_rows]).delete()
    _bulk_create(
      alias,
      ClaimedExecution,
      [ClaimedExecution(job=job, process=process) for job in jobs],
    )

  for job in jobs:
    log_event("job.claimed", job_id=str(job.id), queue_name=job.queue_name, priority=job.priority)
  return jobs


def execute_claimed_job(job_id, *, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  claimed = (
    ClaimedExecution.objects.using(alias)
    .select_related("job", "process")
    .get(job_id=job_id, job__backend_alias=backend_alias)
  )
  job = claimed.job

  try:
    task = import_string(job.task_path)
    args = list(job.payload.get("args", []))
    kwargs = dict(job.payload.get("kwargs", {}))
    if task.takes_context:
      context = TaskContext(task_result=_task_result_for_claimed_job(task, claimed))
      return_value = task.call(context, *args, **kwargs)
    else:
      return_value = task.call(*args, **kwargs)
    return_value = _normalize_return_value(return_value)
  except Exception as exc:
    return fail_claimed_job(
      job.id,
      exc,
      traceback_text=traceback.format_exc(),
      backend_alias=job.backend_alias,
    )

  return complete_claimed_job(job.id, return_value, backend_alias=job.backend_alias)


def complete_claimed_job(job_id, return_value, *, backend_alias="default"):
  alias = get_database_alias(backend_alias)

  with transaction.atomic(using=alias):
    claimed = (
      ClaimedExecution.objects.using(alias)
      .select_for_update()
      .select_related("job")
      .get(job_id=job_id, job__backend_alias=backend_alias)
    )
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


def fail_claimed_job(job_id, error, *, traceback_text="", backend_alias="default"):
  alias = get_database_alias(backend_alias)

  with transaction.atomic(using=alias):
    claimed = (
      ClaimedExecution.objects.using(alias)
      .select_for_update()
      .select_related("job")
      .get(job_id=job_id, job__backend_alias=backend_alias)
    )
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


def promote_scheduled_jobs(*, batch_size, backend_alias="default", use_skip_locked=None):
  alias = get_database_alias(backend_alias)
  if use_skip_locked is None:
    use_skip_locked = load_backend_config(backend_alias).use_skip_locked
  now = timezone.now()

  with transaction.atomic(using=alias):
    queryset = (
      ScheduledExecution.objects.using(alias)
      .select_related("job")
      .filter(job__backend_alias=backend_alias, scheduled_at__lte=now)
      .order_by("scheduled_at", "-priority", "id")
    )
    scheduled_rows = list(locked_queryset(queryset, use_skip_locked=use_skip_locked)[:batch_size])
    if not scheduled_rows:
      return []

    jobs = [row.job for row in scheduled_rows]

    ScheduledExecution.objects.using(alias).filter(
      pk__in=[row.pk for row in scheduled_rows]
    ).delete()
    for job in jobs:
      dispatched_as = _dispatch_existing_job(job)
      if dispatched_as == "ready":
        runtime_notify.notify_ready_queues((job.queue_name,), backend_alias=backend_alias)
    return jobs


def dispatch_scheduled_job_now(job_id, *, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  config = load_backend_config(backend_alias)

  with transaction.atomic(using=alias):
    scheduled = locked_queryset(
      ScheduledExecution.objects.using(alias)
      .select_related("job")
      .filter(job_id=job_id, job__backend_alias=backend_alias),
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


def discard_failed_jobs(*, job_ids=None, batch_size=500, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  config = load_backend_config(backend_alias)

  with transaction.atomic(using=alias):
    queryset = (
      FailedExecution.objects.using(alias).filter(job__backend_alias=backend_alias).order_by("id")
    )
    if job_ids is not None:
      queryset = queryset.filter(job_id__in=job_ids)
    failed_rows = list(
      locked_queryset(queryset, use_skip_locked=config.use_skip_locked)[:batch_size]
    )
    if not failed_rows:
      return 0

    job_ids = [row.job_id for row in failed_rows]
    jobs_by_id = {job.id: job for job in Job.objects.using(alias).filter(pk__in=job_ids)}
    jobs = [jobs_by_id[job_id] for job_id in job_ids]
    Job.objects.using(alias).filter(pk__in=[row.job_id for row in failed_rows]).delete()

  for job in jobs:
    log_event("job.discarded", job_id=str(job.id), reason="failed")
  return len(jobs)


def discard_failed_job(job_id, *, backend_alias="default"):
  return discard_failed_jobs(job_ids=[job_id], batch_size=1, backend_alias=backend_alias)


def discard_ready_jobs(*, job_ids=None, batch_size=500, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  config = load_backend_config(backend_alias)

  with transaction.atomic(using=alias):
    queryset = (
      ReadyExecution.objects.using(alias).filter(job__backend_alias=backend_alias).order_by("id")
    )
    if job_ids is not None:
      queryset = queryset.filter(job_id__in=job_ids)
    ready_rows = list(
      locked_queryset(queryset, use_skip_locked=config.use_skip_locked)[:batch_size]
    )
    if not ready_rows:
      return 0

    job_ids = [row.job_id for row in ready_rows]
    jobs_by_id = {job.id: job for job in Job.objects.using(alias).filter(pk__in=job_ids)}
    jobs = [jobs_by_id[job_id] for job_id in job_ids]
    Job.objects.using(alias).filter(pk__in=[row.job_id for row in ready_rows]).delete()

  for job in jobs:
    _release_concurrency_slot(job)
    log_event("job.discarded", job_id=str(job.id), reason="ready")
  return len(jobs)


def discard_scheduled_jobs(*, job_ids=None, batch_size=500, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  config = load_backend_config(backend_alias)

  with transaction.atomic(using=alias):
    queryset = (
      ScheduledExecution.objects.using(alias)
      .filter(job__backend_alias=backend_alias)
      .order_by("id")
    )
    if job_ids is not None:
      queryset = queryset.filter(job_id__in=job_ids)
    scheduled_rows = list(
      locked_queryset(queryset, use_skip_locked=config.use_skip_locked)[:batch_size]
    )
    if not scheduled_rows:
      return 0

    job_ids = [row.job_id for row in scheduled_rows]
    jobs_by_id = {job.id: job for job in Job.objects.using(alias).filter(pk__in=job_ids)}
    jobs = [jobs_by_id[job_id] for job_id in job_ids]
    Job.objects.using(alias).filter(pk__in=[row.job_id for row in scheduled_rows]).delete()

  for job in jobs:
    log_event("job.discarded", job_id=str(job.id), reason="scheduled")
  return len(jobs)


def discard_blocked_jobs(*, job_ids=None, batch_size=500, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  config = load_backend_config(backend_alias)

  with transaction.atomic(using=alias):
    queryset = (
      BlockedExecution.objects.using(alias).filter(job__backend_alias=backend_alias).order_by("id")
    )
    if job_ids is not None:
      queryset = queryset.filter(job_id__in=job_ids)
    blocked_rows = list(
      locked_queryset(queryset, use_skip_locked=config.use_skip_locked)[:batch_size]
    )
    if not blocked_rows:
      return 0

    job_ids = [row.job_id for row in blocked_rows]
    jobs_by_id = {job.id: job for job in Job.objects.using(alias).filter(pk__in=job_ids)}
    jobs = [jobs_by_id[job_id] for job_id in job_ids]
    Job.objects.using(alias).filter(pk__in=[row.job_id for row in blocked_rows]).delete()

  for job in jobs:
    log_event("job.discarded", job_id=str(job.id), reason="blocked")
  return len(jobs)


def _dispatch_existing_job(job):
  task = import_string(job.task_path)
  return _dispatch_job(job, task=task, backend_alias=job.backend_alias)


def _dispatch_job(job, *, task, backend_alias, now=None):
  alias = get_database_alias(backend_alias)
  if now is None:
    now = timezone.now()

  if job.scheduled_at is not None and job.scheduled_at > now:
    ScheduledExecution.objects.using(alias).create(
      job=job,
      queue_name=job.queue_name,
      priority=job.priority,
      scheduled_at=job.scheduled_at,
    )
    return "scheduled"

  if not job.concurrency_key:
    _lock_active_pauses(alias, backend_alias, {job.queue_name})
    ReadyExecution.objects.using(alias).create(
      job=job,
      queue_name=job.queue_name,
      priority=job.priority,
      latency_started_at=now,
    )
    return "ready"

  limit, duration_seconds, on_conflict = _concurrency_settings(task, backend_alias=backend_alias)
  if semaphore_acquire(
    job.concurrency_key,
    limit=limit,
    duration_seconds=duration_seconds,
    backend_alias=backend_alias,
  ):
    _lock_active_pauses(alias, backend_alias, {job.queue_name})
    ReadyExecution.objects.using(alias).create(
      job=job,
      queue_name=job.queue_name,
      priority=job.priority,
      latency_started_at=now,
    )
    return "ready"

  if on_conflict == "discard":
    job.finished_at = now
    job.return_value = None
    job.save(using=alias, update_fields=["finished_at", "return_value", "updated_at"])
    return "discarded"

  BlockedExecution.objects.using(alias).create(
    job=job,
    queue_name=job.queue_name,
    priority=job.priority,
    concurrency_key=job.concurrency_key,
    expires_at=now + timedelta(seconds=duration_seconds),
  )
  return "blocked"


def _release_concurrency_slot(job):
  if not job.concurrency_key:
    return

  task = import_string(job.task_path)
  limit, duration_seconds, _ = _concurrency_settings(task, backend_alias=job.backend_alias)
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


def _concurrency_settings(task, *, backend_alias):
  limit = _task_option(task, "concurrency_limit")
  if limit in (None, ""):
    raise EnqueueError("concurrency_limit is required when concurrency_key is set")

  limit = _positive_int_option(limit, "concurrency_limit")
  duration_seconds = _positive_int_option(
    _task_option(
      task,
      "concurrency_duration",
      load_backend_config(backend_alias).default_concurrency_duration,
    ),
    "concurrency_duration",
  )
  on_conflict = str(_task_option(task, "on_conflict", "block"))
  if on_conflict not in {"block", "discard"}:
    raise EnqueueError("on_conflict must be 'block' or 'discard'")
  return limit, duration_seconds, on_conflict


def validate_queue_allowed(queue_name, *, backend_alias="default"):
  allowed_queues = load_backend_config(backend_alias).allowed_queues
  if allowed_queues and queue_name not in allowed_queues:
    raise EnqueueError(f"queue {queue_name!r} is not allowed for backend {backend_alias!r}")


def _positive_int_option(value, name):
  try:
    number = int(value)
  except (TypeError, ValueError, OverflowError) as exc:
    raise EnqueueError(f"{name} must be a positive integer") from exc

  if number <= 0:
    raise EnqueueError(f"{name} must be a positive integer")
  return number


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


def _normalize_payload(args, kwargs):
  try:
    return json.loads(json.dumps({"args": list(args), "kwargs": dict(kwargs)}))
  except (TypeError, ValueError) as exc:
    raise EnqueueError("payload must be JSON round-trippable") from exc


def _normalize_return_value(return_value):
  try:
    return json.loads(json.dumps(return_value))
  except (TypeError, ValueError) as exc:
    raise ValueError("return value must be JSON round-trippable") from exc


def _task_option(task, name, default=None):
  if hasattr(task, name):
    return getattr(task, name)
  return getattr(task.func, name, default)


def _lock_active_pauses(alias, backend_alias, queue_names):
  active_queue_names = tuple(queue_name for queue_name in queue_names if queue_name)
  if not active_queue_names:
    return None

  list(
    Pause.objects.using(alias)
    .select_for_update()
    .filter(backend_alias=backend_alias, queue_name__in=active_queue_names)
    .values_list("queue_name", flat=True)
  )
  return None


def _bulk_create(alias, model, objects):
  if not objects:
    return None

  fields = [field for field in model._meta.concrete_fields if not field.generated]
  batch_size = connections[alias].ops.bulk_batch_size(fields, objects)
  if batch_size is None or batch_size <= 0:
    batch_size = len(objects)
  model.objects.using(alias).bulk_create(objects, batch_size=batch_size)
  return None


def _exception_path(error):
  return f"{error.__class__.__module__}.{error.__class__.__qualname__}"


def _task_result_for_claimed_job(task, claimed):
  worker_ids = []
  if claimed.process_id is not None:
    worker_ids = [claimed.process.name]

  return TaskResult(
    task=task,
    id=str(claimed.job.id),
    status=TaskResultStatus.RUNNING,
    enqueued_at=claimed.job.created_at,
    started_at=claimed.created_at,
    finished_at=None,
    last_attempted_at=claimed.created_at,
    args=claimed.job.payload.get("args", []),
    kwargs=claimed.job.payload.get("kwargs", {}),
    backend=claimed.job.backend_alias,
    errors=[],
    worker_ids=worker_ids,
  )
