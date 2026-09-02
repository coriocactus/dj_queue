from datetime import timedelta
from uuid import uuid4

from django.db import IntegrityError, transaction
from django.db.models import Exists, OuterRef, Q
from django.utils.module_loading import import_string

from dj_queue.config import load_backend_config
from dj_queue.cron import is_valid_cron, latest_cron_run, next_cron_run
from dj_queue.db import get_database_alias, locked_queryset
from dj_queue.exceptions import EnqueueError
from dj_queue.models import Job, RecurringExecution, RecurringTask
from dj_queue.operations._helpers import _normalize_payload
from dj_queue.operations._insert import create_ignore_conflicts
from dj_queue.operations.jobs import enqueue_job, validate_priority, validate_queue_allowed


def validate_recurring_task_definition(
  *,
  task_path,
  queue_name,
  priority,
  backend_alias,
  schedule=None,
):
  task_path = _recurring_string(task_path, "task_path")
  queue_name = _recurring_string(queue_name, "queue_name")
  if schedule is not None:
    schedule = _recurring_string(schedule, "schedule")
  if schedule is not None and not is_valid_cron(schedule):
    raise EnqueueError("schedule must be a valid cron expression")
  try:
    task = import_string(task_path)
  except ImportError as exc:
    raise EnqueueError(f"task_path must be importable: {task_path}") from exc
  if not hasattr(task, "using"):
    raise EnqueueError("task_path must reference a Django task")
  validate_queue_allowed(queue_name, backend_alias=backend_alias)
  validate_priority(priority)
  return task


def _recurring_string(value, name):
  if not isinstance(value, str) or value == "":
    raise EnqueueError(f"{name} must be a non-empty string")
  return value


def _recurring_optional_string(value, name):
  if not isinstance(value, str):
    raise EnqueueError(f"{name} must be a string")
  return value


def upsert_static_recurring_tasks(recurring_configs, *, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  active_keys = set()
  configured_keys = tuple(recurring_configs)
  desired_by_key = {}

  for recurring_config in recurring_configs.values():
    active_keys.add(recurring_config.key)
    validate_recurring_task_definition(
      task_path=recurring_config.task_path,
      queue_name=recurring_config.queue_name,
      priority=recurring_config.priority,
      backend_alias=backend_alias,
      schedule=recurring_config.schedule,
    )
    desired_by_key[recurring_config.key] = {
      "task_path": recurring_config.task_path,
      "payload": {
        "args": list(recurring_config.args),
        "kwargs": dict(recurring_config.kwargs),
      },
      "schedule": recurring_config.schedule,
      "queue_name": recurring_config.queue_name,
      "priority": recurring_config.priority,
      "description": recurring_config.description,
      "static": True,
    }

  with transaction.atomic(using=alias):
    _apply_static_recurring_tasks(
      alias,
      backend_alias=backend_alias,
      configured_keys=configured_keys,
      active_keys=active_keys,
      desired_by_key=desired_by_key,
    )


def _apply_static_recurring_tasks(
  alias, *, backend_alias, configured_keys, active_keys, desired_by_key
):
  existing = {
    task.key: task
    for task in RecurringTask.objects.using(alias)
    .select_for_update()
    .filter(
      backend_alias=backend_alias,
      static=True,
    )
  }
  if configured_keys:
    existing.update(
      {
        task.key: task
        for task in RecurringTask.objects.using(alias)
        .select_for_update()
        .filter(
          backend_alias=backend_alias,
          key__in=configured_keys,
          static=False,
        )
      }
    )
  to_create = []

  for key, desired in desired_by_key.items():
    existing_task = existing.get(key)
    if existing_task is None:
      to_create.append(RecurringTask(backend_alias=backend_alias, key=key, **desired))
      continue

    if existing_task.static is False:
      raise EnqueueError(f"recurring task key {key!r} is already scheduled dynamically")

  if to_create:
    RecurringTask.objects.using(alias).bulk_create(to_create, ignore_conflicts=True)

  if configured_keys:
    existing.update(
      {
        task.key: task
        for task in RecurringTask.objects.using(alias)
        .select_for_update()
        .filter(
          backend_alias=backend_alias,
          key__in=configured_keys,
        )
      }
    )

  for key, desired in desired_by_key.items():
    existing_task = existing[key]
    if existing_task.static is False:
      raise EnqueueError(f"recurring task key {key!r} is already scheduled dynamically")

    changed_fields = []
    for field, value in desired.items():
      if getattr(existing_task, field) == value:
        continue
      setattr(existing_task, field, value)
      changed_fields.append(field)

    if changed_fields:
      if "schedule" in changed_fields:
        existing_task.next_run_at = None
        changed_fields.append("next_run_at")
      existing_task.save(using=alias, update_fields=[*changed_fields, "updated_at"])

  queryset = RecurringTask.objects.using(alias).filter(backend_alias=backend_alias, static=True)
  if active_keys:
    queryset = queryset.exclude(key__in=active_keys)
  queryset.delete()


def schedule_recurring_task(
  *,
  key,
  task_path,
  schedule,
  args=(),
  kwargs=None,
  queue_name="default",
  priority=0,
  description="",
  backend_alias="default",
):
  alias = get_database_alias(backend_alias)
  if kwargs is None:
    kwargs = {}
  key = _recurring_string(key, "key")
  description = _recurring_optional_string(description, "description")

  validate_recurring_task_definition(
    task_path=task_path,
    queue_name=queue_name,
    priority=priority,
    backend_alias=backend_alias,
    schedule=schedule,
  )
  payload = _normalize_payload(args, kwargs)
  if key in load_backend_config(backend_alias).recurring:
    raise EnqueueError(f"recurring task key {key!r} is already managed statically")

  with transaction.atomic(using=alias):
    recurring_task, created = (
      RecurringTask.objects.using(alias)
      .select_for_update()
      .get_or_create(
        backend_alias=backend_alias,
        key=key,
        defaults={
          "task_path": task_path,
          "payload": payload,
          "schedule": schedule,
          "queue_name": queue_name,
          "priority": priority,
          "description": description,
          "static": False,
        },
      )
    )
    if created:
      return recurring_task
    if recurring_task.static:
      raise EnqueueError(f"recurring task key {key!r} is already managed statically")

    previous_schedule = recurring_task.schedule
    changed_fields = []
    desired = {
      "task_path": task_path,
      "payload": payload,
      "schedule": schedule,
      "queue_name": queue_name,
      "priority": priority,
      "description": description,
      "static": False,
    }
    for field, value in desired.items():
      if getattr(recurring_task, field) == value:
        continue
      setattr(recurring_task, field, value)
      changed_fields.append(field)

    if previous_schedule != schedule:
      recurring_task.next_run_at = None
      changed_fields.append("next_run_at")

    if changed_fields:
      recurring_task.save(using=alias, update_fields=[*changed_fields, "updated_at"])
    return recurring_task


def unschedule_recurring_task(key, *, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  queryset = RecurringTask.objects.using(alias).filter(
    backend_alias=backend_alias,
    key=key,
    static=False,
  )
  deleted, _ = queryset.delete()
  return deleted


def fire_due_recurring_tasks(
  now,
  *,
  include_dynamic_tasks=False,
  backend_alias="default",
  batch_size=500,
):
  alias = get_database_alias(backend_alias)
  unbackfilled = RecurringExecution.objects.using(alias).filter(
    backend_alias=backend_alias,
    task_key=OuterRef("key"),
    job__isnull=True,
  )
  queryset = (
    RecurringTask.objects.using(alias)
    .filter(backend_alias=backend_alias)
    .annotate(has_unbackfilled=Exists(unbackfilled))
    .filter(Q(next_run_at__isnull=True) | Q(next_run_at__lte=now) | Q(has_unbackfilled=True))
    .order_by("next_run_at", "key")
  )
  if not include_dynamic_tasks:
    queryset = queryset.filter(static=True)
  if batch_size is not None:
    queryset = queryset[:batch_size]

  recurring_tasks = list(queryset)
  if not recurring_tasks:
    return []

  remaining = batch_size
  pending_by_key = {}
  if any(task.has_unbackfilled for task in recurring_tasks):
    pending = (
      RecurringExecution.objects.using(alias)
      .filter(
        backend_alias=backend_alias,
        task_key__in=[task.key for task in recurring_tasks],
        job__isnull=True,
      )
      .order_by("run_at", "id")
    )
    if remaining is not None:
      pending = pending[:remaining]
    for execution in pending:
      pending_by_key.setdefault(execution.task_key, []).append(execution)

  fired_jobs = []
  for recurring_task in recurring_tasks:
    for pending_execution in pending_by_key.get(recurring_task.key, ()):
      execution = fire_recurring_task(
        recurring_task,
        pending_execution.run_at,
        backend_alias=backend_alias,
      )
      if execution is not None and execution.job_id is not None:
        fired_jobs.append(execution.job)
      if remaining is not None:
        remaining -= 1
        if remaining == 0:
          return fired_jobs

    if recurring_task.next_run_at is not None and recurring_task.next_run_at > now:
      continue
    run_at = latest_cron_run(recurring_task.schedule, now)
    if run_at is None:
      continue
    execution = fire_recurring_task(recurring_task, run_at, backend_alias=backend_alias)
    if execution is not None and execution.job_id is not None:
      fired_jobs.append(execution.job)
    if remaining is not None:
      remaining -= 1
      if remaining == 0:
        break
  return fired_jobs


def fire_recurring_task(recurring_task, run_at, *, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  reservation = _reserve_recurring_task(recurring_task, run_at, backend_alias=backend_alias)
  if reservation is None:
    return None

  job = _enqueue_reserved_recurring_task(reservation, using=alias, backend_alias=backend_alias)
  return _attach_reserved_recurring_job(reservation, job, using=alias, backend_alias=backend_alias)


def _reserve_recurring_task(recurring_task, run_at, *, backend_alias):
  alias = get_database_alias(backend_alias)
  config = load_backend_config(backend_alias)
  with transaction.atomic(using=alias):
    recurring_task = locked_queryset(
      RecurringTask.objects.using(alias).filter(pk=recurring_task.pk, backend_alias=backend_alias),
      use_skip_locked=config.use_skip_locked,
    ).first()
    if recurring_task is None:
      return None
    if latest_cron_run(recurring_task.schedule, run_at + timedelta(microseconds=1)) != run_at:
      return None

    next_run_at = _next_run_after(recurring_task.schedule, run_at)
    if recurring_task.next_run_at is not None and recurring_task.next_run_at > run_at:
      execution = (
        RecurringExecution.objects.using(alias)
        .filter(
          backend_alias=backend_alias,
          task_key=recurring_task.key,
          run_at=run_at,
        )
        .first()
      )
      if execution is None or execution.job_id is not None:
        return None
      return _recurring_reservation(execution, recurring_task, next_run_at)

    intended_job_id = uuid4()
    created = create_ignore_conflicts(
      RecurringExecution,
      using=alias,
      backend_alias=backend_alias,
      task_key=recurring_task.key,
      run_at=run_at,
      intended_job_id=intended_job_id,
    )

    execution = RecurringExecution.objects.using(alias).get(
      backend_alias=backend_alias,
      task_key=recurring_task.key,
      run_at=run_at,
    )
    _advance_next_run_at(recurring_task, next_run_at, using=alias)
    if not created and execution.job_id is not None:
      return None
    return _recurring_reservation(execution, recurring_task, next_run_at)


def _recurring_reservation(execution, recurring_task, next_run_at):
  return {
    "execution_id": execution.id,
    "intended_job_id": execution.intended_job_id,
    "recurring_task_id": recurring_task.id,
    "task_key": recurring_task.key,
    "run_at": execution.run_at,
    "next_run_at": next_run_at,
    "task_path": recurring_task.task_path,
    "payload": recurring_task.payload or {},
    "queue_name": recurring_task.queue_name,
    "priority": recurring_task.priority,
    "backend_alias": recurring_task.backend_alias,
  }


def _enqueue_reserved_recurring_task(reservation, *, using, backend_alias):
  existing_job = Job.objects.using(using).filter(pk=reservation["intended_job_id"]).first()
  if existing_job is not None:
    return _validated_reserved_job(reservation, existing_job, using=using)

  task = import_string(reservation["task_path"]).using(
    queue_name=reservation["queue_name"],
    priority=reservation["priority"],
    backend=backend_alias,
  )
  payload = reservation["payload"]
  try:
    return enqueue_job(
      task,
      payload.get("args", []),
      payload.get("kwargs", {}),
      backend_alias=backend_alias,
      job_id=reservation["intended_job_id"],
    )
  except IntegrityError:
    existing_job = Job.objects.using(using).filter(pk=reservation["intended_job_id"]).first()
    if existing_job is None:
      raise
    return _validated_reserved_job(reservation, existing_job, using=using)


def _validated_reserved_job(reservation, job, *, using):
  payload = reservation["payload"]
  expected = (
    reservation["task_path"],
    reservation["queue_name"],
    reservation["priority"],
    _normalize_payload(payload.get("args", []), payload.get("kwargs", {})),
    reservation["backend_alias"],
  )
  actual = (
    job.task_path,
    job.queue_name,
    job.priority,
    job.payload,
    job.backend_alias,
  )
  assigned_elsewhere = (
    RecurringExecution.objects.using(using)
    .filter(job_id=job.id)
    .exclude(pk=reservation["execution_id"])
    .exists()
  )
  if actual != expected or assigned_elsewhere:
    raise EnqueueError("intended recurring job metadata does not match reservation")
  return job


def _attach_reserved_recurring_job(reservation, job, *, using, backend_alias):
  updated = (
    RecurringExecution.objects.using(using)
    .filter(
      pk=reservation["execution_id"],
      backend_alias=backend_alias,
      job__isnull=True,
      intended_job_id=job.id,
    )
    .update(job=job)
  )
  if updated != 1:
    execution = (
      RecurringExecution.objects.using(using)
      .select_related("job")
      .get(pk=reservation["execution_id"])
    )
    if execution.job_id == job.id and execution.intended_job_id == job.id:
      return None
    raise EnqueueError("recurring execution reservation could not be assigned a job")
  return (
    RecurringExecution.objects.using(using)
    .select_related("job")
    .get(pk=reservation["execution_id"])
  )


def _next_run_after(schedule, run_at):
  return next_cron_run(schedule, run_at)


def _advance_next_run_at(recurring_task, next_run_at, *, using):
  updated = (
    RecurringTask.objects.using(using)
    .filter(
      Q(next_run_at__isnull=True) | Q(next_run_at__lt=next_run_at),
      pk=recurring_task.pk,
      backend_alias=recurring_task.backend_alias,
    )
    .update(next_run_at=next_run_at)
  )
  if updated:
    recurring_task.next_run_at = next_run_at
