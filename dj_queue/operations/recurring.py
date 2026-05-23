from django.db import transaction
from django.db.models import Q
from django.utils.module_loading import import_string

from dj_queue.cron import is_valid_cron, next_cron_run
from dj_queue.config import load_backend_config
from dj_queue.db import get_database_alias
from dj_queue.exceptions import EnqueueError
from dj_queue.models import RecurringExecution, RecurringTask
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
  if schedule is not None and not is_valid_cron(str(schedule)):
    raise EnqueueError("schedule must be a valid cron expression")
  task = import_string(task_path)
  if not hasattr(task, "using"):
    raise EnqueueError("task_path must reference a Django task")
  validate_queue_allowed(queue_name, backend_alias=backend_alias)
  validate_priority(priority)
  return task


def upsert_static_recurring_tasks(recurring_configs, *, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  active_keys = set()
  configured_keys = tuple(recurring_configs)
  existing = {
    task.key: task
    for task in RecurringTask.objects.using(alias).filter(
      backend_alias=backend_alias,
      static=True,
    )
  }
  if configured_keys:
    existing.update(
      {
        task.key: task
        for task in RecurringTask.objects.using(alias).filter(
          backend_alias=backend_alias,
          key__in=configured_keys,
          static=False,
        )
      }
    )
  to_create = []

  for recurring_config in recurring_configs.values():
    active_keys.add(recurring_config.key)
    validate_recurring_task_definition(
      task_path=recurring_config.task_path,
      queue_name=recurring_config.queue_name,
      priority=recurring_config.priority,
      backend_alias=backend_alias,
      schedule=recurring_config.schedule,
    )
    desired = {
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
    existing_task = existing.get(recurring_config.key)
    if existing_task is None:
      to_create.append(
        RecurringTask(backend_alias=backend_alias, key=recurring_config.key, **desired)
      )
      continue

    if existing_task.static is False:
      raise EnqueueError(
        f"recurring task key {recurring_config.key!r} is already scheduled dynamically"
      )

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

  if to_create:
    RecurringTask.objects.using(alias).bulk_create(to_create)

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
  deleted = queryset.count()
  queryset.delete()
  return deleted


def fire_recurring_task(recurring_task, run_at, *, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  next_run_at = _next_run_after(recurring_task.schedule, run_at)

  with transaction.atomic(using=alias):
    created = create_ignore_conflicts(
      RecurringExecution,
      using=alias,
      backend_alias=backend_alias,
      task_key=recurring_task.key,
      run_at=run_at,
    )
    if not created:
      _advance_next_run_at(recurring_task, next_run_at, using=alias)
      # treat an existing reservation row as authoritative even if its job backfill
      # has not happened yet, so duplicate scheduler ticks never enqueue twice
      return None

    execution = RecurringExecution.objects.using(alias).get(
      backend_alias=backend_alias,
      task_key=recurring_task.key,
      run_at=run_at,
    )

    task = import_string(recurring_task.task_path).using(
      queue_name=recurring_task.queue_name,
      priority=recurring_task.priority,
      backend=backend_alias,
    )
    payload = recurring_task.payload or {}
    job = enqueue_job(
      task,
      payload.get("args", []),
      payload.get("kwargs", {}),
      backend_alias=backend_alias,
    )
    execution.job = job
    execution.save(using=alias, update_fields=["job"])
    _advance_next_run_at(recurring_task, next_run_at, using=alias)
    return execution


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
