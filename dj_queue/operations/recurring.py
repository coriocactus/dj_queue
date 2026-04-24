from django.db import transaction
from django.utils.module_loading import import_string

from dj_queue.db import get_database_alias
from dj_queue.models import RecurringExecution, RecurringTask
from dj_queue.operations._insert import create_ignore_conflicts
from dj_queue.operations.jobs import enqueue_job


def upsert_static_recurring_tasks(recurring_configs, *, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  active_keys = set()
  existing = {
    task.key: task
    for task in RecurringTask.objects.using(alias).filter(
      backend_alias=backend_alias,
      static=True,
    )
  }
  to_create = []

  for recurring_config in recurring_configs.values():
    active_keys.add(recurring_config.key)
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

    changed_fields = []
    for field, value in desired.items():
      if getattr(existing_task, field) == value:
        continue
      setattr(existing_task, field, value)
      changed_fields.append(field)

    if changed_fields:
      existing_task.save(using=alias, update_fields=[*changed_fields, "updated_at"])

  if to_create:
    RecurringTask.objects.using(alias).bulk_create(to_create)

  queryset = RecurringTask.objects.using(alias).filter(backend_alias=backend_alias, static=True)
  if active_keys:
    queryset = queryset.exclude(key__in=active_keys)
  queryset.delete()


def fire_recurring_task(recurring_task, run_at, *, backend_alias="default"):
  alias = get_database_alias(backend_alias)

  with transaction.atomic(using=alias):
    created = create_ignore_conflicts(
      RecurringExecution,
      using=alias,
      backend_alias=backend_alias,
      task_key=recurring_task.key,
      run_at=run_at,
    )
    if not created:
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
    return execution
