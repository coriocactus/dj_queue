from django.db import IntegrityError, transaction
from django.utils.module_loading import import_string

from dj_queue.db import get_database_alias
from dj_queue.models import RecurringExecution, RecurringTask
from dj_queue.operations.jobs import enqueue_job


def upsert_static_recurring_tasks(recurring_configs, *, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  active_keys = set()

  for recurring_config in recurring_configs.values():
    active_keys.add(recurring_config.key)
    RecurringTask.objects.using(alias).update_or_create(
      key=recurring_config.key,
      defaults={
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
      },
    )

  queryset = RecurringTask.objects.using(alias).filter(static=True)
  if active_keys:
    queryset = queryset.exclude(key__in=active_keys)
  queryset.delete()


def fire_recurring_task(recurring_task, run_at, *, backend_alias="default"):
  alias = get_database_alias(backend_alias)

  with transaction.atomic(using=alias):
    try:
      execution = RecurringExecution.objects.using(alias).create(
        task_key=recurring_task.key,
        run_at=run_at,
      )
    except IntegrityError:
      return None

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
