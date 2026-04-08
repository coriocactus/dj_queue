from functools import partial

from django.db import transaction

from dj_queue.db import get_database_alias
from dj_queue.models import RecurringTask


def enqueue_on_commit(task, *args, using=None, **kwargs):
  transaction.on_commit(partial(task.enqueue, *args, **kwargs), using=using)


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

  recurring_task, _ = RecurringTask.objects.using(alias).update_or_create(
    key=key,
    defaults={
      "task_path": task_path,
      "payload": {"args": list(args), "kwargs": dict(kwargs)},
      "schedule": schedule,
      "queue_name": queue_name,
      "priority": priority,
      "description": description,
      "static": False,
    },
  )
  recurring_task.full_clean()
  recurring_task.save(using=alias)
  return recurring_task


def unschedule_recurring_task(key, *, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  queryset = RecurringTask.objects.using(alias).filter(key=key, static=False)
  deleted = queryset.count()
  queryset.delete()
  return deleted
