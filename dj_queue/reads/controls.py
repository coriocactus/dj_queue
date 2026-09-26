from django.db.models import (
  Count,
  IntegerField,
  Max,
  OuterRef,
  Subquery,
  Value,
)
from django.db.models.functions import Coalesce

from dj_queue.cron import next_cron_run
from dj_queue.db import get_database_alias
from dj_queue.models import (
  BlockedExecution,
  RecurringExecution,
  RecurringTask,
  Semaphore,
)


def recurring_rows_for_backend(*, backend_alias, now):
  alias = get_database_alias(backend_alias)
  last_runs = dict(
    RecurringExecution.objects.using(alias)
    .filter(backend_alias=backend_alias)
    .values_list("task_key")
    .annotate(last_run_at=Max("run_at"))
  )
  return [
    {
      "key": task.key,
      "task_path": task.task_path,
      "queue_name": task.queue_name,
      "schedule": task.schedule,
      "static": task.static,
      "last_run_at": last_runs.get(task.key),
      "next_run_at": task.next_run_at or next_run_at(task.schedule, now),
    }
    for task in RecurringTask.objects.using(alias)
    .filter(backend_alias=backend_alias)
    .order_by("key")
  ]


def semaphore_rows_for_backend(*, backend_alias):
  alias = get_database_alias(backend_alias)
  waiters = _counts_by_value(
    BlockedExecution.objects.using(alias),
    field_name="concurrency_key",
  )
  return [
    {
      "scope": "queue_database",
      "queue_database_alias": alias,
      "key": semaphore.key,
      "active_count": semaphore.occupied_count,
      "available_slots": semaphore.available_count,
      "limit": semaphore.limit,
      "blocked_waiters": waiters.get(semaphore.key, 0),
      "expires_at": semaphore.expires_at,
    }
    for semaphore in Semaphore.objects.using(alias).order_by("key")
  ]


def semaphore_blocked_waiter_count_expression(alias):
  blocked_waiters = (
    BlockedExecution.objects.using(alias)
    .filter(concurrency_key=OuterRef("key"))
    .values("concurrency_key")
    .annotate(total=Count("id"))
    .values("total")[:1]
  )
  return Coalesce(
    Subquery(blocked_waiters, output_field=IntegerField()),
    Value(0),
  )


def next_run_at(schedule, now):
  return next_cron_run(schedule, now)


def _counts_by_value(queryset, *, field_name):
  return {
    row[field_name]: row["count"]
    for row in queryset.values(field_name).annotate(count=Count("id"))
  }
