#!/usr/bin/env -S uv run

"""schedule and unschedule recurring tasks at runtime."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _settings  # noqa: F401
from _example import result, step, takeaway, title
from django.tasks import task

from dj_queue.api import schedule_recurring_task, unschedule_recurring_task
from dj_queue.models import RecurringTask


@task
def generate_report(tenant_id):
  return tenant_id


title("ex22", "create, update, and remove recurring tasks at runtime")

step(1, "schedule one recurring task")
scheduled = schedule_recurring_task(
  key="tenant_42_report",
  task_path=generate_report.module_path,
  schedule="30 9 * * 1-5",
  kwargs={"tenant_id": 42},
  queue_name="reports",
  description="weekday morning report for tenant 42",
)
result(f"key={scheduled.key} schedule={scheduled.schedule} static={scheduled.static}")

step(2, "update the same recurring task in place")
schedule_recurring_task(
  key="tenant_42_report",
  task_path=generate_report.module_path,
  schedule="0 8 * * 1-5",
  kwargs={"tenant_id": 42},
  queue_name="reports",
  description="updated to 8am",
)
result(f"updated_schedule={RecurringTask.objects.get(key='tenant_42_report').schedule}")

step(3, "unschedule the runtime task")
deleted = unschedule_recurring_task("tenant_42_report")
result(f"deleted_rows={deleted}")
result(f"remaining_recurring_tasks={RecurringTask.objects.count()}")

takeaway("runtime recurring APIs let operators change schedules without editing Django settings")
