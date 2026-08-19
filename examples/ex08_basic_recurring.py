#!/usr/bin/env -S uv run

"""configure static recurring tasks via settings and inspect the schedule."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _settings  # noqa: F401
from _example import result, step, takeaway, title
from django.conf import settings
from django.tasks import task

from dj_queue.models import RecurringTask
from dj_queue.runtime.scheduler import Scheduler


@task
def cleanup():
  return "cleanup"


@task
def digest():
  return "digest"


title("ex08", "configure static recurring tasks and inspect the stored schedule")

step(1, "define recurring tasks in TASKS settings")
settings.TASKS["default"]["OPTIONS"]["recurring"] = {
  "nightly_cleanup": {
    "task_path": cleanup.module_path,
    "schedule": "0 3 * * *",
    "queue_name": "maintenance",
    "priority": -5,
    "description": "nightly cleanup",
  },
  "hourly_digest": {
    "task_path": digest.module_path,
    "schedule": "0 * * * *",
    "queue_name": "default",
    "description": "hourly email digest",
  },
}
result("configured keys=nightly_cleanup, hourly_digest")

step(2, "persist the static recurring schedule")
scheduler = Scheduler.from_backend_config(
  backend_alias="default",
  tasks_settings=settings.TASKS,
  name="example-static-recurring",
  pid=12345,
  hostname="localhost",
)
scheduler.sync_static_tasks()
for recurring_task in RecurringTask.objects.order_by("key"):
  result(
    f"key={recurring_task.key} schedule={recurring_task.schedule} "
    f"queue={recurring_task.queue_name} static={recurring_task.static}"
  )

takeaway(
  "static recurring settings are copied into recurring task rows that the scheduler can execute"
)
