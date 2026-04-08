from datetime import datetime, timedelta
from uuid import uuid4

import pytest
from django.utils import timezone

from dj_queue.api import schedule_recurring_task, unschedule_recurring_task
from dj_queue.models import Job, RecurringExecution, RecurringTask
from dj_queue.runtime.scheduler import Scheduler


pytestmark = pytest.mark.django_db(transaction=True)


def fixed_now():
  return datetime(2026, 4, 8, 12, 0, 1, tzinfo=timezone.get_current_timezone())


def scheduler_tasks_settings():
  return {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {
        "workers": [{"queues": "*", "threads": 1, "processes": 1, "polling_interval": 0.1}],
        "dispatchers": [],
        "scheduler": {
          "dynamic_tasks_enabled": True,
          "polling_interval": 5,
        },
        "recurring": {},
      },
    }
  }


def build_scheduler(name=None):
  return Scheduler.from_backend_config(
    backend_alias="default",
    tasks_settings=scheduler_tasks_settings(),
    name=name or f"scheduler-{uuid4()}",
    pid=34567,
    hostname="localhost",
  )


def test_many_dynamic_recurring_add_remove_no_timer_leak():
  now = fixed_now()
  later = now + timedelta(minutes=1)
  scheduler = build_scheduler()
  scheduler.start()

  removed_keys = set()
  active_keys = set()
  expected_values = set()

  try:
    for index in range(90):
      schedule_recurring_task(
        key=f"dynamic-{index:03d}",
        task_path="tests.tasks.echo",
        schedule="*/5 * * * *",
        args=(f"initial-{index}",),
      )

    for index in range(90):
      key = f"dynamic-{index:03d}"
      if index % 3 == 0:
        unschedule_recurring_task(key)
        removed_keys.add(key)
        continue

      active_keys.add(key)
      if index % 3 == 1:
        value = f"updated-{index}"
        schedule_recurring_task(
          key=key,
          task_path="tests.tasks.echo",
          schedule="* * * * *",
          args=(value,),
        )
      else:
        value = f"stable-{index}"
        schedule_recurring_task(
          key=key,
          task_path="tests.tasks.echo",
          schedule="*/5 * * * *",
          args=(value,),
        )
      expected_values.add(value)

    fired_jobs = scheduler.poll_once(now=now)

    assert RecurringTask.objects.count() == len(active_keys)
    assert set(RecurringTask.objects.values_list("key", flat=True)) == active_keys
    assert set(Job.objects.values_list("payload__args__0", flat=True)) == expected_values
    assert {job.payload["args"][0] for job in fired_jobs} == expected_values
    assert len(fired_jobs) == len(expected_values)
    assert RecurringExecution.objects.count() == len(expected_values)
    assert removed_keys.isdisjoint(RecurringTask.objects.values_list("key", flat=True)) is True

    assert scheduler.poll_once(now=now) == []
    assert Job.objects.count() == len(expected_values)
    assert RecurringExecution.objects.count() == len(expected_values)

    for key in active_keys:
      unschedule_recurring_task(key)

    assert RecurringTask.objects.count() == 0
    assert scheduler.poll_once(now=later) == []
    assert Job.objects.count() == len(expected_values)
    assert RecurringExecution.objects.count() == len(expected_values)
  finally:
    scheduler.stop()
