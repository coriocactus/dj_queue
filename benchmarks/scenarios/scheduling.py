from datetime import timedelta
from uuid import uuid4

from django.utils import timezone

from benchmarks.harness import Timer, throughput
from benchmarks.tasks import noop
from dj_queue.config import BackendConfig, SchedulerConfig
from dj_queue.models import (
  Job,
  ReadyExecution,
  RecurringExecution,
  RecurringTask,
  ScheduledExecution,
)
from dj_queue.operations.jobs import promote_scheduled_jobs
from dj_queue.runtime.scheduler import Scheduler


def scheduled_promotion(size):
  now = timezone.now()
  due_at = now - timedelta(seconds=1)
  future_at = now + timedelta(days=1)
  due_jobs = [_scheduled_job(f"due-{index}", due_at, now=now) for index in range(size)]
  future_jobs = [_scheduled_job(f"future-{index}", future_at, now=now) for index in range(size)]
  Job.objects.bulk_create([*due_jobs, *future_jobs], batch_size=1000)
  ScheduledExecution.objects.bulk_create(
    [_scheduled_row(job, due_at) for job in due_jobs]
    + [_scheduled_row(job, future_at) for job in future_jobs],
    batch_size=1000,
  )

  promoted = 0
  with Timer() as timer:
    while True:
      jobs = promote_scheduled_jobs(batch_size=1000)
      if not jobs:
        break
      promoted += len(jobs)

  ready_count = ReadyExecution.objects.count()
  scheduled_count = ScheduledExecution.objects.count()
  if promoted != size or ready_count != size or scheduled_count != size:
    raise AssertionError(
      f"expected {size} promoted/ready/future scheduled rows, got "
      f"{promoted}/{ready_count}/{scheduled_count}"
    )

  return {
    "duration_seconds": timer.duration,
    "rows_per_second": throughput(promoted, timer.duration),
    "promoted_count": promoted,
    "ready_count": ready_count,
    "future_scheduled_count": scheduled_count,
  }


def recurring_scale(size):
  now = timezone.now().replace(second=1, microsecond=0)
  not_due_at = now + timedelta(days=1)
  RecurringTask.objects.bulk_create(
    [
      RecurringTask(
        backend_alias="default",
        key=f"not-due-{index}",
        task_path=noop.module_path,
        payload={"args": [index], "kwargs": {}},
        schedule="* * * * *",
        queue_name=noop.queue_name,
        priority=noop.priority,
        static=False,
        next_run_at=not_due_at,
      )
      for index in range(size)
    ],
    batch_size=1000,
  )
  scheduler = Scheduler(
    BackendConfig(
      scheduler=SchedulerConfig(dynamic_tasks_enabled=True, polling_interval=5),
      workers=(),
      dispatchers=(),
      recurring={},
      process_heartbeat_interval=0,
      preserve_finished_jobs=True,
      clear_finished_jobs_after=None,
      clear_failed_jobs_after=None,
      clear_recurring_executions_after=None,
    ),
    backend_alias="default",
    name=f"benchmark-scheduler-{uuid4()}",
    pid=34567,
    hostname="benchmark",
  )

  try:
    with Timer() as timer:
      fired_jobs = scheduler.poll_once(now=now)
  finally:
    scheduler.stop()

  if fired_jobs or Job.objects.exists() or RecurringExecution.objects.exists():
    raise AssertionError("not-due recurring tasks should not fire")

  return {
    "duration_seconds": timer.duration,
    "rows_per_second": throughput(size, timer.duration),
    "recurring_task_count": RecurringTask.objects.count(),
    "fired_count": len(fired_jobs),
  }


def _scheduled_job(value, scheduled_at, *, now):
  return Job(
    task_path=noop.module_path,
    queue_name=noop.queue_name,
    priority=noop.priority,
    payload={"args": [value], "kwargs": {}},
    backend_alias="default",
    scheduled_at=scheduled_at,
    created_at=now,
    updated_at=now,
  )


def _scheduled_row(job, scheduled_at):
  return ScheduledExecution(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
    scheduled_at=scheduled_at,
  )
