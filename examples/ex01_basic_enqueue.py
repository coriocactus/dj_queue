#!/usr/bin/env -S uv run

"""enqueue a task and inspect the ready queue."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _settings
from _example import result, status_name, step, takeaway, title
from django.tasks import task

from dj_queue.models import Job, ReadyExecution


@task
def greet(name):
  return f"hello, {name}"


title("ex01", "enqueue a task and inspect the ready queue")

step(1, "enqueue one task through the backend")
task_result = greet.enqueue("world")
result(
  f"backend={_settings.DB_BACKEND} job_id={task_result.id} status={status_name(task_result.status)}"
)

step(2, "read the stored job and ready execution rows")
job = Job.objects.get(pk=task_result.id)
result(f"task_path={job.task_path}")
result(f"payload={job.payload}")
result(f"ready={job.ready} ready_executions={ReadyExecution.objects.count()}")

takeaway("enqueue writes the job row immediately and places the job in the ready queue")
