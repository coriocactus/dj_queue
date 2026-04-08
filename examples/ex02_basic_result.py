#!/usr/bin/env -S uv run

"""enqueue a task, execute it, and inspect the result."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _settings
from _example import result, status_name, step, takeaway, title

from django.tasks import task

from dj_queue.operations.jobs import claim_ready_jobs, execute_claimed_job


@task
def add(a, b):
  return a + b


title("ex02", "enqueue a task, execute it, and inspect the stored result")

step(1, "enqueue one task")
task_result = add.enqueue(3, 7)
result(
  f"backend={_settings.DB_BACKEND} job_id={task_result.id} status={status_name(task_result.status)}"
)

step(2, "claim the ready job and execute it")
jobs = claim_ready_jobs(limit=1)
execute_claimed_job(jobs[0].id)
result(f"claimed_job_id={jobs[0].id}")
result("execution finished without raising an error")

step(3, "read the persisted task result")
fresh_result = add.get_backend().get_result(task_result.id)
result(f"status={status_name(fresh_result.status)}")
result(f"return_value={fresh_result.return_value}")

takeaway("claiming and executing a job writes the return value back to the queue database")
