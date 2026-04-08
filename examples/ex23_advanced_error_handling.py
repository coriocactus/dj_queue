#!/usr/bin/env -S uv run

"""failed job inspection, retry, and discard."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _settings  # noqa: F401
from _example import result, step, takeaway, title

from django.tasks import task

from dj_queue.models import FailedExecution, Job, ReadyExecution
from dj_queue.operations.jobs import (
  claim_ready_jobs,
  discard_failed_job,
  execute_claimed_job,
  retry_failed_job,
)


@task
def flaky(attempt_marker):
  raise ConnectionError(f"connection refused (marker={attempt_marker})")


title("ex23", "inspect a failed job, retry it, and then discard it")

step(1, "execute one job that raises an exception")
task_result = flaky.enqueue("first")
jobs = claim_ready_jobs(limit=1)
execute_claimed_job(jobs[0].id)
job = Job.objects.select_related("failed_execution").get(pk=task_result.id)
result(f"failed={job.failed}")
result(f"exception_class={job.failed_execution.exception_class}")
result(f"message={job.failed_execution.message}")

step(2, "retry the failed job back into the ready queue")
retry_failed_job(job.id)
job.refresh_from_db()
result(f"ready_after_retry={job.ready}")
result(f"failed_after_retry={job.failed}")
result(f"ready_executions={ReadyExecution.objects.count()}")

step(3, "execute the job again and discard the failed row")
jobs = claim_ready_jobs(limit=1)
execute_claimed_job(jobs[0].id)
discard_failed_job(job.id)
result(f"job_exists_after_discard={Job.objects.filter(pk=job.id).exists()}")
result(f"failed_executions={FailedExecution.objects.count()}")

takeaway(
  "failed jobs stay inspectable until you retry or discard them through the operations layer"
)
