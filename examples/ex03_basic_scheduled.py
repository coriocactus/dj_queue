#!/usr/bin/env -S uv run

"""enqueue a task with run_after to defer execution."""

import os
import sys
from datetime import timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _settings  # noqa: F401
from _example import result, step, takeaway, title
from django.tasks import task
from django.utils import timezone

from dj_queue.models import Job, ReadyExecution, ScheduledExecution


@task
def report(name):
  return f"report: {name}"


title("ex03", "defer a task with run_after and compare it to an immediate enqueue")

step(1, "enqueue one task for the future")
future = timezone.now() + timedelta(hours=1)
scheduled_result = report.using(run_after=future).enqueue("daily")
scheduled_job = Job.objects.get(pk=scheduled_result.id)
result(f"scheduled_at={scheduled_job.scheduled_at}")
result(
  f"ready_executions={ReadyExecution.objects.count()} scheduled_executions={ScheduledExecution.objects.count()}"
)

step(2, "enqueue the same task without run_after")
immediate_result = report.enqueue("immediate")
immediate_job = Job.objects.get(pk=immediate_result.id)
result(f"immediate_job_id={immediate_result.id}")
result(f"ready={immediate_job.ready} scheduled={immediate_job.scheduled}")

takeaway("run_after keeps work out of the ready queue until the scheduled time arrives")
