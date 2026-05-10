#!/usr/bin/env -S uv run

"""enqueue tasks with different priorities and observe claim order."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _settings  # noqa: F401
from _example import result, step, takeaway, title

from django.tasks import task

from dj_queue.api import claim_ready_jobs


@task
def work(label):
  return label


title("ex04", "show that higher-priority jobs are claimed first")

step(1, "enqueue three jobs with different priorities")
work.using(priority=-10).enqueue("low")
work.using(priority=0).enqueue("normal")
work.using(priority=10).enqueue("high")
result("enqueued labels=low, normal, high")

step(2, "claim the batch and inspect the order")
claimed_jobs = claim_ready_jobs(limit=3)
for claimed_job in claimed_jobs:
  result(f"priority={claimed_job.job.priority:+d} payload={claimed_job.job.payload}")

takeaway("ready jobs are claimed in descending priority order")
