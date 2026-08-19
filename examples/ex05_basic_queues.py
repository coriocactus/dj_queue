#!/usr/bin/env -S uv run

"""route tasks to named queues and claim from specific queues."""

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


title("ex05", "route work to named queues and claim only the queue you want")

step(1, "enqueue work into the email and export queues")
work.using(queue_name="email").enqueue("welcome")
work.using(queue_name="email").enqueue("receipt")
work.using(queue_name="export").enqueue("csv")
result("queued labels=welcome, receipt, csv")

step(2, "claim only the email queue")
email_jobs = claim_ready_jobs(limit=10, queues=["email"])
for claimed_job in email_jobs:
  result(f"queue={claimed_job.job.queue_name} payload={claimed_job.job.payload}")

step(3, "claim only the export queue")
export_jobs = claim_ready_jobs(limit=10, queues=["export"])
for claimed_job in export_jobs:
  result(f"queue={claimed_job.job.queue_name} payload={claimed_job.job.payload}")

takeaway("queue selectors let one worker pool ignore work that belongs to another queue")
