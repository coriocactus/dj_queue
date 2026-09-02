#!/usr/bin/env -S uv run

"""concurrency controls: concurrency_key, concurrency_limit, on_conflict."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _settings  # noqa: F401
from _example import result, status_name, step, takeaway, title
from django.tasks import task

from dj_queue.models import BlockedExecution, Job, ReadyExecution, Semaphore


@task
def process_account(account_id, action):
  return f"{account_id}:{action}"


@task
def singleton_job(key):
  return key


title("ex20", "apply concurrency limits and inspect blocked or discarded work")

step(1, "configure a per-account semaphore and enqueue competing jobs")
process_account.func.concurrency_key = "account:{account_id}"
process_account.func.concurrency_limit = 1
process_account.func.concurrency_duration = 60
first = process_account.enqueue("42", "sync")
second = process_account.enqueue("42", "export")
third = process_account.enqueue("99", "sync")
result(f"job_1_ready={Job.objects.get(pk=first.id).ready}")
result(f"job_2_blocked={Job.objects.get(pk=second.id).blocked}")
result(f"job_3_ready={Job.objects.get(pk=third.id).ready}")

step(2, "inspect the queue and semaphore state")
result(f"ready_executions={ReadyExecution.objects.count()}")
result(f"blocked_executions={BlockedExecution.objects.count()}")
for semaphore in Semaphore.objects.order_by("key"):
  result(
    f"semaphore key={semaphore.key} active={semaphore.active_count} "
    f"available={semaphore.available_count} limit={semaphore.limit}"
  )

step(3, "switch to on_conflict=discard for singleton work")
singleton_job.func.concurrency_key = "singleton:{key}"
singleton_job.func.concurrency_limit = 1
singleton_job.func.on_conflict = "discard"
discarded_first = singleton_job.enqueue("report")
discarded_second = singleton_job.enqueue("report")
result(f"singleton_job_1_status={status_name(discarded_first.status)}")
result(f"singleton_job_2_status={status_name(discarded_second.status)}")

takeaway(
  "concurrency keys can block duplicate work or discard it immediately, depending on policy"
)
