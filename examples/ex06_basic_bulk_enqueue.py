#!/usr/bin/env -S uv run

"""enqueue multiple tasks in a single call with enqueue_all."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _settings  # noqa: F401
from _example import result, status_name, step, takeaway, title

from django.tasks import task

from dj_queue.models import ReadyExecution


@task
def process_item(item_id):
  return item_id


title("ex06", "submit multiple jobs in one backend call")

step(1, "bulk enqueue five tasks with enqueue_all")
results = process_item.get_backend().enqueue_all([(process_item, [i], {}) for i in range(5)])
result(f"enqueued_count={len(results)}")

step(2, "inspect the returned task results and ready queue size")
for task_result in results:
  result(f"job_id={task_result.id} status={status_name(task_result.status)}")
result(f"ready_executions={ReadyExecution.objects.count()}")

takeaway("enqueue_all batches submission work while still returning one TaskResult per job")
