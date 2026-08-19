#!/usr/bin/env -S uv run

"""enqueue a task only after the surrounding transaction commits."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _settings  # noqa: F401
from _example import result, step, takeaway, title
from django.db import transaction
from django.tasks import task

from dj_queue.api import enqueue_on_commit
from dj_queue.models import ReadyExecution


@task
def notify(message):
  return message


title("ex07", "defer enqueue until the surrounding transaction commits")

step(1, "register an on-commit enqueue inside a transaction")
with transaction.atomic():
  enqueue_on_commit(notify, "order confirmed")
  result(f"ready_executions_inside_transaction={ReadyExecution.objects.count()}")

step(2, "inspect the queue after the transaction commits")
result(f"ready_executions_after_commit={ReadyExecution.objects.count()}")

takeaway(
  "enqueue_on_commit prevents queue side effects from escaping a transaction that later rolls back"
)
