#!/usr/bin/env -S uv run

"""queue introspection and control: pause, resume, clear, size, latency."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _settings  # noqa: F401
from _example import result, step, takeaway, title

from django.tasks import task

from dj_queue.api import QueueInfo
from dj_queue.operations.jobs import claim_ready_jobs


@task
def work(n):
  return n


title("ex21", "inspect queues, pause them, and clear queued work")

step(1, "enqueue jobs into two named queues")
for i in range(5):
  work.using(queue_name="orders").enqueue(i)
for i in range(3):
  work.using(queue_name="emails").enqueue(i)
for queue_info in QueueInfo.all():
  result(
    f"queue={queue_info.queue_name} size={queue_info.size} "
    f"latency={queue_info.latency:.3f}s paused={queue_info.paused}"
  )

step(2, "pause the orders queue and show that claims skip it")
orders = QueueInfo("orders")
orders.pause()
claimed = claim_ready_jobs(limit=10, queues=["orders"])
result(f"orders_paused={orders.paused}")
result(f"claimed_from_paused_orders={len(claimed)}")

step(3, "resume and clear the orders queue")
orders.resume()
cleared = orders.clear()
result(f"orders_paused_after_resume={orders.paused}")
result(f"cleared_jobs={cleared}")
result(f"orders_size_after_clear={orders.size}")

takeaway("QueueInfo exposes operational controls without bypassing the queue tables")
