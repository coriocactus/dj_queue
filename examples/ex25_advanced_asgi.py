#!/usr/bin/env -S uv run

"""start and stop dj_queue through the ASGI lifespan protocol."""

import asyncio
import os
import tempfile
from pathlib import Path

from _example import ensure_project_on_path, result, status_name, step, takeaway, title, wait_until

ensure_project_on_path()

tempdir = tempfile.TemporaryDirectory()
os.environ["DJ_QUEUE_EXAMPLE_SERVER_DB"] = str(Path(tempdir.name) / "asgi.sqlite3")

from examples import _server_app

_server_app.prepare_database()


async def main():
  title("ex25", "start and stop an embedded supervisor through the ASGI lifespan protocol")

  sent_messages = []
  incoming = asyncio.Queue()

  async def receive():
    return await incoming.get()

  async def send(message):
    sent_messages.append(message)

  app_task = asyncio.create_task(_server_app.asgi_application({"type": "lifespan"}, receive, send))

  step(1, "send lifespan.startup and wait for the embedded supervisor to start")
  await incoming.put({"type": "lifespan.startup"})
  await asyncio.to_thread(
    wait_until,
    lambda: sent_messages and sent_messages[-1]["type"] == "lifespan.startup.complete",
  )
  result(f"lifespan_event={sent_messages[-1]['type']}")

  step(2, "enqueue one task while the lifespan wrapper is running")
  task_result = await asyncio.to_thread(_server_app.echo.enqueue, "asgi")
  result(f"job_id={task_result.id} initial_status={status_name(task_result.status)}")

  def result_ready():
    fresh_result = _server_app.echo.get_backend().get_result(task_result.id)
    if status_name(fresh_result.status) != "successful":
      return None
    return fresh_result

  fresh_result = await asyncio.to_thread(wait_until, result_ready, timeout=5.0)
  result(f"status={status_name(fresh_result.status)}")
  result(f"return_value={fresh_result.return_value}")

  step(3, "send lifespan.shutdown and wait for the supervisor to stop")
  await incoming.put({"type": "lifespan.shutdown"})
  await asyncio.to_thread(
    wait_until,
    lambda: sent_messages and sent_messages[-1]["type"] == "lifespan.shutdown.complete",
  )
  await app_task
  result(f"lifespan_event={sent_messages[-1]['type']}")

  takeaway(
    "DjQueueLifespan starts and stops the embedded async supervisor with the ASGI server lifecycle"
  )


asyncio.run(main())
