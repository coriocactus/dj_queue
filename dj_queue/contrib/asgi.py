import asyncio
import inspect
from contextlib import suppress

from dj_queue.runtime.errors import handle_thread_error
from dj_queue.runtime.supervisor import AsyncSupervisor


def build_supervisor(backend_alias="default"):
  return AsyncSupervisor.from_backend_config(backend_alias=backend_alias, standalone=False)


class DjQueueLifespan:
  def __init__(self, app, *, backend_alias="default"):
    self.app = app
    self.backend_alias = backend_alias
    self.supervisor = None
    self._poll_task = None
    self._poll_stop = None

  async def _poll_supervisor(self):
    while self.supervisor is not None and self._poll_stop is not None and not self._poll_stop.is_set():
      try:
        await asyncio.to_thread(self.supervisor.poll_once)
      except Exception as error:
        handle_thread_error(
          error,
          context="supervisor.run",
          backend_alias=self.supervisor.backend_alias,
        )

      if self.supervisor is None or self._poll_stop is None or self._poll_stop.is_set():
        return

      try:
        await asyncio.wait_for(self._poll_stop.wait(), timeout=self.supervisor.polling_interval)
      except asyncio.TimeoutError:
        continue

  async def _start_wrapped_lifespan_app(self, scope, receive, send):
    result = self.app(scope, receive, send)
    if inspect.isawaitable(result):
      return asyncio.create_task(result)

    loop = asyncio.get_running_loop()
    task = loop.create_future()
    task.set_result(result)
    return task

  async def _forward_lifespan_message(self, app_task, receive_queue, send_queue, message):
    if app_task.done():
      await app_task
      return None

    await receive_queue.put(message)
    response_task = asyncio.create_task(send_queue.get())
    try:
      done, _ = await asyncio.wait({app_task, response_task}, return_when=asyncio.FIRST_COMPLETED)
      if response_task in done:
        return response_task.result()
      await app_task
      return None
    finally:
      if not response_task.done():
        response_task.cancel()
        with suppress(asyncio.CancelledError):
          await response_task

  async def _start_supervisor(self):
    self.supervisor = build_supervisor(self.backend_alias)
    await asyncio.to_thread(self.supervisor.start)
    self._poll_stop = asyncio.Event()
    self._poll_task = asyncio.create_task(self._poll_supervisor())

  async def _stop_supervisor(self):
    poll_stop = self._poll_stop
    self._poll_stop = None
    if poll_stop is not None:
      poll_stop.set()

    poll_task = self._poll_task
    self._poll_task = None
    if poll_task is not None:
      await poll_task

    if self.supervisor is not None:
      await asyncio.to_thread(self.supervisor.stop)
      self.supervisor = None

  async def __call__(self, scope, receive, send):
    if scope["type"] != "lifespan":
      await self.app(scope, receive, send)
      return

    receive_queue = asyncio.Queue()
    send_queue = asyncio.Queue()
    app_task = await self._start_wrapped_lifespan_app(scope, receive_queue.get, send_queue.put)

    try:
      while True:
        message = await receive()
        if message["type"] == "lifespan.startup":
          response = await self._forward_lifespan_message(app_task, receive_queue, send_queue, message)
          if response is not None and response["type"] != "lifespan.startup.complete":
            await send(response)
            return

          await self._start_supervisor()
          await send({"type": "lifespan.startup.complete"})
        elif message["type"] == "lifespan.shutdown":
          await self._stop_supervisor()
          response = await self._forward_lifespan_message(app_task, receive_queue, send_queue, message)
          if response is None:
            response = {"type": "lifespan.shutdown.complete"}
          await send(response)
          await app_task
          return
    finally:
      await self._stop_supervisor()
      if not app_task.done():
        app_task.cancel()
        with suppress(asyncio.CancelledError):
          await app_task
