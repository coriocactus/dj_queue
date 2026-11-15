import asyncio
import inspect
import threading
from contextlib import suppress

from dj_queue.runtime.supervisor import AsyncSupervisor


class DjQueueLifespan:
  def __init__(self, app, *, backend_alias="default", forward_wrapped_lifespan=True):
    self.app = app
    self.backend_alias = backend_alias
    self.forward_wrapped_lifespan = forward_wrapped_lifespan
    self.supervisor = None
    self._supervisor_thread = None

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
    supervisor = AsyncSupervisor.from_backend_config(
      backend_alias=self.backend_alias, standalone=False
    )
    await asyncio.to_thread(supervisor.start)
    self.supervisor = supervisor
    self._supervisor_thread = threading.Thread(target=supervisor.run, daemon=True)
    self._supervisor_thread.start()

  @staticmethod
  def _is_unsupported_lifespan_error(error):
    message = str(error).lower()
    return isinstance(error, ValueError) and (
      "django can only handle asgi/http connections" in message and "lifespan" in message
    )

  async def _stop_supervisor(self):
    supervisor, self.supervisor = self.supervisor, None
    if supervisor is not None:
      supervisor.request_stop()
      await asyncio.to_thread(self._supervisor_thread.join)

  async def __call__(self, scope, receive, send):
    if scope["type"] != "lifespan":
      await self.app(scope, receive, send)
      return

    receive_queue = asyncio.Queue()
    send_queue = asyncio.Queue()
    wrapped_app_supports_lifespan = self.forward_wrapped_lifespan
    app_task = None
    if wrapped_app_supports_lifespan:
      app_task = await self._start_wrapped_lifespan_app(scope, receive_queue.get, send_queue.put)

    try:
      while True:
        message = await receive()
        if message["type"] == "lifespan.startup":
          response = None
          if wrapped_app_supports_lifespan:
            try:
              response = await self._forward_lifespan_message(
                app_task, receive_queue, send_queue, message
              )
            except Exception as error:
              if not self._is_unsupported_lifespan_error(error):
                raise
              wrapped_app_supports_lifespan = False
          if response is not None and response["type"] != "lifespan.startup.complete":
            await send(response)
            return

          await self._start_supervisor()
          await send({"type": "lifespan.startup.complete"})
        elif message["type"] == "lifespan.shutdown":
          await self._stop_supervisor()
          response = None
          if wrapped_app_supports_lifespan:
            response = await self._forward_lifespan_message(
              app_task, receive_queue, send_queue, message
            )
          if response is None:
            response = {"type": "lifespan.shutdown.complete"}
          await send(response)
          if wrapped_app_supports_lifespan and app_task is not None:
            await app_task
          return
    finally:
      await self._stop_supervisor()
      if app_task is not None and not app_task.done():
        app_task.cancel()
        with suppress(asyncio.CancelledError):
          await app_task
