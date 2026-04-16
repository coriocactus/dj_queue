import asyncio

from dj_queue.runtime.supervisor import AsyncSupervisor


def build_supervisor(backend_alias="default"):
  return AsyncSupervisor.from_backend_config(backend_alias=backend_alias, standalone=False)


class DjQueueLifespan:
  def __init__(self, app, *, backend_alias="default"):
    self.app = app
    self.backend_alias = backend_alias
    self.supervisor = None
    self._poll_task = None

  async def _poll_supervisor(self):
    while self.supervisor is not None:
      await asyncio.to_thread(self.supervisor.poll_once)
      await asyncio.sleep(self.supervisor.polling_interval)

  async def __call__(self, scope, receive, send):
    if scope["type"] != "lifespan":
      await self.app(scope, receive, send)
      return

    while True:
      message = await receive()
      if message["type"] == "lifespan.startup":
        self.supervisor = build_supervisor(self.backend_alias)
        await asyncio.to_thread(self.supervisor.start)
        self._poll_task = asyncio.create_task(self._poll_supervisor())
        await send({"type": "lifespan.startup.complete"})
      elif message["type"] == "lifespan.shutdown":
        poll_task = self._poll_task
        self._poll_task = None
        if poll_task is not None:
          poll_task.cancel()
          try:
            await poll_task
          except asyncio.CancelledError:
            pass
        if self.supervisor is not None:
          await asyncio.to_thread(self.supervisor.stop)
          self.supervisor = None
        await send({"type": "lifespan.shutdown.complete"})
        return
