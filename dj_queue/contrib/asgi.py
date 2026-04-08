from dj_queue.runtime.supervisor import AsyncSupervisor


def build_supervisor(backend_alias="default"):
  return AsyncSupervisor.from_backend_config(backend_alias=backend_alias, standalone=False)


class DjQueueLifespan:
  def __init__(self, app, *, backend_alias="default"):
    self.app = app
    self.backend_alias = backend_alias
    self.supervisor = None

  async def __call__(self, scope, receive, send):
    if scope["type"] != "lifespan":
      await self.app(scope, receive, send)
      return

    while True:
      message = await receive()
      if message["type"] == "lifespan.startup":
        self.supervisor = build_supervisor(self.backend_alias)
        self.supervisor.start()
        await send({"type": "lifespan.startup.complete"})
      elif message["type"] == "lifespan.shutdown":
        if self.supervisor is not None:
          self.supervisor.stop()
          self.supervisor = None
        await send({"type": "lifespan.shutdown.complete"})
        return
