from dj_queue.runtime.supervisor import AsyncSupervisor


def build_supervisor(backend_alias="default"):
  return AsyncSupervisor.from_backend_config(backend_alias=backend_alias, standalone=False)


def post_fork(_server, worker):
  if worker.age != 1:
    return None

  supervisor = build_supervisor()
  worker._dj_queue_supervisor = supervisor
  supervisor.start()
  return supervisor


def worker_exit(_server, worker):
  supervisor = getattr(worker, "_dj_queue_supervisor", None)
  if supervisor is None:
    return None

  supervisor.stop()
  worker._dj_queue_supervisor = None
  return None
