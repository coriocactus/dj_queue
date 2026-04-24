import threading

from dj_queue.runtime.supervisor import AsyncSupervisor


def build_supervisor(backend_alias="default"):
  return AsyncSupervisor.from_backend_config(backend_alias=backend_alias, standalone=False)


def post_fork(_server, worker):
  if worker.age != 1:
    return None

  supervisor = build_supervisor()
  worker._dj_queue_supervisor = supervisor
  worker._dj_queue_supervisor_poll_stop = threading.Event()
  supervisor.start()

  def poll_supervisor():
    stop_event = worker._dj_queue_supervisor_poll_stop
    while stop_event.wait(supervisor.polling_interval) is False:
      supervisor.poll_once()

  poll_thread = threading.Thread(target=poll_supervisor, daemon=True)
  worker._dj_queue_supervisor_poll_thread = poll_thread
  poll_thread.start()
  return supervisor


def worker_exit(_server, worker):
  supervisor = getattr(worker, "_dj_queue_supervisor", None)
  stop_event = getattr(worker, "_dj_queue_supervisor_poll_stop", None)
  poll_thread = getattr(worker, "_dj_queue_supervisor_poll_thread", None)
  if stop_event is not None:
    stop_event.set()
  if poll_thread is not None:
    poll_thread.join(timeout=1)
    worker._dj_queue_supervisor_poll_thread = None
  worker._dj_queue_supervisor_poll_stop = None
  if supervisor is None:
    return None

  supervisor.stop()
  worker._dj_queue_supervisor = None
  return None
