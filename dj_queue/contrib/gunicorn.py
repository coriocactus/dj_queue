import fcntl
import tempfile
import threading
from pathlib import Path

from dj_queue.runtime.errors import handle_thread_error
from dj_queue.runtime.supervisor import AsyncSupervisor

LOCK_PATH = Path(tempfile.gettempdir()) / "dj_queue_gunicorn_supervisor.lock"


def build_supervisor(backend_alias="default"):
  return AsyncSupervisor.from_backend_config(backend_alias=backend_alias, standalone=False)


def post_fork(_server, worker):
  lock_file = _try_acquire_supervisor_lock()
  if lock_file is None:
    return None

  try:
    supervisor = build_supervisor()
    worker._dj_queue_supervisor_lock = lock_file
    worker._dj_queue_supervisor = supervisor
    worker._dj_queue_supervisor_poll_stop = threading.Event()
    supervisor.start()
  except Exception:
    _release_supervisor_lock(lock_file)
    worker._dj_queue_supervisor_lock = None
    raise

  def poll_supervisor():
    stop_event = worker._dj_queue_supervisor_poll_stop
    while stop_event.wait(supervisor.polling_interval) is False:
      try:
        supervisor.poll_once()
      except Exception as error:
        handle_thread_error(
          error,
          context="supervisor.run",
          backend_alias=supervisor.backend_alias,
        )

  poll_thread = threading.Thread(target=poll_supervisor, daemon=True)
  worker._dj_queue_supervisor_poll_thread = poll_thread
  poll_thread.start()
  return supervisor


def worker_exit(_server, worker):
  supervisor = getattr(worker, "_dj_queue_supervisor", None)
  lock_file = getattr(worker, "_dj_queue_supervisor_lock", None)
  stop_event = getattr(worker, "_dj_queue_supervisor_poll_stop", None)
  poll_thread = getattr(worker, "_dj_queue_supervisor_poll_thread", None)
  if stop_event is not None:
    stop_event.set()
  if poll_thread is not None:
    poll_thread.join(timeout=1)
    worker._dj_queue_supervisor_poll_thread = None
  worker._dj_queue_supervisor_poll_stop = None
  if supervisor is None:
    if lock_file is not None:
      _release_supervisor_lock(lock_file)
      worker._dj_queue_supervisor_lock = None
    return None

  supervisor.stop()
  worker._dj_queue_supervisor = None
  _release_supervisor_lock(lock_file)
  worker._dj_queue_supervisor_lock = None
  return None


def _try_acquire_supervisor_lock():
  LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
  lock_file = LOCK_PATH.open("a+")
  try:
    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
  except BlockingIOError:
    lock_file.close()
    return None
  return lock_file


def _release_supervisor_lock(lock_file):
  if lock_file is None:
    return None
  try:
    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
  finally:
    lock_file.close()
  return None
