import fcntl
import tempfile
import threading
from pathlib import Path

from dj_queue.runtime.errors import handle_thread_error
from dj_queue.runtime.supervisor import AsyncSupervisor

LOCK_PATH = Path(tempfile.gettempdir()) / "dj_queue_gunicorn_supervisor.lock"
LOCK_RETRY_INTERVAL = 1.0


def build_supervisor(backend_alias="default"):
  return AsyncSupervisor.from_backend_config(backend_alias=backend_alias, standalone=False)


def _set_supervisor_state(worker, **state):
  for name, value in state.items():
    setattr(worker, f"_dj_queue_{name}", value)


def _start_embedded_supervisor(worker, *, backend_alias="default"):
  lock_file = _try_acquire_supervisor_lock()
  if lock_file is None:
    return None

  try:
    supervisor = build_supervisor(backend_alias=backend_alias)
    poll_stop = threading.Event()
    _set_supervisor_state(
      worker,
      supervisor_lock=lock_file,
      supervisor=supervisor,
      supervisor_poll_stop=poll_stop,
    )
    supervisor.start()
  except Exception:
    _release_supervisor_lock(lock_file)
    _set_supervisor_state(
      worker,
      supervisor_lock=None,
      supervisor=None,
      supervisor_poll_stop=None,
      supervisor_poll_thread=None,
    )
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


def _start_lock_retry_loop(worker, *, backend_alias="default"):
  retry_stop = threading.Event()

  def retry():
    while retry_stop.wait(LOCK_RETRY_INTERVAL) is False:
      if getattr(worker, "_dj_queue_supervisor", None) is not None:
        return
      if _start_embedded_supervisor(worker, backend_alias=backend_alias) is not None:
        retry_stop.set()
        return

  retry_thread = threading.Thread(target=retry, daemon=True)
  _set_supervisor_state(
    worker, supervisor_retry_stop=retry_stop, supervisor_retry_thread=retry_thread
  )
  retry_thread.start()


def post_fork(_server, worker):
  _set_supervisor_state(
    worker,
    supervisor=None,
    supervisor_lock=None,
    supervisor_poll_stop=None,
    supervisor_poll_thread=None,
    supervisor_retry_stop=None,
    supervisor_retry_thread=None,
  )
  supervisor = _start_embedded_supervisor(worker)
  if supervisor is not None:
    return supervisor

  _start_lock_retry_loop(worker)
  return None


def worker_exit(_server, worker):
  supervisor = getattr(worker, "_dj_queue_supervisor", None)
  lock_file = getattr(worker, "_dj_queue_supervisor_lock", None)
  stop_event = getattr(worker, "_dj_queue_supervisor_poll_stop", None)
  poll_thread = getattr(worker, "_dj_queue_supervisor_poll_thread", None)
  retry_stop = getattr(worker, "_dj_queue_supervisor_retry_stop", None)
  retry_thread = getattr(worker, "_dj_queue_supervisor_retry_thread", None)

  if retry_stop is not None:
    retry_stop.set()
  if retry_thread is not None:
    retry_thread.join(timeout=1)
    worker._dj_queue_supervisor_retry_thread = None
  worker._dj_queue_supervisor_retry_stop = None

  if stop_event is not None:
    stop_event.set()
  if poll_thread is not None:
    poll_thread.join()
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
