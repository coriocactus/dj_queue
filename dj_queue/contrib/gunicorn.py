import fcntl
import hashlib
import tempfile
import threading
from pathlib import Path

from dj_queue.config import load_backend_config
from dj_queue.runtime.errors import handle_thread_error

LOCK_PATH_PREFIX = "dj_queue_gunicorn_supervisor"
LOCK_RETRY_INTERVAL = 1.0


def _set_supervisor_state(worker, **state):
  for name, value in state.items():
    setattr(worker, f"_dj_queue_{name}", value)


def _start_embedded_supervisor(worker, *, backend_alias="default"):
  from dj_queue.runtime.supervisor import AsyncSupervisor

  if getattr(worker, "_dj_queue_supervisor_exiting", False):
    return None

  lock_file = _try_acquire_supervisor_lock(backend_alias=backend_alias)
  if lock_file is None:
    return None

  try:
    if getattr(worker, "_dj_queue_supervisor_exiting", False):
      _release_supervisor_lock(lock_file)
      return None
    supervisor = AsyncSupervisor.from_backend_config(backend_alias=backend_alias, standalone=False)
    _set_supervisor_state(worker, supervisor_lock=lock_file, supervisor=supervisor)
    supervisor.start()
    if getattr(worker, "_dj_queue_supervisor_exiting", False):
      supervisor.stop()
      _release_supervisor_lock(lock_file)
      _set_supervisor_state(worker, supervisor_lock=None, supervisor=None)
      return None
  except Exception:
    _release_supervisor_lock(lock_file)
    _set_supervisor_state(worker, supervisor_lock=None, supervisor=None)
    raise

  def run_supervisor():
    try:
      supervisor.run()
    finally:
      _release_supervisor_lock(lock_file)

  thread = threading.Thread(target=run_supervisor, daemon=True)
  worker._dj_queue_supervisor_thread = thread
  thread.start()
  return supervisor


def _start_lock_retry_loop(worker, *, backend_alias="default"):
  retry_stop = threading.Event()

  def retry():
    while retry_stop.wait(LOCK_RETRY_INTERVAL) is False:
      if getattr(worker, "_dj_queue_supervisor", None) is not None:
        return
      try:
        supervisor = _start_embedded_supervisor(worker, backend_alias=backend_alias)
      except Exception as error:
        handle_thread_error(
          error, context="gunicorn.supervisor", config=load_backend_config(backend_alias)
        )
        continue
      if supervisor is not None:
        retry_stop.set()
        return

  retry_thread = threading.Thread(target=retry, daemon=True)
  _set_supervisor_state(
    worker, supervisor_retry_stop=retry_stop, supervisor_retry_thread=retry_thread
  )
  retry_thread.start()


def post_fork(server, worker):
  backend_alias = _backend_alias(server, worker)
  _set_supervisor_state(
    worker,
    supervisor=None,
    supervisor_lock=None,
    supervisor_thread=None,
    supervisor_retry_stop=None,
    supervisor_retry_thread=None,
    supervisor_exiting=False,
  )
  supervisor = _start_embedded_supervisor(worker, backend_alias=backend_alias)
  if supervisor is not None:
    return supervisor

  _start_lock_retry_loop(worker, backend_alias=backend_alias)
  return None


def worker_exit(_server, worker):
  worker._dj_queue_supervisor_exiting = True
  retry_stop = getattr(worker, "_dj_queue_supervisor_retry_stop", None)
  retry_thread = getattr(worker, "_dj_queue_supervisor_retry_thread", None)
  supervisor = getattr(worker, "_dj_queue_supervisor", None)

  if retry_stop is not None:
    retry_stop.set()
  if retry_thread is not None:
    retry_thread.join(timeout=_supervisor_shutdown_timeout(supervisor))
    if not retry_thread.is_alive():
      worker._dj_queue_supervisor_retry_thread = None
  worker._dj_queue_supervisor_retry_stop = None

  # the supervisor thread stops the supervisor and releases the lock on its way out
  thread = getattr(worker, "_dj_queue_supervisor_thread", None)
  if thread is not None:
    worker._dj_queue_supervisor.request_stop()
    thread.join()
  _set_supervisor_state(worker, supervisor=None, supervisor_lock=None, supervisor_thread=None)


def _supervisor_shutdown_timeout(supervisor, default=1.0):
  config = getattr(supervisor, "config", None)
  return getattr(config, "shutdown_timeout", default)


def _backend_alias(server, worker):
  return getattr(worker, "dj_queue_backend_alias", None) or getattr(
    server, "dj_queue_backend_alias", "default"
  )


def _supervisor_lock_path(*, backend_alias):
  lock_scope = f"{Path.cwd()}:{backend_alias}"
  digest = hashlib.sha256(lock_scope.encode()).hexdigest()[:12]
  return Path(tempfile.gettempdir()) / f"{LOCK_PATH_PREFIX}_{backend_alias}_{digest}.lock"


def _try_acquire_supervisor_lock(*, backend_alias="default"):
  lock_path = _supervisor_lock_path(backend_alias=backend_alias)
  lock_path.parent.mkdir(parents=True, exist_ok=True)
  lock_file = lock_path.open("a+")
  try:
    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
  except BlockingIOError:
    lock_file.close()
    return None
  return lock_file


def _release_supervisor_lock(lock_file):
  if lock_file is None:
    return
  try:
    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
  finally:
    lock_file.close()
  return
