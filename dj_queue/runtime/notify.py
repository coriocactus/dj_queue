import threading

from django.db import connections

from dj_queue.config import load_backend_config
from dj_queue.db import get_database_alias, supports_listen_notify
from dj_queue.runtime.errors import handle_thread_error

READY_CHANNEL = "dj_queue_ready"


class NoopWakeupBackend:
  def start(self):
    return None

  def stop(self):
    return None


class NotifyWakeupBackend:
  def __init__(self, *, backend_alias, wake_up):
    self.backend_alias = backend_alias
    self.wake_up = wake_up
    self.failed = False
    self._connection = None
    self._watcher = None
    self._stop_event = threading.Event()

  def start(self):
    if self._watcher is not None:
      return None

    try:
      self._connection = self._open_connection()
      self._start_watcher()
    except Exception as error:
      self.failed = True
      self._close_connection()
      handle_thread_error(error, context="worker.notify", backend_alias=self.backend_alias)
    return None

  def stop(self):
    self._stop_event.set()
    if self._watcher is not None:
      self._watcher.join(timeout=1)
      self._watcher = None
    self._close_connection()
    return None

  def _start_watcher(self):
    self._watcher = threading.Thread(target=self._watch, daemon=True, name="dj-queue-notify")
    self._watcher.start()

  def _watch(self):
    connection = self._connection
    if connection is None:
      return

    while not self._stop_event.is_set():
      try:
        notifications = connection.notifies(timeout=0.5, stop_after=1)
      except Exception as error:
        self.failed = True
        handle_thread_error(error, context="worker.notify", backend_alias=self.backend_alias)
        return

      for _notify in notifications:
        self.wake_up()

  def _open_connection(self):
    alias = get_database_alias(self.backend_alias)
    wrapper = connections[alias]
    wrapper.ensure_connection()
    connection = wrapper.Database.connect(**wrapper.get_connection_params())
    connection.autocommit = True
    with connection.cursor() as cursor:
      cursor.execute(f"LISTEN {READY_CHANNEL}")
    return connection

  def _close_connection(self):
    connection = self._connection
    if connection is None:
      return
    self._connection = None
    try:
      connection.close()
    except Exception:
      return


def notify_ready_queues(queue_names, *, backend_alias="default"):
  config = load_backend_config(backend_alias)
  if not queue_names or not config.listen_notify:
    return None

  alias = get_database_alias(backend_alias)
  if not supports_listen_notify(alias):
    return None

  for queue_name in queue_names:
    _notify(READY_CHANNEL, queue_name, backend_alias=backend_alias)
  return None


def build_wakeup_backend(*, backend_alias="default", queues=(), wake_up=None):
  config = load_backend_config(backend_alias)
  alias = get_database_alias(backend_alias)
  if wake_up is None or not config.listen_notify or not supports_listen_notify(alias):
    return NoopWakeupBackend()
  return NotifyWakeupBackend(backend_alias=backend_alias, wake_up=wake_up)


def _notify(channel, payload, *, backend_alias):
  try:
    with connections[get_database_alias(backend_alias)].cursor() as cursor:
      cursor.execute("SELECT pg_notify(%s, %s)", [channel, payload])
  except Exception:
    return None
  return None
