import json
import threading

from django.db import connections

from dj_queue.config import load_backend_config
from dj_queue.db import get_database_alias, supports_listen_notify
from dj_queue.log import log_event
from dj_queue.queue_selectors import any_queue_matches_selectors
from dj_queue.runtime.errors import handle_thread_error

READY_CHANNEL = "dj_queue_ready"
READY_PAYLOAD = "ready"
NOTIFY_RECONNECT_BASE_DELAY = 0.5
NOTIFY_RECONNECT_MAX_DELAY = 5.0


class NoopWakeupBackend:
  def start(self):
    return None

  def stop(self, *, timeout=None):
    return None


class NotifyWakeupBackend:
  def __init__(
    self,
    *,
    backend_alias,
    wake_up,
    queues=("*",),
    reconnect_base_delay=NOTIFY_RECONNECT_BASE_DELAY,
    reconnect_max_delay=NOTIFY_RECONNECT_MAX_DELAY,
  ):
    self.backend_alias = backend_alias
    self.wake_up = wake_up
    self.queues = tuple(queues or ("*",))
    self.reconnect_base_delay = reconnect_base_delay
    self.reconnect_max_delay = reconnect_max_delay
    self.failed = False
    self._connection = None
    self._watcher = None
    self._stop_event = threading.Event()

  def start(self):
    if self._watcher is not None:
      return None

    self._stop_event.clear()
    try:
      self._connection = self._open_connection()
    except Exception as error:
      self.failed = True
      self._close_connection()
      handle_thread_error(error, context="worker.notify", backend_alias=self.backend_alias)
    try:
      self._start_watcher()
    except Exception as error:
      self.failed = True
      self._watcher = None
      self._close_connection()
      handle_thread_error(error, context="worker.notify", backend_alias=self.backend_alias)
    return None

  def stop(self, *, timeout=1):
    self._stop_event.set()
    self._close_connection()
    if self._watcher is not None:
      self._watcher.join(timeout=timeout)
      if not self._watcher.is_alive():
        self._watcher = None
    return None

  def _start_watcher(self):
    self._watcher = threading.Thread(target=self._watch, daemon=True, name="dj-queue-notify")
    self._watcher.start()

  def _watch(self):
    failures = 0
    while not self._stop_event.is_set():
      connection = self._connection
      if connection is None:
        if self._stop_event.wait(self._reconnect_delay(failures)):
          return
        if self._reconnect():
          failures = 0
        else:
          failures += 1
        continue

      try:
        notifications = connection.notifies(timeout=0.5, stop_after=1)
        for notification in notifications:
          if any_queue_matches_selectors(
            _queue_names_from_payload(notification.payload), self.queues
          ):
            self.wake_up()
      except Exception as error:
        if self._stop_event.is_set():
          return
        failures += 1
        self.failed = True
        self._close_connection()
        handle_thread_error(error, context="worker.notify", backend_alias=self.backend_alias)

  def _reconnect(self):
    try:
      self._connection = self._open_connection()
    except Exception as error:
      self.failed = True
      self._close_connection()
      handle_thread_error(error, context="worker.notify", backend_alias=self.backend_alias)
      return False

    self.failed = False
    log_event("notify.restored", backend_alias=self.backend_alias)
    return True

  def _reconnect_delay(self, failures):
    delay = self.reconnect_base_delay * (2 ** max(failures - 1, 0))
    return min(delay, self.reconnect_max_delay)

  def _open_connection(self):
    alias = get_database_alias(self.backend_alias)
    wrapper = connections[alias]
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

  payload = json.dumps(list(dict.fromkeys(queue_names)), separators=(",", ":"))
  _notify(READY_CHANNEL, payload, backend_alias=backend_alias)
  return None


def build_wakeup_backend(*, backend_alias="default", queues=(), wake_up=None):
  config = load_backend_config(backend_alias)
  alias = get_database_alias(backend_alias)
  if wake_up is None or not config.listen_notify or not supports_listen_notify(alias):
    return NoopWakeupBackend()
  return NotifyWakeupBackend(backend_alias=backend_alias, queues=queues, wake_up=wake_up)


def _notify(channel, payload, *, backend_alias):
  try:
    with connections[get_database_alias(backend_alias)].cursor() as cursor:
      cursor.execute("SELECT pg_notify(%s, %s)", [channel, payload])
  except Exception as error:
    handle_thread_error(error, context="producer.notify", backend_alias=backend_alias)
    return None
  return None


def _queue_names_from_payload(payload):
  if payload == READY_PAYLOAD:
    return None
  try:
    queue_names = json.loads(payload)
  except (TypeError, ValueError):
    return None
  if not isinstance(queue_names, list):
    return None
  return tuple(str(queue_name) for queue_name in queue_names)
