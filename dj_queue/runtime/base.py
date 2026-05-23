from contextlib import contextmanager, nullcontext
import math
import threading
import time

from django.db import close_old_connections
from django.db.utils import OperationalError
from django.utils import timezone

from dj_queue.config import load_backend_config
from dj_queue.db import database_capabilities, get_database_alias
from dj_queue.hooks import fire_hooks
from dj_queue.models import Process
from dj_queue.runtime.errors import handle_thread_error
from dj_queue.runtime.interruptible import InterruptibleSleeper


@contextmanager
def app_executor():
  close_old_connections()
  try:
    yield
  finally:
    close_old_connections()


_sqlite_process_write_lock = threading.Lock()
_safe_polling_interval = 1.0


def _process_write_context(alias):
  if database_capabilities(alias).uses_serialized_writes:
    return _sqlite_process_write_lock
  return nullcontext()


def sqlite_retry(operation, *, alias, timeout=1):
  if not database_capabilities(alias).uses_serialized_writes:
    return operation()

  deadline = time.monotonic() + timeout
  while True:
    try:
      return operation()
    except OperationalError as error:
      if "locked" not in str(error).lower():
        raise
      if time.monotonic() >= deadline:
        raise
      time.sleep(0.01)


class BaseRunner:
  process_kind = "Runner"
  hook_prefix = "runner"

  def __init__(
    self,
    config,
    *,
    backend_alias="default",
    name,
    pid,
    hostname,
    sleeper=None,
    heartbeat_interval=None,
    process_alive_threshold=None,
    supervisor=None,
  ):
    self.config = config
    self.backend_alias = backend_alias
    self.name = name
    self.pid = pid
    self.hostname = hostname
    self.sleeper = sleeper or InterruptibleSleeper()
    self.supervisor = supervisor
    self.supervisor_id = getattr(supervisor, "pk", supervisor)
    self.process = None
    self._stop_event = threading.Event()
    self._heartbeat_stop_event = threading.Event()
    self._heartbeat_thread = None
    if heartbeat_interval is None:
      heartbeat_interval = load_backend_config(backend_alias).process_heartbeat_interval
    self._heartbeat_interval = heartbeat_interval
    if process_alive_threshold is None:
      process_alive_threshold = getattr(config, "process_alive_threshold", None)
    self._process_alive_threshold = process_alive_threshold
    self._started = False
    self._stopped = False

  @property
  def polling_interval(self):
    return self._normalized_polling_interval(getattr(self.config, "polling_interval", None))

  @staticmethod
  def _normalized_polling_interval(value):
    try:
      polling_interval = float(value)
    except (TypeError, ValueError):
      return _safe_polling_interval

    if not math.isfinite(polling_interval) or polling_interval <= 0:
      return _safe_polling_interval
    return polling_interval

  def start(self):
    if self.process is None:
      self.process = self._register_process()
      self._start_heartbeat_thread()
      fire_hooks(f"{self.hook_prefix}.start", self.process, backend_alias=self.backend_alias)
      self._started = True
    return self.process

  def run(self):
    try:
      self.start()
      self.run_poll_loop()
    finally:
      self.stop()
      close_old_connections()

  def run_poll_loop(self):
    while self.should_continue():
      if not self._poll_once_handled():
        return False
      if not self.should_continue():
        break
      self.sleeper.sleep(self.polling_interval)
    return True

  def run_managed_poll_loop(self, *, host_stop_requested):
    while not self.stop_requested() and not host_stop_requested():
      if not self._poll_once_handled():
        return False
      if self.stop_requested() or host_stop_requested():
        break
      self.sleeper.sleep(self.polling_interval)
    return host_stop_requested()

  def stop(self):
    process = self._begin_stop()
    if process is None:
      return None
    self._finish_stop(process)
    return None

  def request_stop(self):
    self._stop_event.set()
    wake_up = getattr(self.sleeper, "wake_up", None)
    if callable(wake_up):
      wake_up()

  def stop_requested(self):
    return self._stop_event.is_set()

  def should_continue(self):
    if self._stop_event.is_set():
      return False
    if self.process is None:
      return False

    alias = get_database_alias(self.backend_alias)
    with app_executor():
      exists = Process.objects.using(alias).filter(pk=self.process.pk).exists()
    if exists:
      return True

    self.request_stop()
    return False

  def poll_once(self):
    raise NotImplementedError

  def _poll_once_handled(self):
    try:
      self.poll_once()
    except Exception as error:
      handle_thread_error(
        error,
        context=f"{self.hook_prefix}.run",
        backend_alias=self.backend_alias,
      )
      return False
    return True

  def process_metadata(self):
    return {}

  def runtime_metadata(self):
    return self.process_metadata()

  def _begin_stop(self, *, stop_heartbeat=True):
    if self._stopped:
      return None

    self._stopped = True
    self.request_stop()
    process = self.process
    if process is not None and self._started:
      fire_hooks(f"{self.hook_prefix}.stop", process, backend_alias=self.backend_alias)
    if stop_heartbeat:
      self._stop_heartbeat_thread()
    return process

  def _finish_stop(self, process):
    self._deregister_process()
    if process is not None and self._started:
      fire_hooks(f"{self.hook_prefix}.exit", process, backend_alias=self.backend_alias)

    close = getattr(self.sleeper, "close", None)
    if callable(close):
      close()

  def _register_process(self):
    alias = get_database_alias(self.backend_alias)
    with _process_write_context(alias):
      return sqlite_retry(
        lambda: Process.objects.using(alias).create(
          backend_alias=self.backend_alias,
          kind=self.process_kind,
          pid=self.pid,
          hostname=self.hostname,
          name=self.name,
          metadata=self.runtime_metadata(),
          supervisor_id=self.supervisor_id,
          last_heartbeat_at=timezone.now(),
        ),
        alias=alias,
      )

  def _deregister_process(self):
    if self.process is None:
      return

    alias = get_database_alias(self.backend_alias)
    with _process_write_context(alias):
      sqlite_retry(
        lambda: Process.objects.using(alias).filter(pk=self.process.pk).delete(), alias=alias
      )
    self.process = None

  def _start_heartbeat_thread(self):
    interval = self._effective_heartbeat_interval()
    if interval <= 0:
      return
    self._heartbeat_stop_event.clear()
    self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
    self._heartbeat_thread.start()

  def _stop_heartbeat_thread(self):
    thread = self._heartbeat_thread
    if thread is None:
      return
    self._heartbeat_stop_event.set()
    thread.join(timeout=max(self._effective_heartbeat_interval(), 0.1) + 0.1)
    self._heartbeat_thread = None

  def _heartbeat_loop(self):
    interval = self._effective_heartbeat_interval()
    while not self._heartbeat_stop_event.wait(interval):
      if self.process is None:
        return
      try:
        updated = self._touch_process_row()
      except Exception as error:
        handle_thread_error(
          error,
          context=f"{self.hook_prefix}.heartbeat",
          backend_alias=self.backend_alias,
        )
        self.request_stop()
        return

      if updated == 0:
        self.request_stop()
        return

  def _touch_process_row(self):
    alias = get_database_alias(self.backend_alias)
    with app_executor():
      with _process_write_context(alias):
        return sqlite_retry(
          lambda: (
            Process.objects.using(alias)
            .filter(pk=self.process.pk)
            .update(last_heartbeat_at=timezone.now())
          ),
          alias=alias,
        )

  def _effective_heartbeat_interval(self):
    try:
      heartbeat_interval = float(self._heartbeat_interval)
    except (TypeError, ValueError):
      heartbeat_interval = 0

    if math.isfinite(heartbeat_interval) and heartbeat_interval > 0:
      return heartbeat_interval

    threshold = self._process_alive_threshold
    if threshold is None:
      threshold = load_backend_config(self.backend_alias).process_alive_threshold
    try:
      threshold = float(threshold)
    except (TypeError, ValueError):
      threshold = 0

    if not math.isfinite(threshold) or threshold <= 0:
      return 1.0
    return max(min(threshold / 2, 60.0), 0.01)
