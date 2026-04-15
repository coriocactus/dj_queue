from contextlib import contextmanager, nullcontext
import threading
import time

from django.db import close_old_connections, connections
from django.db.utils import OperationalError
from django.utils import timezone

from dj_queue.config import load_backend_config
from dj_queue.db import get_database_alias
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
    connections.close_all()


_sqlite_process_write_lock = threading.Lock()


def _process_write_context(alias):
  if connections[alias].vendor == "sqlite":
    return _sqlite_process_write_lock
  return nullcontext()


def sqlite_retry(operation, *, alias, timeout=1):
  if connections[alias].vendor != "sqlite":
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
    supervisor=None,
  ):
    self.config = config
    self.backend_alias = backend_alias
    self.name = name
    self.pid = pid
    self.hostname = hostname
    self.sleeper = sleeper or InterruptibleSleeper()
    self.supervisor = supervisor
    self.process = None
    self._stop_event = threading.Event()
    self._heartbeat_thread = None
    if heartbeat_interval is None:
      heartbeat_interval = load_backend_config(backend_alias).process_heartbeat_interval
    self._heartbeat_interval = heartbeat_interval
    self._started = False
    self._stopped = False

  @property
  def polling_interval(self):
    return getattr(self.config, "polling_interval", 0)

  def start(self):
    if self.process is None:
      self.process = self._register_process()
      self._start_heartbeat_thread()
      fire_hooks(f"{self.hook_prefix}.start", self.process, backend_alias=self.backend_alias)
      self._started = True
    return self.process

  def run(self):
    self.start()
    try:
      while self.should_continue():
        try:
          self.poll_once()
        except Exception as error:
          handle_thread_error(
            error,
            context=f"{self.hook_prefix}.run",
            backend_alias=self.backend_alias,
          )
          break

        if not self.should_continue():
          break
        self.sleeper.sleep(self.polling_interval)
    finally:
      self.stop()
      close_old_connections()

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

  def should_continue(self):
    if self._stop_event.is_set():
      return False
    if self.process is None:
      return False

    alias = get_database_alias(self.backend_alias)
    if Process.objects.using(alias).filter(pk=self.process.pk).exists():
      return True

    self.request_stop()
    return False

  def poll_once(self):
    raise NotImplementedError

  def process_metadata(self):
    return {}

  def runtime_metadata(self):
    return self.process_metadata()

  def _begin_stop(self):
    if self._stopped:
      return None

    self._stopped = True
    self.request_stop()
    process = self.process
    if process is not None and self._started:
      fire_hooks(f"{self.hook_prefix}.stop", process, backend_alias=self.backend_alias)
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
          supervisor=self.supervisor,
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
    if self._heartbeat_interval <= 0:
      return
    self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
    self._heartbeat_thread.start()

  def _stop_heartbeat_thread(self):
    thread = self._heartbeat_thread
    if thread is None:
      return
    thread.join(timeout=max(self._heartbeat_interval, 0.1) + 0.1)
    self._heartbeat_thread = None

  def _heartbeat_loop(self):
    alias = get_database_alias(self.backend_alias)
    while not self._stop_event.wait(self._heartbeat_interval):
      if self.process is None:
        return
      try:
        with app_executor():
          with _process_write_context(alias):
            updated = sqlite_retry(
              lambda: (
                Process.objects.using(alias)
                .filter(pk=self.process.pk)
                .update(last_heartbeat_at=timezone.now())
              ),
              alias=alias,
            )
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
