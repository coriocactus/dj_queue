from contextlib import contextmanager
import logging
import threading

from django.db import close_old_connections
from django.utils import timezone
from django.utils.module_loading import import_string

from dj_queue.config import load_backend_config
from dj_queue.db import get_database_alias
from dj_queue.models import Process
from dj_queue.runtime.interruptible import InterruptibleSleeper

logger = logging.getLogger("dj_queue")


@contextmanager
def app_executor():
  close_old_connections()
  try:
    yield
  finally:
    close_old_connections()


def handle_thread_error(error, *, context="", backend_alias="default"):
  callback_path = load_backend_config(backend_alias).on_thread_error
  if callback_path:
    try:
      callback = import_string(callback_path)
      callback(error)
      return
    except Exception:
      logger.exception(
        "on_thread_error callback raised",
        extra={
          "event": "dj_queue.thread_error_callback_failed",
          "backend_alias": backend_alias,
          "thread_error_context": context,
          "on_thread_error": callback_path,
          "thread_error_type": error.__class__.__name__,
        },
      )
      return

  logger.error(
    "dj_queue infrastructure error",
    exc_info=(error.__class__, error, error.__traceback__),
    extra={
      "event": "dj_queue.thread_error",
      "backend_alias": backend_alias,
      "thread_error_context": context,
      "thread_error_type": error.__class__.__name__,
    },
  )


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
      from dj_queue.hooks import fire_hooks

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

  def _begin_stop(self):
    if self._stopped:
      return None

    self._stopped = True
    self.request_stop()
    process = self.process
    if process is not None and self._started:
      from dj_queue.hooks import fire_hooks

      fire_hooks(f"{self.hook_prefix}.stop", process, backend_alias=self.backend_alias)
    self._stop_heartbeat_thread()
    return process

  def _finish_stop(self, process):
    self._deregister_process()
    if process is not None and self._started:
      from dj_queue.hooks import fire_hooks

      fire_hooks(f"{self.hook_prefix}.exit", process, backend_alias=self.backend_alias)

    close = getattr(self.sleeper, "close", None)
    if callable(close):
      close()

  def _register_process(self):
    alias = get_database_alias(self.backend_alias)
    return Process.objects.using(alias).create(
      kind=self.process_kind,
      pid=self.pid,
      hostname=self.hostname,
      name=self.name,
      metadata=self.process_metadata(),
      supervisor=self.supervisor,
      last_heartbeat_at=timezone.now(),
    )

  def _deregister_process(self):
    if self.process is None:
      return

    alias = get_database_alias(self.backend_alias)
    Process.objects.using(alias).filter(pk=self.process.pk).delete()
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
          updated = (
            Process.objects.using(alias)
            .filter(pk=self.process.pk)
            .update(last_heartbeat_at=timezone.now())
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
