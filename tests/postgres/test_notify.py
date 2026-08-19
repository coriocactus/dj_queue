import json
import threading
import time

import pytest
from django.db import connections

from dj_queue.config import WorkerConfig
from dj_queue.models import Job
from dj_queue.runtime.notify import (
  NoopWakeupBackend,
  NotifyWakeupBackend,
  build_wakeup_backend,
  notify_ready_queues,
)
from dj_queue.runtime.worker import Worker
from tests.tasks import echo

pytestmark = pytest.mark.postgres


def wait_until(predicate, timeout=2):
  deadline = time.monotonic() + timeout
  while time.monotonic() < deadline:
    if predicate():
      return
    time.sleep(0.01)
  assert predicate()


class InlinePool:
  def __init__(self, max_workers):
    self.idle_capacity = max_workers

  def submit(self, fn, *args, **kwargs):
    from concurrent.futures import Future

    future = Future()
    try:
      future.set_result(fn(*args, **kwargs))
    except Exception as exc:
      future.set_exception(exc)
    return future

  def shutdown(self, timeout, *, on_drained=None):
    if on_drained is not None:
      on_drained()
    return True


def make_worker(**overrides):
  return Worker(
    WorkerConfig(queues=("*",), threads=1, processes=1, polling_interval=0.5),
    backend_alias="default",
    name=overrides.pop("name", "notify-worker"),
    pid=overrides.pop("pid", 12345),
    hostname=overrides.pop("hostname", "localhost"),
    pool=overrides.pop("pool", InlinePool(1)),
    wakeup_backend=overrides.pop("wakeup_backend", None),
    sleeper=overrides.pop("sleeper", None),
  )


@pytest.mark.django_db(transaction=True)
def test_notify_watcher_starts_only_on_postgres():
  backend = build_wakeup_backend(backend_alias="default", queues=("*",), wake_up=lambda: None)

  assert isinstance(backend, NotifyWakeupBackend)


@pytest.mark.django_db(transaction=True)
def test_enqueue_ready_job_sends_ready_notify(monkeypatch):
  seen = []

  monkeypatch.setattr(
    "dj_queue.runtime.notify._notify",
    lambda channel, payload, *, backend_alias: seen.append((channel, payload, backend_alias)),
  )

  echo.enqueue("notify")

  assert len(seen) == 1
  channel, payload, backend_alias = seen[0]
  assert channel == "dj_queue_ready"
  assert json.loads(payload) == ["default"]
  assert backend_alias == "default"


@pytest.mark.django_db(transaction=True)
def test_dispatcher_ready_promotion_sends_ready_notify(monkeypatch):
  seen = []

  monkeypatch.setattr(
    "dj_queue.runtime.notify._notify",
    lambda channel, payload, *, backend_alias: seen.append((channel, payload, backend_alias)),
  )

  notify_ready_queues(("default",), backend_alias="default")

  assert len(seen) == 1
  channel, payload, backend_alias = seen[0]
  assert channel == "dj_queue_ready"
  assert json.loads(payload) == ["default"]
  assert backend_alias == "default"


@pytest.mark.django_db(transaction=True)
def test_worker_wakes_on_notify():
  worker = make_worker()
  worker.start()
  thread = threading.Thread(target=worker.run, daemon=True)
  thread.start()

  started_at = time.monotonic()
  result = echo.enqueue("wake")

  wait_until(lambda: Job.objects.get(pk=result.id).finished_at is not None, timeout=1)

  elapsed = time.monotonic() - started_at
  worker.request_stop()
  thread.join(timeout=1)
  worker.stop()

  assert thread.is_alive() is False
  assert elapsed < 0.3


@pytest.mark.django_db(transaction=True)
def test_notify_connection_failure_falls_back_to_polling(monkeypatch):
  monkeypatch.setattr(
    "dj_queue.runtime.notify.NotifyWakeupBackend._start_watcher",
    lambda self: (_ for _ in ()).throw(RuntimeError("boom")),
  )

  backend = build_wakeup_backend(backend_alias="default", queues=("*",), wake_up=lambda: None)
  backend.start()

  assert isinstance(backend, NotifyWakeupBackend)
  assert backend.failed is True


@pytest.mark.django_db(transaction=True)
def test_notify_raw_connection_does_not_open_django_wrapper(monkeypatch):
  wrapper = connections["default"]
  wrapper.close()
  monkeypatch.setattr(
    wrapper,
    "ensure_connection",
    lambda: (_ for _ in ()).throw(AssertionError("wrapper connection opened")),
  )
  backend = NotifyWakeupBackend(backend_alias="default", queues=("*",), wake_up=lambda: None)

  connection = backend._open_connection()

  try:
    assert connection.closed is False
  finally:
    connection.close()


@pytest.mark.django_db(transaction=True)
def test_listen_notify_false_disables_watcher(settings):
  settings.TASKS = {
    **settings.TASKS,
    "default": {
      **settings.TASKS["default"],
      "OPTIONS": {
        **settings.TASKS["default"]["OPTIONS"],
        "listen_notify": False,
      },
    },
  }

  backend = build_wakeup_backend(backend_alias="default", queues=("*",), wake_up=lambda: None)

  assert isinstance(backend, NoopWakeupBackend)
