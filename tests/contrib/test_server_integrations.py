import asyncio
import os
import subprocess
import sys
import threading

import pytest
from django.core.management import call_command
from django.db import connections
from django.tasks import TaskResultStatus

from dj_queue.models import Job, Process
from dj_queue.runtime.supervisor import AsyncSupervisor
from tests.factories import wait_until
from tests.tasks import echo

pytestmark = [
  pytest.mark.django_db(transaction=True),
  pytest.mark.filterwarnings(
    r"ignore:Overriding setting DATABASES can lead to unexpected behavior\.:UserWarning"
  ),
]


def _queue_tasks(database_alias="default"):
  return {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {
        "mode": "async",
        "database_alias": database_alias,
        "workers": [{"queues": "*", "threads": 1, "processes": 1, "polling_interval": 0.01}],
        "dispatchers": [],
        "scheduler": {"dynamic_tasks_enabled": False, "polling_interval": 5},
        "recurring": {},
        "process_heartbeat_interval": 1,
        "process_alive_threshold": 5,
        "preserve_finished_jobs": True,
        "clear_finished_jobs_after": None,
      },
    }
  }


def _sqlite_databases(tmp_path):
  return {
    "default": {
      "ENGINE": "django.db.backends.sqlite3",
      "NAME": str(tmp_path / "default.sqlite3"),
    },
    "queue": {
      "ENGINE": "django.db.backends.sqlite3",
      "NAME": str(tmp_path / "queue.sqlite3"),
    },
  }


def test_gunicorn_hooks_import_before_django_setup():
  environment = {
    key: value for key, value in os.environ.items() if key != "DJANGO_SETTINGS_MODULE"
  }
  result = subprocess.run(
    [sys.executable, "-c", "from dj_queue.contrib.gunicorn import post_fork, worker_exit"],
    check=False,
    env=environment,
    capture_output=True,
    text=True,
  )

  assert result.returncode == 0, result.stderr


class StubSupervisor:
  def __init__(self, events):
    self.events = events
    self._stop_requested = threading.Event()

  def start(self):
    self.events.append("start")

  def run(self):
    self._stop_requested.wait()
    self.stop()

  def request_stop(self):
    self._stop_requested.set()

  def stop(self):
    self.events.append("stop")


def stub_supervisors(monkeypatch, events, supervisor_class=StubSupervisor):
  aliases = []

  def build(*, backend_alias, standalone):
    assert standalone is False
    aliases.append(backend_alias)
    return supervisor_class(events)

  monkeypatch.setattr(AsyncSupervisor, "from_backend_config", build)
  return aliases


def test_gunicorn_post_fork_starts_one_embedded_supervisor(monkeypatch):
  events = []
  released = []

  class Lock:
    pass

  locks = [Lock()]
  aliases = stub_supervisors(monkeypatch, events)

  from dj_queue.contrib import gunicorn

  monkeypatch.setattr(
    gunicorn,
    "_try_acquire_supervisor_lock",
    lambda **_kwargs: locks.pop(0) if locks else None,
  )
  monkeypatch.setattr(gunicorn, "_release_supervisor_lock", lambda lock: released.append(lock))

  worker_one = type("Worker", (), {"age": 2})()
  worker_two = type("Worker", (), {"age": 3})()

  try:
    gunicorn.post_fork(object(), worker_one)
    gunicorn.post_fork(object(), worker_two)

    assert isinstance(worker_one._dj_queue_supervisor, StubSupervisor)
    assert worker_two._dj_queue_supervisor is None
    acquired_lock = worker_one._dj_queue_supervisor_lock

    gunicorn.worker_exit(object(), worker_one)

    assert aliases == ["default"]
    assert events == ["start", "stop"]
    assert worker_one._dj_queue_supervisor is None
    assert released == [acquired_lock]
    assert worker_one._dj_queue_supervisor_lock is None
  finally:
    gunicorn.worker_exit(object(), worker_one)
    gunicorn.worker_exit(object(), worker_two)


def test_gunicorn_retries_lock_until_replacement_worker_can_start(monkeypatch):
  events = []
  released = []

  class Lock:
    pass

  locks = [Lock(), Lock()]
  available = {"open": False}
  stub_supervisors(monkeypatch, events)

  from dj_queue.contrib import gunicorn

  monkeypatch.setattr(gunicorn, "LOCK_RETRY_INTERVAL", 0.01)
  monkeypatch.setattr(
    gunicorn,
    "_try_acquire_supervisor_lock",
    lambda **_kwargs: locks.pop(0) if available["open"] and locks else None,
  )
  monkeypatch.setattr(gunicorn, "_release_supervisor_lock", lambda lock: released.append(lock))

  worker_one = type("Worker", (), {"age": 2})()
  worker_two = type("Worker", (), {"age": 3})()

  available["open"] = True
  gunicorn.post_fork(object(), worker_one)
  available["open"] = False
  first_lock = worker_one._dj_queue_supervisor_lock
  gunicorn.post_fork(object(), worker_two)
  assert worker_two._dj_queue_supervisor is None

  available["open"] = True
  gunicorn.worker_exit(object(), worker_one)

  wait_until(lambda: getattr(worker_two, "_dj_queue_supervisor", None) is not None)
  acquired_lock = worker_two._dj_queue_supervisor_lock

  gunicorn.worker_exit(object(), worker_two)

  assert events.count("start") == 2
  assert released == [first_lock, acquired_lock]


def test_gunicorn_worker_exit_releases_lock_only_after_supervisor_stops(monkeypatch):
  events = []
  released = []
  stopping = threading.Event()
  allow_stop = threading.Event()

  class SlowStopSupervisor(StubSupervisor):
    def stop(self):
      stopping.set()
      allow_stop.wait(timeout=1)
      super().stop()

  stub_supervisors(monkeypatch, events, SlowStopSupervisor)

  from dj_queue.contrib import gunicorn

  monkeypatch.setattr(gunicorn, "_try_acquire_supervisor_lock", lambda **_kwargs: object())
  monkeypatch.setattr(gunicorn, "_release_supervisor_lock", lambda lock: released.append(lock))
  worker = type("Worker", (), {"age": 1})()

  gunicorn.post_fork(object(), worker)
  acquired_lock = worker._dj_queue_supervisor_lock

  exit_thread = threading.Thread(target=lambda: gunicorn.worker_exit(object(), worker))
  exit_thread.start()

  assert stopping.wait(timeout=1) is True
  assert released == []
  assert exit_thread.is_alive() is True

  allow_stop.set()
  exit_thread.join(timeout=1)

  assert events == ["start", "stop"]
  assert released == [acquired_lock]


def test_gunicorn_start_aborts_when_worker_exits_during_supervisor_start(monkeypatch):
  events = []
  released = []

  from dj_queue.contrib import gunicorn

  worker = type("Worker", (), {"age": 1, "_dj_queue_supervisor_exiting": False})()

  class ExitDuringStartSupervisor(StubSupervisor):
    def start(self):
      super().start()
      worker._dj_queue_supervisor_exiting = True

  lock = object()
  stub_supervisors(monkeypatch, events, ExitDuringStartSupervisor)
  monkeypatch.setattr(gunicorn, "_try_acquire_supervisor_lock", lambda **_kwargs: lock)
  monkeypatch.setattr(gunicorn, "_release_supervisor_lock", lambda lock: released.append(lock))

  supervisor = gunicorn._start_embedded_supervisor(worker)

  assert supervisor is None
  assert events == ["start", "stop"]
  assert released == [lock]
  assert worker._dj_queue_supervisor is None
  assert worker._dj_queue_supervisor_lock is None


def test_asgi_lifespan_startup_and_shutdown_wrap_supervisor(monkeypatch):
  events = []
  stub_supervisors(monkeypatch, events)

  from dj_queue.contrib.asgi import DjQueueLifespan

  app = DjQueueLifespan(lambda scope, receive, send: None)
  sent_messages = []
  messages = iter(
    [
      {"type": "lifespan.startup"},
      {"type": "lifespan.shutdown"},
    ]
  )

  async def receive():
    return next(messages)

  async def send(message):
    sent_messages.append(message)

  asyncio.run(app({"type": "lifespan"}, receive, send))

  assert events == ["start", "stop"]
  assert sent_messages == [
    {"type": "lifespan.startup.complete"},
    {"type": "lifespan.shutdown.complete"},
  ]


def test_asgi_lifespan_forwards_wrapped_app_startup_and_shutdown(monkeypatch):
  events = []

  async def wrapped_app(scope, receive, send):
    events.append(("scope", scope["type"]))
    startup = await receive()
    events.append(startup["type"])
    await send({"type": "lifespan.startup.complete"})
    shutdown = await receive()
    events.append(shutdown["type"])
    await send({"type": "lifespan.shutdown.complete"})

  stub_supervisors(monkeypatch, events)

  from dj_queue.contrib.asgi import DjQueueLifespan

  app = DjQueueLifespan(wrapped_app)
  sent_messages = []
  messages = iter(
    [
      {"type": "lifespan.startup"},
      {"type": "lifespan.shutdown"},
    ]
  )

  async def receive():
    return next(messages)

  async def send(message):
    sent_messages.append(message)

  asyncio.run(app({"type": "lifespan"}, receive, send))

  assert events == [
    ("scope", "lifespan"),
    "lifespan.startup",
    "start",
    "stop",
    "lifespan.shutdown",
  ]
  assert sent_messages == [
    {"type": "lifespan.startup.complete"},
    {"type": "lifespan.shutdown.complete"},
  ]


def test_asgi_lifespan_forwards_wrapped_app_startup_failure(monkeypatch):
  events = []

  async def wrapped_app(_scope, receive, send):
    startup = await receive()
    assert startup["type"] == "lifespan.startup"
    await send({"type": "lifespan.startup.failed", "message": "wrapped app failed"})

  stub_supervisors(monkeypatch, events)

  from dj_queue.contrib.asgi import DjQueueLifespan

  app = DjQueueLifespan(wrapped_app)
  sent_messages = []
  messages = iter([{"type": "lifespan.startup"}])

  async def receive():
    return next(messages)

  async def send(message):
    sent_messages.append(message)

  asyncio.run(app({"type": "lifespan"}, receive, send))

  assert events == []
  assert sent_messages == [{"type": "lifespan.startup.failed", "message": "wrapped app failed"}]


def test_asgi_lifespan_ignores_wrapped_app_without_lifespan_support(monkeypatch):
  events = []

  async def wrapped_app(_scope, _receive, _send):
    raise ValueError("Django can only handle ASGI/HTTP connections, not lifespan.")

  stub_supervisors(monkeypatch, events)

  from dj_queue.contrib.asgi import DjQueueLifespan

  app = DjQueueLifespan(wrapped_app)
  sent_messages = []
  messages = iter(
    [
      {"type": "lifespan.startup"},
      {"type": "lifespan.shutdown"},
    ]
  )

  async def receive():
    return next(messages)

  async def send(message):
    sent_messages.append(message)

  asyncio.run(app({"type": "lifespan"}, receive, send))

  assert events == ["start", "stop"]
  assert sent_messages == [
    {"type": "lifespan.startup.complete"},
    {"type": "lifespan.shutdown.complete"},
  ]


def test_asgi_lifespan_can_skip_wrapped_lifespan_forwarding_explicitly(monkeypatch):
  events = []

  async def wrapped_app(_scope, _receive, _send):
    events.append("wrapped-app-started")
    await asyncio.sleep(0)

  stub_supervisors(monkeypatch, events)

  from dj_queue.contrib.asgi import DjQueueLifespan

  app = DjQueueLifespan(wrapped_app, forward_wrapped_lifespan=False)
  sent_messages = []
  messages = iter(
    [
      {"type": "lifespan.startup"},
      {"type": "lifespan.shutdown"},
    ]
  )

  async def receive():
    return next(messages)

  async def send(message):
    sent_messages.append(message)

  asyncio.run(app({"type": "lifespan"}, receive, send))

  assert events == ["start", "stop"]
  assert sent_messages == [
    {"type": "lifespan.startup.complete"},
    {"type": "lifespan.shutdown.complete"},
  ]


def test_embedded_server_executes_jobs_end_to_end(
  tmp_path, django_db_blocker, queue_test_settings
):
  queue_test_settings(
    databases=_sqlite_databases(tmp_path),
    tasks=_queue_tasks(database_alias="queue"),
  )

  with django_db_blocker.unblock():
    call_command("migrate", "dj_queue", database="queue", interactive=False, verbosity=0)
    supervisor = AsyncSupervisor.from_backend_config(backend_alias="default", standalone=False)
    supervisor.start()
    result = echo.enqueue("embedded")

    wait_until(
      lambda: (
        Job.objects.using("queue")
        .filter(
          pk=result.id,
          finished_at__isnull=False,
          return_value="embedded",
        )
        .exists()
      ),
      timeout=2,
    )

    fresh_result = echo.get_backend().get_result(result.id)

    assert fresh_result.status == TaskResultStatus.SUCCESSFUL
    assert fresh_result.return_value == "embedded"
    assert Process.objects.using("queue").filter(kind="Worker").exists() is True

    supervisor.stop()


def _crash_supervisor_poll(supervisor):
  def crash():
    raise RuntimeError("supervisor crashed")

  supervisor.poll_once = crash


def _prune_supervisor_row(supervisor):
  process_pk = supervisor.process.pk
  # wait_until retries a delete that hits a SQLite shared-cache table lock
  wait_until(lambda: Process.objects.filter(pk=process_pk).delete())


@pytest.mark.parametrize("fail", [_crash_supervisor_poll, _prune_supervisor_row])
def test_gunicorn_embedded_supervisor_failure_stops_runners_and_releases_lock(
  settings, monkeypatch, tmp_path, fail
):
  from dj_queue.contrib import gunicorn

  # the real lock path is shared by every test run started from this directory
  monkeypatch.setattr(
    gunicorn, "_supervisor_lock_path", lambda *, backend_alias: tmp_path / "supervisor.lock"
  )

  def lock_is_free():
    lock_file = gunicorn._try_acquire_supervisor_lock(backend_alias="default")
    if lock_file is None:
      return False
    gunicorn._release_supervisor_lock(lock_file)
    return True

  settings.TASKS = _queue_tasks()
  worker = type("Worker", (), {})()
  supervisor = gunicorn.post_fork(object(), worker)

  try:
    wait_until(lambda: Process.objects.filter(kind="Worker").exists())

    fail(supervisor)

    wait_until(lambda: Process.objects.exists() is False, timeout=3)
    wait_until(lock_is_free, timeout=3)
  finally:
    gunicorn.worker_exit(object(), worker)


@pytest.mark.parametrize("fail", [_crash_supervisor_poll, _prune_supervisor_row])
def test_asgi_embedded_supervisor_failure_stops_runners(settings, fail):
  from dj_queue.contrib.asgi import DjQueueLifespan

  settings.TASKS = _queue_tasks()

  def fail_and_wait_for_runners_to_stop(supervisor):
    try:
      wait_until(lambda: Process.objects.filter(kind="Worker").exists())
      fail(supervisor)
      wait_until(lambda: Process.objects.exists() is False, timeout=3)
    finally:
      connections.close_all()

  async def lifespan():
    app = DjQueueLifespan(lambda scope, receive, send: None, forward_wrapped_lifespan=False)
    received = asyncio.Queue()
    sent = asyncio.Queue()
    task = asyncio.create_task(app({"type": "lifespan"}, received.get, sent.put))
    await received.put({"type": "lifespan.startup"})
    assert await sent.get() == {"type": "lifespan.startup.complete"}
    try:
      await asyncio.to_thread(fail_and_wait_for_runners_to_stop, app.supervisor)
    finally:
      await received.put({"type": "lifespan.shutdown"})
      assert await sent.get() == {"type": "lifespan.shutdown.complete"}
      await task

  asyncio.run(lifespan())
