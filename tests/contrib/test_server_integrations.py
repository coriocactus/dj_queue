import asyncio
import os
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

import pytest
from django.core.management import call_command
from django.tasks import TaskResultStatus

from dj_queue.models import Job, Process
from dj_queue.runtime.supervisor import AsyncSupervisor
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


def wait_until(predicate, timeout=1):
  deadline = time.monotonic() + timeout
  while time.monotonic() < deadline:
    if predicate():
      return
    time.sleep(0.01)
  assert predicate()


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


def test_gunicorn_post_fork_starts_one_embedded_supervisor(monkeypatch):
  events = []
  released = []

  class Lock:
    pass

  locks = [Lock()]

  class StubSupervisor:
    polling_interval = 0.01

    def __init__(self, backend_alias):
      self.backend_alias = backend_alias

    def start(self):
      events.append(("start", self.backend_alias))

    def poll_once(self):
      events.append(("poll", self.backend_alias))

    def stop(self):
      events.append(("stop", self.backend_alias))

  monkeypatch.setattr(
    "dj_queue.contrib.gunicorn.build_supervisor",
    lambda backend_alias="default": StubSupervisor(backend_alias),
  )

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
    wait_until(lambda: any(event[0] == "poll" for event in events), timeout=1)

    assert isinstance(worker_one._dj_queue_supervisor, StubSupervisor)
    assert worker_two._dj_queue_supervisor is None
    acquired_lock = worker_one._dj_queue_supervisor_lock

    gunicorn.worker_exit(object(), worker_one)

    assert events[0] == ("start", "default")
    assert any(event[0] == "poll" for event in events)
    assert events[-1] == ("stop", "default")
    assert worker_one._dj_queue_supervisor is None
    assert released == [acquired_lock]
    assert worker_one._dj_queue_supervisor_lock is None
  finally:
    gunicorn.worker_exit(object(), worker_one)
    gunicorn.worker_exit(object(), worker_two)


def test_gunicorn_embedded_supervisor_poll_survives_errors(monkeypatch):
  errors = []
  polled_after_error = threading.Event()

  class StubSupervisor:
    polling_interval = 0.01
    backend_alias = "default"

    def __init__(self):
      self.poll_count = 0

    def start(self):
      return None

    def poll_once(self):
      self.poll_count += 1
      if self.poll_count == 1:
        raise RuntimeError("poll failed")
      polled_after_error.set()

    def stop(self):
      return None

  monkeypatch.setattr(
    "dj_queue.contrib.gunicorn.build_supervisor",
    lambda backend_alias="default": StubSupervisor(),
  )

  from dj_queue.contrib import gunicorn

  monkeypatch.setattr(
    gunicorn, "handle_thread_error", lambda error, **kwargs: errors.append(error)
  )
  monkeypatch.setattr(
    gunicorn,
    "_try_acquire_supervisor_lock",
    lambda **_kwargs: object(),
  )
  monkeypatch.setattr(
    gunicorn,
    "_release_supervisor_lock",
    lambda lock: None,
  )
  worker = type("Worker", (), {"age": 1})()

  supervisor = gunicorn.post_fork(object(), worker)

  try:
    assert polled_after_error.wait(timeout=1) is True
  finally:
    gunicorn.worker_exit(object(), worker)

  assert supervisor.poll_count >= 2
  assert [str(error) for error in errors] == ["poll failed"]


def test_gunicorn_retries_lock_until_replacement_worker_can_start(monkeypatch):
  events = []
  released = []

  class Lock:
    pass

  locks = [Lock(), Lock()]
  available = {"open": False}

  class StubSupervisor:
    polling_interval = 0.01

    def __init__(self, backend_alias):
      self.backend_alias = backend_alias

    def start(self):
      events.append(("start", self.backend_alias))

    def poll_once(self):
      events.append(("poll", self.backend_alias))

    def stop(self):
      events.append(("stop", self.backend_alias))

  monkeypatch.setattr(
    "dj_queue.contrib.gunicorn.build_supervisor",
    lambda backend_alias="default": StubSupervisor(backend_alias),
  )

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

  wait_until(lambda: getattr(worker_two, "_dj_queue_supervisor", None) is not None, timeout=1)
  acquired_lock = worker_two._dj_queue_supervisor_lock

  gunicorn.worker_exit(object(), worker_two)

  assert events.count(("start", "default")) == 2
  assert released == [first_lock, acquired_lock]


def test_gunicorn_worker_exit_waits_for_poll_thread_before_releasing_lock(monkeypatch):
  released = []
  poll_started = threading.Event()
  allow_poll_exit = threading.Event()

  class StubSupervisor:
    polling_interval = 0.01
    backend_alias = "default"

    def start(self):
      return None

    def poll_once(self):
      poll_started.set()
      allow_poll_exit.wait(timeout=1)

    def stop(self):
      return None

  monkeypatch.setattr(
    "dj_queue.contrib.gunicorn.build_supervisor",
    lambda backend_alias="default": StubSupervisor(),
  )

  from dj_queue.contrib import gunicorn

  monkeypatch.setattr(gunicorn, "_try_acquire_supervisor_lock", lambda **_kwargs: object())
  monkeypatch.setattr(gunicorn, "_release_supervisor_lock", lambda lock: released.append(lock))
  worker = type("Worker", (), {"age": 1})()

  gunicorn.post_fork(object(), worker)
  acquired_lock = worker._dj_queue_supervisor_lock
  assert poll_started.wait(timeout=1) is True

  exit_thread = threading.Thread(target=lambda: gunicorn.worker_exit(object(), worker))
  exit_thread.start()

  time.sleep(0.05)
  assert released == []

  allow_poll_exit.set()
  exit_thread.join(timeout=1)

  assert released == [acquired_lock]


def test_gunicorn_worker_exit_uses_shutdown_timeout_for_poll_thread(monkeypatch):
  released = []
  poll_started = threading.Event()

  class StubSupervisor:
    polling_interval = 0.01
    backend_alias = "default"
    config = SimpleNamespace(shutdown_timeout=0.01)

    def start(self):
      return None

    def poll_once(self):
      poll_started.set()
      time.sleep(1)

    def stop(self):
      return None

  monkeypatch.setattr(
    "dj_queue.contrib.gunicorn.build_supervisor",
    lambda backend_alias="default": StubSupervisor(),
  )

  from dj_queue.contrib import gunicorn

  monkeypatch.setattr(gunicorn, "_try_acquire_supervisor_lock", lambda **_kwargs: object())
  monkeypatch.setattr(gunicorn, "_release_supervisor_lock", lambda lock: released.append(lock))
  worker = type("Worker", (), {"age": 1})()

  gunicorn.post_fork(object(), worker)
  acquired_lock = worker._dj_queue_supervisor_lock
  assert poll_started.wait(timeout=1) is True

  started_at = time.monotonic()
  gunicorn.worker_exit(object(), worker)

  assert time.monotonic() - started_at < 0.5
  assert released == [acquired_lock]
  assert worker._dj_queue_supervisor_poll_thread is not None


def test_gunicorn_start_aborts_when_worker_exits_during_supervisor_start(monkeypatch):
  events = []
  released = []

  class StubSupervisor:
    polling_interval = 0.01
    backend_alias = "default"

    def __init__(self, worker):
      self.worker = worker

    def start(self):
      events.append("start")
      self.worker._dj_queue_supervisor_exiting = True

    def poll_once(self):
      events.append("poll")

    def stop(self):
      events.append("stop")

  from dj_queue.contrib import gunicorn

  worker = type("Worker", (), {"age": 1, "_dj_queue_supervisor_exiting": False})()
  lock = object()
  monkeypatch.setattr(gunicorn, "_try_acquire_supervisor_lock", lambda **_kwargs: lock)
  monkeypatch.setattr(gunicorn, "_release_supervisor_lock", lambda lock: released.append(lock))
  monkeypatch.setattr(
    gunicorn,
    "build_supervisor",
    lambda backend_alias="default": StubSupervisor(worker),
  )

  supervisor = gunicorn._start_embedded_supervisor(worker)

  assert supervisor is None
  assert events == ["start", "stop"]
  assert released == [lock]
  assert worker._dj_queue_supervisor is None
  assert worker._dj_queue_supervisor_lock is None


def test_asgi_lifespan_startup_and_shutdown_wrap_supervisor(monkeypatch):
  events = []

  class StubSupervisor:
    def start(self):
      events.append("start")

    def stop(self):
      events.append("stop")

  monkeypatch.setattr(
    "dj_queue.contrib.asgi.build_supervisor",
    lambda backend_alias="default": StubSupervisor(),
  )

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

  class StubSupervisor:
    def start(self):
      events.append("supervisor-start")

    def stop(self):
      events.append("supervisor-stop")

  async def wrapped_app(scope, receive, send):
    events.append(("scope", scope["type"]))
    startup = await receive()
    events.append(startup["type"])
    await send({"type": "lifespan.startup.complete"})
    shutdown = await receive()
    events.append(shutdown["type"])
    await send({"type": "lifespan.shutdown.complete"})

  monkeypatch.setattr(
    "dj_queue.contrib.asgi.build_supervisor",
    lambda backend_alias="default": StubSupervisor(),
  )

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
    "supervisor-start",
    "supervisor-stop",
    "lifespan.shutdown",
  ]
  assert sent_messages == [
    {"type": "lifespan.startup.complete"},
    {"type": "lifespan.shutdown.complete"},
  ]


def test_asgi_lifespan_forwards_wrapped_app_startup_failure(monkeypatch):
  class StubSupervisor:
    def start(self):
      raise AssertionError("supervisor should not start after wrapped startup failure")

  async def wrapped_app(_scope, receive, send):
    startup = await receive()
    assert startup["type"] == "lifespan.startup"
    await send({"type": "lifespan.startup.failed", "message": "wrapped app failed"})

  monkeypatch.setattr(
    "dj_queue.contrib.asgi.build_supervisor",
    lambda backend_alias="default": StubSupervisor(),
  )

  from dj_queue.contrib.asgi import DjQueueLifespan

  app = DjQueueLifespan(wrapped_app)
  sent_messages = []
  messages = iter([{"type": "lifespan.startup"}])

  async def receive():
    return next(messages)

  async def send(message):
    sent_messages.append(message)

  asyncio.run(app({"type": "lifespan"}, receive, send))

  assert sent_messages == [{"type": "lifespan.startup.failed", "message": "wrapped app failed"}]


def test_asgi_lifespan_ignores_wrapped_app_without_lifespan_support(monkeypatch):
  events = []

  class StubSupervisor:
    def start(self):
      events.append("start")

    def stop(self):
      events.append("stop")

  async def wrapped_app(_scope, _receive, _send):
    raise ValueError("Django can only handle ASGI/HTTP connections, not lifespan.")

  monkeypatch.setattr(
    "dj_queue.contrib.asgi.build_supervisor",
    lambda backend_alias="default": StubSupervisor(),
  )

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

  class StubSupervisor:
    def start(self):
      events.append("start")

    def stop(self):
      events.append("stop")

  async def wrapped_app(_scope, _receive, _send):
    events.append("wrapped-app-started")
    await asyncio.sleep(0)

  monkeypatch.setattr(
    "dj_queue.contrib.asgi.build_supervisor",
    lambda backend_alias="default": StubSupervisor(),
  )

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


def test_asgi_lifespan_prunes_stale_process_rows(monkeypatch):
  events = []

  class StubSupervisor:
    polling_interval = 0.01

    def start(self):
      events.append("start")

    def poll_once(self):
      events.append("poll")

    def stop(self):
      events.append("stop")

  monkeypatch.setattr(
    "dj_queue.contrib.asgi.build_supervisor",
    lambda backend_alias="default": StubSupervisor(),
  )

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
    message = next(messages)
    if message["type"] == "lifespan.shutdown":
      await asyncio.sleep(0.05)
    return message

  async def send(message):
    sent_messages.append(message)

  asyncio.run(app({"type": "lifespan"}, receive, send))

  assert events[0] == "start"
  assert "poll" in events[1:-1]
  assert events[-1] == "stop"
  assert sent_messages == [
    {"type": "lifespan.startup.complete"},
    {"type": "lifespan.shutdown.complete"},
  ]


def test_asgi_lifespan_poll_survives_errors(monkeypatch):
  errors = []
  events = []

  class StubSupervisor:
    polling_interval = 0.01
    backend_alias = "default"

    def __init__(self):
      self.poll_count = 0

    def start(self):
      events.append("start")

    def poll_once(self):
      self.poll_count += 1
      if self.poll_count == 1:
        raise RuntimeError("poll failed")
      events.append("poll")

    def stop(self):
      events.append("stop")

  monkeypatch.setattr(
    "dj_queue.contrib.asgi.build_supervisor",
    lambda backend_alias="default": StubSupervisor(),
  )

  from dj_queue.contrib import asgi

  monkeypatch.setattr(asgi, "handle_thread_error", lambda error, **kwargs: errors.append(error))
  app = asgi.DjQueueLifespan(lambda scope, receive, send: None)
  sent_messages = []
  messages = iter(
    [
      {"type": "lifespan.startup"},
      {"type": "lifespan.shutdown"},
    ]
  )

  async def receive():
    message = next(messages)
    if message["type"] == "lifespan.shutdown":
      await asyncio.sleep(0.05)
    return message

  async def send(message):
    sent_messages.append(message)

  asyncio.run(app({"type": "lifespan"}, receive, send))

  assert events[0] == "start"
  assert "poll" in events[1:-1]
  assert events[-1] == "stop"
  assert [str(error) for error in errors] == ["poll failed"]
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
