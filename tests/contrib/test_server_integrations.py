import asyncio
import time

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


def test_gunicorn_post_fork_starts_one_embedded_supervisor(monkeypatch):
  started = []

  class StubSupervisor:
    def __init__(self, backend_alias):
      self.backend_alias = backend_alias

    def start(self):
      started.append(self.backend_alias)

  monkeypatch.setattr(
    "dj_queue.contrib.gunicorn.build_supervisor",
    lambda backend_alias="default": StubSupervisor(backend_alias),
  )

  from dj_queue.contrib import gunicorn

  worker_one = type("Worker", (), {"age": 1})()
  worker_two = type("Worker", (), {"age": 2})()

  gunicorn.post_fork(object(), worker_one)
  gunicorn.post_fork(object(), worker_two)

  assert started == ["default"]
  assert isinstance(worker_one._dj_queue_supervisor, StubSupervisor)
  assert hasattr(worker_two, "_dj_queue_supervisor") is False


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
