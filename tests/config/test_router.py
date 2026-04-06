import pytest
from django.core.management import call_command
from django.db import connections
from django.test import override_settings

from dj_queue.models import Job
from dj_queue.routers import DjQueueRouter

pytestmark = pytest.mark.filterwarnings(
  r"ignore:Overriding setting DATABASES can lead to unexpected behavior\.:UserWarning"
)


def _queue_tasks():
  return {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {
        "database_alias": "queue",
      },
    },
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


def _reset_connections():
  aliases = list(connections)
  connections.close_all()
  for alias in aliases:
    if hasattr(connections._connections, alias):
      delattr(connections._connections, alias)
  connections.__dict__.pop("settings", None)
  connections._settings = None


def _make_job():
  return Job.objects.create(
    task_path="tests.tasks.example",
    queue_name="default",
    priority=0,
    payload={
      "args": [],
      "kwargs": {},
    },
    backend_name="default",
  )


def _dj_queue_tables(alias):
  return {
    table_name
    for table_name in connections[alias].introspection.table_names()
    if table_name.startswith("dj_queue_")
  }


def test_router_directs_reads_and_writes_to_queue_db(tmp_path, django_db_blocker):
  with override_settings(
    DATABASES=_sqlite_databases(tmp_path),
    TASKS=_queue_tasks(),
  ):
    _reset_connections()
    try:
      with django_db_blocker.unblock():
        call_command("migrate", "dj_queue", database="queue", interactive=False, verbosity=0)

        job = _make_job()
        fetched_job = Job.objects.get(pk=job.pk)

        assert job._state.db == "queue"
        assert fetched_job._state.db == "queue"
        assert Job.objects.using("queue").filter(pk=job.pk).exists() is True
        assert _dj_queue_tables("default") == set()
    finally:
      _reset_connections()


def test_router_allows_queue_migrations_only_on_queue_db(tmp_path, django_db_blocker):
  with override_settings(
    DATABASES=_sqlite_databases(tmp_path),
    TASKS=_queue_tasks(),
  ):
    _reset_connections()
    try:
      with django_db_blocker.unblock():
        call_command("migrate", "dj_queue", database="queue", interactive=False, verbosity=0)

        router = DjQueueRouter()

        assert router.allow_migrate("queue", "dj_queue") is True
        assert router.allow_migrate("default", "dj_queue") is False
        assert "dj_queue_jobs" in _dj_queue_tables("queue")
        assert _dj_queue_tables("default") == set()
    finally:
      _reset_connections()
