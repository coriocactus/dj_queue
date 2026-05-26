from datetime import timedelta

import pytest
from django.utils import timezone

from dj_queue import observability
from dj_queue.models import Pause, Process, RecurringTask, Semaphore
from tests.factories import make_ready_job


pytestmark = pytest.mark.django_db(transaction=True)


def test_backend_snapshot_filters_workers_to_backend(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {"database_alias": "default"},
    },
    "critical": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {"database_alias": "default"},
    },
  }
  now = timezone.now()
  make_ready_job(queue_name="alpha", backend_alias="default")
  Process.objects.create(
    backend_alias="default",
    kind="Worker",
    pid=101,
    hostname="localhost",
    name="default-worker",
    metadata={"queues": ["alpha"]},
    last_heartbeat_at=now,
  )
  Process.objects.create(
    backend_alias="critical",
    kind="Worker",
    pid=102,
    hostname="localhost",
    name="critical-worker",
    metadata={"queues": ["alpha"]},
    last_heartbeat_at=now,
  )

  snapshot = observability.backend_snapshot(backend_alias="default", now=now)

  assert snapshot["runner_metrics"] == {
    "live": 1,
    "stale": 0,
    "by_kind": {"Worker": {"live": 1, "stale": 0}},
  }
  assert snapshot["queue_rows"][0]["live_worker_count"] == 1
  assert [row["name"] for row in snapshot["process_rows"]] == ["default-worker"]
  assert snapshot["process_rows"][0]["backend_alias"] == "default"


def test_backend_snapshot_scopes_pause_and_recurring_rows_to_backend(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {"database_alias": "default"},
    },
    "critical": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {"database_alias": "default"},
    },
  }
  now = timezone.now()
  Pause.objects.create(backend_alias="critical", queue_name="shared")
  RecurringTask.objects.create(
    backend_alias="critical",
    key="nightly",
    task_path="tests.tasks.echo",
    payload={"args": [], "kwargs": {}},
    schedule="0 0 * * *",
    queue_name="shared",
    priority=0,
    static=False,
  )
  Semaphore.objects.create(
    key="account:1",
    value=1,
    limit=2,
    expires_at=now + timedelta(minutes=5),
  )

  snapshot = observability.backend_snapshot(backend_alias="critical", now=now)

  assert [row["name"] for row in snapshot["queue_rows"]] == ["shared"]
  assert snapshot["queue_rows"][0]["paused"] is True
  assert [row["key"] for row in snapshot["recurring_rows"]] == ["nightly"]
  assert [row["key"] for row in snapshot["semaphore_rows"]] == ["account:1"]
  assert snapshot["semaphore_rows"][0]["scope"] == "queue_database"
  assert snapshot["semaphore_rows"][0]["queue_database_alias"] == "default"


def test_configured_backend_aliases_ignore_non_dj_queue_backends(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {},
    },
    "other": {
      "BACKEND": "other.backend.Backend",
      "QUEUES": [],
      "OPTIONS": {},
    },
  }

  assert observability.configured_backend_aliases() == ("default",)


def test_configured_backend_aliases_falls_back_to_implicit_default_when_tasks_empty(settings):
  settings.TASKS = {}

  assert observability.configured_backend_aliases() == ("default",)
