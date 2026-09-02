from datetime import timedelta
from types import SimpleNamespace

import pytest
from django.utils import timezone

from dj_queue import observability
from dj_queue.models import (
  FailedExecution,
  Job,
  Pause,
  Process,
  ReadyExecution,
  RecurringExecution,
  RecurringTask,
  Semaphore,
)
from tests.factories import enqueue_ready_job, make_failed_job

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
  enqueue_ready_job(queue_name="alpha", backend_alias="default")
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

  assert snapshot.runner_metrics == {
    "live": 1,
    "stale": 0,
    "by_kind": {"Worker": {"live": 1, "stale": 0}},
  }
  assert snapshot.queue_rows[0]["live_worker_count"] == 1
  assert [row["name"] for row in snapshot.process_rows] == ["default-worker"]
  assert snapshot.process_rows[0]["backend_alias"] == "default"


def test_backend_snapshot_treats_malformed_worker_queue_metadata_as_no_match():
  now = timezone.now()
  enqueue_ready_job(queue_name="alpha")
  Process.objects.create(
    backend_alias="default",
    kind="Worker",
    pid=101,
    hostname="localhost",
    name="bad-container-worker",
    metadata=["not", "a", "mapping"],
    last_heartbeat_at=now,
  )
  Process.objects.create(
    backend_alias="default",
    kind="Worker",
    pid=102,
    hostname="localhost",
    name="bad-selector-worker",
    metadata={"queues": [1]},
    last_heartbeat_at=now,
  )

  snapshot = observability.backend_snapshot(backend_alias="default", now=now)

  assert snapshot.queue_rows[0]["live_worker_count"] == 0
  assert [row["name"] for row in snapshot.process_rows] == [
    "bad-container-worker",
    "bad-selector-worker",
  ]


def test_all_backend_snapshots_reuses_shared_queue_database_semaphore_rows(settings, monkeypatch):
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
  calls = []

  def semaphore_rows_for_backend(*, backend_alias):
    calls.append(backend_alias)
    return ({"key": f"{backend_alias}:semaphore"},)

  monkeypatch.setattr(observability, "semaphore_rows_for_backend", semaphore_rows_for_backend)

  snapshots = observability.all_backend_snapshots(now=timezone.now())

  assert calls == ["default"]
  assert [snapshot.backend_alias for snapshot in snapshots] == ["default", "critical"]
  assert snapshots[0].semaphore_rows == snapshots[1].semaphore_rows


def test_stats_payload_can_include_postgres_diagnostics(monkeypatch):
  now = timezone.now()

  monkeypatch.setattr(
    observability,
    "database_capabilities",
    lambda alias: SimpleNamespace(backend_family="postgresql"),
  )
  monkeypatch.setattr(
    observability,
    "postgres_queue_table_rows",
    lambda *, backend_alias: ({"table_name": "dj_queue_jobs", "dead_tuples": 12},),
  )
  monkeypatch.setattr(
    observability,
    "postgres_xmin_activity_rows",
    lambda *, backend_alias: ({"pid": 101, "state": "idle in transaction"},),
  )
  monkeypatch.setattr(
    observability,
    "postgres_replication_slot_rows",
    lambda *, backend_alias: (),
  )
  monkeypatch.setattr(
    observability,
    "postgres_prepared_transaction_rows",
    lambda *, backend_alias: (),
  )

  payload = observability.stats_payload(now=now)

  diagnostics = payload["backends"][0]["postgres_diagnostics"]
  assert diagnostics["queue_tables"] == ({"table_name": "dj_queue_jobs", "dead_tuples": 12},)
  assert diagnostics["xmin_activity"] == ({"pid": 101, "state": "idle in transaction"},)
  assert diagnostics["long_transaction_threshold_seconds"] == 300.0


def test_backend_snapshot_exposes_failed_retention_metrics(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {"clear_failed_jobs_after": 60},
    }
  }
  now = timezone.now()
  old_failed = make_failed_job(queue_name="alpha")
  recent_failed = make_failed_job(queue_name="alpha")
  FailedExecution.objects.filter(job=old_failed).update(created_at=now - timedelta(seconds=120))
  FailedExecution.objects.filter(job=recent_failed).update(created_at=now - timedelta(seconds=30))

  snapshot = observability.backend_snapshot(backend_alias="default", now=now)

  assert snapshot.failed_metrics == {
    "count": 2,
    "oldest_created_at": now - timedelta(seconds=120),
    "oldest_age_seconds": 120.0,
    "retention_seconds": 60,
    "over_retention_count": 1,
    "oldest_over_retention_created_at": now - timedelta(seconds=120),
    "oldest_over_retention_age_seconds": 120.0,
  }
  assert snapshot.stats_row()["failed_jobs"] == snapshot.failed_metrics


def test_deep_health_reports_failed_jobs_past_configured_retention(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {"clear_failed_jobs_after": 60},
    }
  }
  now = timezone.now()
  old_failed = make_failed_job(queue_name="alpha")
  FailedExecution.objects.filter(job=old_failed).update(created_at=now - timedelta(seconds=120))

  problems = observability.deep_health_problems(backend_alias="default", now=now)

  assert "1 failed execution rows exceed configured retention of 60 seconds" in problems


def test_postgres_health_problems_report_bloat_and_xmin_blockers(monkeypatch):
  diagnostics = {
    "queue_tables": (
      {
        "table_name": "dj_queue_ready_executions",
        "dead_tuples": 20_000,
        "dead_tuple_ratio": 0.5,
      },
    ),
    "xmin_activity": (
      {
        "pid": 101,
        "state": "idle in transaction",
        "transaction_age_seconds": 30,
      },
    ),
    "replication_slots": (),
    "prepared_transactions": (),
    "long_transaction_threshold_seconds": 300.0,
  }
  monkeypatch.setattr(
    observability,
    "postgres_diagnostics_for_backend",
    lambda **_kwargs: diagnostics,
  )

  problems = observability.postgres_health_problems(backend_alias="default")

  assert problems == (
    "1 PostgreSQL queue tables have high dead tuples: dj_queue_ready_executions",
    "1 PostgreSQL sessions or slots may be pinning xmin",
  )


def test_queue_rows_use_canonical_job_queue_names():
  now = timezone.now()
  ready = enqueue_ready_job(queue_name="canonical")
  ReadyExecution.objects.filter(job=ready).update(queue_name="drifted")

  rows = observability.queue_rows(
    backend_alias="default",
    now=now,
    process_cutoff=now - timedelta(minutes=1),
  )

  assert [row["name"] for row in rows] == ["canonical"]
  assert rows[0]["ready_count"] == 1
  assert rows[0]["latency_seconds"] is not None


def test_process_rows_expose_shutdown_drain_state():
  now = timezone.now()
  Process.objects.create(
    backend_alias="default",
    kind="Worker",
    pid=101,
    hostname="localhost",
    name="draining-worker",
    metadata={
      "shutdown_state": "draining",
      "shutdown_started_at": (now - timedelta(seconds=5)).isoformat(),
      "shutdown_timeout": 0.01,
      "active_jobs": 1,
    },
    last_heartbeat_at=now,
  )

  rows = observability.process_rows(
    backend_alias="default",
    now=now,
    process_cutoff=now - timedelta(minutes=1),
    scope="backend",
  )

  assert rows[0]["shutdown_state"] == "draining"
  assert rows[0]["shutdown_age_seconds"] == 5.0
  assert rows[0]["shutdown_timeout"] == 0.01
  assert rows[0]["active_jobs"] == 1


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
    active_count=1,
    limit=2,
    expires_at=now + timedelta(minutes=5),
  )

  snapshot = observability.backend_snapshot(backend_alias="critical", now=now)

  assert [row["name"] for row in snapshot.queue_rows] == ["shared"]
  assert snapshot.queue_rows[0]["paused"] is True
  assert [row["key"] for row in snapshot.recurring_rows] == ["nightly"]
  assert [row["key"] for row in snapshot.semaphore_rows] == ["account:1"]
  assert snapshot.semaphore_rows[0]["scope"] == "queue_database"
  assert snapshot.semaphore_rows[0]["queue_database_alias"] == "default"
  assert snapshot.semaphore_rows[0]["active_count"] == 1
  assert snapshot.semaphore_rows[0]["available_slots"] == 1


def test_deep_health_allows_semaphore_occupancy_above_a_reduced_limit():
  Semaphore.objects.create(
    key="account:1",
    value=0,
    active_count=2,
    limit=1,
    expires_at=timezone.now() + timedelta(minutes=5),
  )

  problems = observability.deep_health_problems(backend_alias="default")

  assert not any("semaphores have impossible slot counts" in problem for problem in problems)


def test_deep_health_reports_invalid_job_concurrency_policy():
  Job.objects.create(
    task_path="tests.tasks.echo",
    backend_alias="default",
    concurrency_key="account:1",
    concurrency_limit=1,
  )

  problems = observability.deep_health_problems(backend_alias="default")

  assert "1 jobs have invalid concurrency policy" in problems


def test_backend_snapshot_derives_active_count_from_authoritative_value():
  Semaphore.objects.create(
    key="account:mixed-version",
    value=0,
    active_count=0,
    limit=1,
    expires_at=timezone.now() + timedelta(minutes=5),
  )

  snapshot = observability.backend_snapshot(backend_alias="default", now=timezone.now())

  assert snapshot.semaphore_rows[0]["active_count"] == 1
  assert snapshot.semaphore_rows[0]["available_slots"] == 0


def test_deep_health_allows_unresolved_legacy_recurring_reservation():
  RecurringExecution.objects.create(
    backend_alias="default",
    task_key="legacy",
    run_at=timezone.now(),
    intended_job_id=None,
    job=None,
  )

  problems = observability.deep_health_problems(backend_alias="default")

  assert not any("recurring execution reservations have no job" in problem for problem in problems)


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
