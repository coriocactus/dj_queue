from importlib import import_module
from types import SimpleNamespace

import pytest
from django.db import DatabaseError
from django.utils import timezone

from dj_queue import health, observability, postgres_diagnostics
from dj_queue.queue_state import empty_queue_state_summary

pytestmark = pytest.mark.django_db


@pytest.mark.parametrize(
  ("owner", "names"),
  [
    (
      "reads.queues",
      ("queue_rows", "queue_snapshot", "queue_ready_count", "queue_latency_seconds"),
    ),
    ("reads.processes", ("process_rows", "process_cutoff_for_backend", "has_live_processes")),
    ("reads.controls", ("recurring_rows_for_backend", "semaphore_rows_for_backend")),
    ("reads.jobs", ("failed_job_metrics",)),
    ("health", ("deep_health_problems", "postgres_health_problems")),
    ("postgres_diagnostics", ("postgres_diagnostics_for_backend", "postgres_autovacuum_sql")),
  ],
)
def test_observability_preserves_read_imports(owner, names):
  module = import_module(f"dj_queue.{owner}")
  for name in names:
    assert getattr(observability, name) is getattr(module, name)


def test_supplied_empty_queue_projection_does_not_reload(django_assert_num_queries):
  now = timezone.now()
  with django_assert_num_queries(0):
    row = observability.queue_snapshot(
      backend_alias="default",
      queue_name="empty",
      now=now,
      process_cutoff=now,
      state_summary=empty_queue_state_summary("empty"),
      paused=False,
      oldest_ready_at=None,
      oldest_scheduled_at=None,
      oldest_blocked_at=None,
      live_workers=[],
    )
  assert row == {
    "name": "empty",
    "ready_count": 0,
    "scheduled_count": 0,
    "claimed_count": 0,
    "blocked_count": 0,
    "failed_count": 0,
    "finished_count": 0,
    "invalid_count": 0,
    "paused": False,
    "latency_seconds": None,
    "oldest_scheduled_at": None,
    "oldest_blocked_at": None,
    "live_worker_count": 0,
  }


def test_diagnostic_errors_remain_advisory(monkeypatch):
  now = timezone.now()
  monkeypatch.setattr(
    postgres_diagnostics,
    "database_capabilities",
    lambda alias: SimpleNamespace(backend_family="postgresql"),
  )

  def unavailable(**kwargs):
    raise DatabaseError("diagnostics unavailable")

  monkeypatch.setattr(postgres_diagnostics, "postgres_queue_table_rows", unavailable)
  payload = observability.stats_payload(now=now)
  assert payload["backends"][0]["postgres_diagnostics"] == {
    "error": "diagnostics unavailable",
    "captured_at": now,
  }
  assert observability.postgres_health_problems(backend_alias="default", now=now) == ()


def test_old_transaction_alone_is_not_unhealthy(monkeypatch):
  monkeypatch.setattr(
    health,
    "postgres_diagnostics_for_backend",
    lambda **kwargs: {
      "queue_tables": [{"dead_tuples": 0, "dead_tuple_ratio": 0.0}],
      "xmin_activity": [{"state": "idle in transaction", "transaction_age_seconds": 500}],
      "replication_slots": (),
      "prepared_transactions": (),
      "long_transaction_threshold_seconds": 300,
    },
  )
  assert observability.postgres_health_problems(backend_alias="default") == ()


@pytest.mark.postgres
def test_postgres_diagnostics_read_real_catalogs():
  diagnostics = observability.postgres_diagnostics_for_backend(backend_alias="default")
  assert "error" not in diagnostics
  assert {row["table_name"] for row in diagnostics["queue_tables"]} == {
    model._meta.db_table for model in observability.POSTGRES_DIAGNOSTIC_TABLE_MODELS
  }
