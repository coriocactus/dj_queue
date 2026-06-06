from contextlib import contextmanager

import pytest

from benchmarks.scenarios import enqueue, runtime, scheduling
from dj_queue.models import Job
from dj_queue.operations.jobs import ClaimedJob


@pytest.mark.django_db
def test_single_enqueue_reports_sampled_query_count():
  metrics = enqueue.single_enqueue(2)

  assert metrics["job_count"] == 2
  assert metrics["query_count_sample"] > 0


@pytest.mark.django_db
def test_scheduled_promotion_seeds_backend_scoped_rows():
  metrics = scheduling.scheduled_promotion(2)

  assert metrics["promoted_count"] == 2
  assert metrics["ready_count"] == 2
  assert metrics["future_scheduled_count"] == 2


@pytest.mark.django_db
def test_worker_drain_seeds_backend_scoped_rows(monkeypatch):
  class FakeSupervisor:
    runners = (object(),)

    def start(self):
      return None

    def stop(self):
      return None

  def assert_seeded_ready_rows_are_claimable(size, **_kwargs):
    jobs = runtime.claim_ready_jobs(limit=size)
    assert len(jobs) == size
    for claimed_job in jobs:
      runtime.execute_claimed_job(claimed_job)

  monkeypatch.setattr(
    runtime.AsyncSupervisor,
    "from_backend_config",
    lambda **_kwargs: FakeSupervisor(),
  )
  monkeypatch.setattr(runtime, "_wait_for_drain", assert_seeded_ready_rows_are_claimable)

  metrics = runtime.worker_drain(2)

  assert metrics["completed_count"] == 2
  assert metrics["ready_count"] == 0


def test_held_xmin_worker_drain_samples_postgres_table_health(monkeypatch):
  events = []
  samples = iter(
    [
      {"dead_tuples": 1, "live_tuples": 2, "relation_bytes": 3},
      {"dead_tuples": 4, "live_tuples": 5, "relation_bytes": 6},
      {"dead_tuples": 7, "live_tuples": 8, "relation_bytes": 9},
    ]
  )

  @contextmanager
  def fake_held_snapshot():
    events.append("hold")
    try:
      yield
    finally:
      events.append("release")

  monkeypatch.setattr(runtime, "_ensure_postgres_benchmark", lambda: events.append("postgres"))
  monkeypatch.setattr(runtime, "_postgres_bloat_totals", lambda: next(samples))
  monkeypatch.setattr(runtime, "_held_repeatable_read_snapshot", fake_held_snapshot)
  monkeypatch.setattr(
    runtime,
    "worker_drain",
    lambda size: {"duration_seconds": 1, "jobs_per_second": size, "completed_count": size},
  )

  metrics = runtime.held_xmin_worker_drain(2)

  assert events == ["postgres", "hold", "release"]
  assert metrics == {
    "duration_seconds": 1,
    "jobs_per_second": 2,
    "completed_count": 2,
    "held_xmin": True,
    "dead_tuples_before": 1,
    "dead_tuples_during": 4,
    "dead_tuples_after_release": 7,
    "live_tuples_before": 2,
    "live_tuples_during": 5,
    "live_tuples_after_release": 8,
    "relation_bytes_before": 3,
    "relation_bytes_during": 6,
    "relation_bytes_after_release": 9,
  }


def test_held_xmin_worker_drain_rejects_non_postgres(monkeypatch):
  monkeypatch.setattr(runtime, "connection", type("Connection", (), {"vendor": "sqlite"})())

  with pytest.raises(RuntimeError, match="requires PostgreSQL"):
    runtime.held_xmin_worker_drain(1)


@pytest.mark.django_db
def test_concurrency_contention_uses_claimed_job_execution_path(monkeypatch):
  seen = []
  original_execute_claimed_job = runtime.execute_claimed_job

  def capture(claimed_job, *, backend_alias="default"):
    seen.append(claimed_job)
    return original_execute_claimed_job(claimed_job, backend_alias=backend_alias)

  monkeypatch.setattr(runtime, "execute_claimed_job", capture)

  metrics = runtime.concurrency_contention(2)

  assert len(seen) == 2
  assert all(isinstance(claimed_job, ClaimedJob) for claimed_job in seen)
  assert Job.objects.filter(finished_at__isnull=False).count() == 2
  assert metrics["finished_count"] == 2


@pytest.mark.django_db
def test_concurrency_contention_reports_drain_query_counts():
  metrics = runtime.concurrency_contention(2)

  assert metrics["enqueue_query_count"] > 0
  assert metrics["claim_query_count"] > 0
  assert metrics["execute_query_count"] > 0
  assert metrics["drain_query_count"] == (
    metrics["claim_query_count"] + metrics["execute_query_count"]
  )


@pytest.mark.django_db
def test_runtime_hot_key_contention_uses_async_supervisor(monkeypatch):
  events = []

  class FakeSupervisor:
    runners = (object(), object())

    def start(self):
      events.append("start")

    def stop(self):
      events.append("stop")

  def fake_from_backend_config(**kwargs):
    assert kwargs == {"backend_alias": "default", "standalone": False}
    return FakeSupervisor()

  def drain_hot_key_jobs(size, **_kwargs):
    completed = 0
    while completed < size:
      claimed_jobs = runtime.claim_ready_jobs(limit=1)
      assert claimed_jobs
      for claimed_job in claimed_jobs:
        runtime.execute_claimed_job(claimed_job)
        completed += 1

  monkeypatch.setattr(
    runtime.AsyncSupervisor,
    "from_backend_config",
    fake_from_backend_config,
  )
  monkeypatch.setattr(runtime, "_wait_for_drain", drain_hot_key_jobs)

  metrics = runtime.runtime_hot_key_contention(2)

  assert events == ["start", "stop"]
  assert metrics["completed_count"] == 2
  assert metrics["blocked_count"] == 0
  assert metrics["claimed_count"] == 0
  assert metrics["runner_count"] == 2
  assert Job.objects.filter(finished_at__isnull=False).count() == 2


@pytest.mark.django_db
def test_ordered_selector_claim_reports_claim_query_count():
  metrics = runtime.ordered_selector_claim(6)

  assert metrics["finished_count"] == 6
  assert metrics["claim_query_count"] > 0
  assert metrics["claim_duration_seconds"] > 0
  assert metrics["execute_query_count"] > 0
  assert metrics["execute_duration_seconds"] > 0
