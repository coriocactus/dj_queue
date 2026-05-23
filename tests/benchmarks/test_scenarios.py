import pytest

from benchmarks.scenarios import runtime, scheduling
from dj_queue.models import Job
from dj_queue.operations.jobs import ClaimedJob


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

  assert metrics["claim_query_count"] > 0
  assert metrics["execute_query_count"] > 0
  assert metrics["drain_query_count"] == (
    metrics["claim_query_count"] + metrics["execute_query_count"]
  )
