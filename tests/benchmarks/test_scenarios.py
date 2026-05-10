import pytest

from benchmarks.scenarios import runtime, scheduling


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
      runtime.complete_claimed_job(claimed_job.job.id, claimed_job.job.payload["args"][0])

  monkeypatch.setattr(
    runtime.AsyncSupervisor,
    "from_backend_config",
    lambda **_kwargs: FakeSupervisor(),
  )
  monkeypatch.setattr(runtime, "_wait_for_drain", assert_seeded_ready_rows_are_claimable)

  metrics = runtime.worker_drain(2)

  assert metrics["completed_count"] == 2
  assert metrics["ready_count"] == 0
