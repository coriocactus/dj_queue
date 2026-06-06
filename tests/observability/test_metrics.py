from dj_queue.observability import BackendSnapshot
from dj_queue.metrics import MetricSample, metric_families


def test_metric_families_project_snapshot_without_prometheus_dependency():
  families = metric_families(
    snapshots=[
      BackendSnapshot(
        backend_alias="default",
        queue_database_alias="queue",
        process_alive_threshold=60,
        queue_rows=(
          {
            "name": "alpha",
            "ready_count": 2,
            "claimed_count": 1,
            "scheduled_count": 0,
            "blocked_count": 0,
            "failed_count": 0,
            "finished_count": 3,
            "invalid_count": 0,
            "paused": False,
            "latency_seconds": 4.5,
            "live_worker_count": 1,
          },
        ),
        runner_metrics={
          "live": 1,
          "stale": 0,
          "by_kind": {"Worker": {"live": 1, "stale": 0}},
        },
        failed_metrics={
          "count": 1,
          "oldest_created_at": None,
          "oldest_age_seconds": 30.0,
          "retention_seconds": 60,
          "over_retention_count": 1,
          "oldest_over_retention_created_at": None,
          "oldest_over_retention_age_seconds": 30.0,
        },
        recurring_rows=({"key": "nightly"},),
        semaphore_rows=({"key": "account:1"},),
        process_rows=({"name": "worker-1"},),
      )
    ]
  )
  by_name = {family.name: family for family in families}

  assert {family.metric_type for family in families} == {"gauge"}
  assert by_name["dj_queue_queue_jobs"].samples[0] == MetricSample(
    labels=("default", "alpha", "ready"),
    value=2,
  )
  assert (
    MetricSample(labels=("default", "alpha"), value=4.5)
    in by_name["dj_queue_queue_latency_seconds"].samples
  )
  assert by_name["dj_queue_failed_jobs"].samples == (MetricSample(labels=("default",), value=1),)
  assert by_name["dj_queue_failed_job_oldest_age_seconds"].samples == (
    MetricSample(labels=("default",), value=30.0),
  )
  assert by_name["dj_queue_failed_job_retention_seconds"].samples == (
    MetricSample(labels=("default",), value=60),
  )
  assert by_name["dj_queue_failed_jobs_over_retention"].samples == (
    MetricSample(labels=("default",), value=1),
  )
  assert by_name["dj_queue_semaphores"].samples == (MetricSample(labels=("queue",), value=1),)
