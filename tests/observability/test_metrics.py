from dataclasses import replace
from unittest.mock import Mock

from dj_queue.metrics import MetricSample, metric_families
from dj_queue.observability import BackendSnapshot


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


def test_metric_family_metadata_and_empty_input_do_not_load_snapshots(monkeypatch):
  load = Mock(side_effect=AssertionError("supplied snapshots must not load data"))
  monkeypatch.setattr("dj_queue.metrics.observability.all_backend_snapshots", load)

  families = metric_families(snapshots=iter(()))

  assert [(family.name, family.help_text, family.labels) for family in families] == [
    (
      "dj_queue_queue_jobs",
      "Current job count by backend, queue, and state",
      ("backend", "queue", "state"),
    ),
    ("dj_queue_queue_paused", "Whether a queue is paused for a backend", ("backend", "queue")),
    (
      "dj_queue_queue_latency_seconds",
      "Latency of the oldest ready job in a backend queue",
      ("backend", "queue"),
    ),
    (
      "dj_queue_queue_live_workers",
      "Live workers that can service a backend queue",
      ("backend", "queue"),
    ),
    (
      "dj_queue_runner_processes",
      "Current runner process count by backend and liveness",
      ("backend", "status"),
    ),
    (
      "dj_queue_runner_processes_by_kind",
      "Current runner process count by backend, kind, and liveness",
      ("backend", "kind", "status"),
    ),
    ("dj_queue_recurring_tasks", "Current recurring task count by backend", ("backend",)),
    ("dj_queue_semaphores", "Current semaphore count by queue database", ("queue_database",)),
    ("dj_queue_process_rows", "Current process row count by backend", ("backend",)),
    ("dj_queue_failed_jobs", "Current failed job count by backend", ("backend",)),
    (
      "dj_queue_failed_job_oldest_age_seconds",
      "Age of the oldest failed job by backend",
      ("backend",),
    ),
    (
      "dj_queue_failed_job_retention_seconds",
      "Configured failed-job retention window by backend",
      ("backend",),
    ),
    (
      "dj_queue_failed_jobs_over_retention",
      "Current failed job count older than configured retention by backend",
      ("backend",),
    ),
  ]
  assert all(family.metric_type == "gauge" and family.samples == () for family in families)
  assert metric_families(snapshots=[]) == families
  load.assert_not_called()


def test_metric_samples_preserve_iterator_order_zeroes_and_database_scope(monkeypatch):
  first = BackendSnapshot(
    backend_alias="z",
    queue_database_alias="shared",
    process_alive_threshold=60,
    queue_rows=tuple(
      {
        "name": name,
        "ready_count": 1,
        "claimed_count": 2,
        "scheduled_count": 3,
        "blocked_count": 4,
        "failed_count": 5,
        "finished_count": 6,
        "invalid_count": 7,
        "paused": latency is None,
        "latency_seconds": latency,
        "live_worker_count": 0,
      }
      for name, latency in (("z", None), ("a", 0))
    ),
    process_rows=({},),
    recurring_rows=({}, {}),
    semaphore_rows=({}, {}),
    runner_metrics={
      "live": 2,
      "stale": 1,
      "by_kind": {"Worker": {"live": 2}, "Dispatcher": {"stale": 1}},
    },
    failed_metrics={
      "count": 0,
      "oldest_age_seconds": 0,
      "retention_seconds": 0,
      "over_retention_count": 0,
    },
  )
  second = replace(
    first,
    backend_alias="a",
    semaphore_rows=({},),
    failed_metrics={"count": 1, "oldest_age_seconds": None, "retention_seconds": None},
  )
  third = replace(first, backend_alias="m", queue_database_alias="separate", failed_metrics=None)
  snapshots = (first, second, third)
  load = Mock(return_value=iter(snapshots))
  monkeypatch.setattr("dj_queue.metrics.observability.all_backend_snapshots", load)

  families = metric_families(snapshots=iter(snapshots))
  load.assert_not_called()
  assert metric_families() == families
  load.assert_called_once_with()
  by_name = {family.name: family.samples for family in families}

  assert by_name["dj_queue_queue_jobs"] == tuple(
    MetricSample((backend, queue, state), value)
    for backend in ("z", "a", "m")
    for queue in ("z", "a")
    for state, value in (
      ("ready", 1),
      ("scheduled", 3),
      ("claimed", 2),
      ("blocked", 4),
      ("failed", 5),
      ("finished", 6),
      ("invalid", 7),
    )
  )
  assert by_name["dj_queue_queue_latency_seconds"] == tuple(
    MetricSample((backend, "a"), 0) for backend in ("z", "a", "m")
  )
  assert by_name["dj_queue_runner_processes_by_kind"] == tuple(
    MetricSample((backend, kind, status), value)
    for backend in ("z", "a", "m")
    for kind, status, value in (
      ("Dispatcher", "live", 0),
      ("Dispatcher", "stale", 1),
      ("Worker", "live", 2),
      ("Worker", "stale", 0),
    )
  )
  assert by_name["dj_queue_semaphores"] == (
    MetricSample(("shared",), 2),
    MetricSample(("separate",), 2),
  )
  assert by_name["dj_queue_failed_jobs"] == (MetricSample(("z",), 0), MetricSample(("a",), 1))
  for name in ("oldest_age_seconds", "retention_seconds"):
    assert by_name[f"dj_queue_failed_job_{name}"] == (MetricSample(("z",), 0),)
  assert by_name["dj_queue_failed_jobs_over_retention"] == (MetricSample(("z",), 0),)
  for name, count in (("recurring_tasks", 2), ("process_rows", 1)):
    assert by_name[f"dj_queue_{name}"] == tuple(
      MetricSample((backend,), count) for backend in ("z", "a", "m")
    )
