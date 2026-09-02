import json

import pytest

from bin import prerelease


class LatencyProbe:
  def __init__(self, x, y):
    self.values = {"X": x, "Y": y}

  def enqueue_latencies(self, _run_id, label):
    return self.values[label]


def test_smoke_defaults_use_small_isolated_run(tmp_path):
  result_dir = tmp_path / "results"

  args = prerelease.parse_args(
    [
      "--from-ref",
      "old",
      "--to-ref",
      "new",
      "--backend",
      "sqlite",
      "--result-dir",
      str(result_dir),
      "--smoke",
    ]
  )

  assert args.duration == 30
  assert args.seed_jobs == 100
  assert args.seed_queues == 10
  assert args.seed_semaphores == 10
  assert args.seed_recurring_executions == 100
  assert args.calibration_jobs == 100
  assert args.database_name == str(result_dir / "dj_queue_prerelease.sqlite3")


def test_phase_plan_preserves_ten_minute_rollout_order():
  plan = prerelease.PhasePlan.for_duration(600)

  assert plan.migration_at == 180
  assert plan.y_start_at == 240
  assert plan.producer_switch_at == 300
  assert plan.x_stop_at == 360
  assert plan.producer_stop_at == 480


def test_performance_results_accept_valid_rollout():
  plan = prerelease.PhasePlan.for_duration(30)
  samples = [
    {
      "elapsed_seconds": 1.5,
      "completed_x": 15,
      "completed_y": 0,
      "depth": 100,
      "deadlocks": 3,
    },
    {
      "elapsed_seconds": 6,
      "completed_x": 60,
      "completed_y": 0,
      "depth": 102,
      "deadlocks": 3,
    },
    {
      "elapsed_seconds": 8,
      "completed_x": 80,
      "completed_y": 0,
      "depth": 104,
      "deadlocks": 3,
    },
    {
      "elapsed_seconds": 10,
      "completed_x": 90,
      "completed_y": 0,
      "depth": 200,
      "deadlocks": 3,
    },
    {
      "elapsed_seconds": 16,
      "completed_x": 90,
      "completed_y": 0,
      "depth": 110,
      "deadlocks": 3,
    },
    {
      "elapsed_seconds": 20,
      "completed_x": 90,
      "completed_y": 0,
      "depth": 100,
      "deadlocks": 3,
    },
    {
      "elapsed_seconds": 23,
      "completed_x": 90,
      "completed_y": 27,
      "depth": 100,
      "deadlocks": 3,
    },
  ]

  result = prerelease.performance_results(
    samples,
    LatencyProbe([8, 10], [10, 12]),
    run_id="run",
    plan=plan,
    migration_finished_at=9.5,
  )

  assert result["healthy"] is True
  assert result["x_throughput"] == pytest.approx(10)
  assert result["y_throughput"] == pytest.approx(9)
  assert result["x_enqueue_p95_ms"] == 10
  assert result["y_enqueue_p95_ms"] == 12
  assert result["queue_recovered_at_seconds"] == 16


def test_performance_results_reject_regressions_and_collector_errors():
  plan = prerelease.PhasePlan.for_duration(100)
  samples = [
    {"elapsed_seconds": 5, "completed_x": 0, "depth": 10, "deadlocks": 0},
    {"elapsed_seconds": 29, "completed_x": 240, "depth": 10, "deadlocks": 0},
    {"elapsed_seconds": 32, "depth": 100, "deadlocks": 0},
    {"elapsed_seconds": 65, "completed_y": 0, "depth": 100, "deadlocks": 0},
    {"elapsed_seconds": 79, "completed_y": 70, "depth": 100, "deadlocks": 1},
    {"elapsed_seconds": 80, "collector_error": "connection lost"},
  ]

  result = prerelease.performance_results(
    samples,
    LatencyProbe([10], [13]),
    run_id="run",
    plan=plan,
    migration_finished_at=31,
  )

  assert result["healthy"] is False
  assert any("throughput" in problem for problem in result["problems"])
  assert any("p95" in problem for problem in result["problems"])
  assert any("queue depth" in problem for problem in result["problems"])
  assert any("deadlock" in problem for problem in result["problems"])
  assert any("metrics collector" in problem for problem in result["problems"])


def test_resolve_revisions_rejects_unrelated_revisions(monkeypatch):
  revisions = iter(("a" * 40, "b" * 40))
  monkeypatch.setattr(prerelease, "git_output", lambda *_args: next(revisions))
  monkeypatch.setattr(
    prerelease.subprocess,
    "run",
    lambda *_args, **_kwargs: type("Result", (), {"returncode": 1})(),
  )

  with pytest.raises(ValueError, match="is not an ancestor"):
    prerelease.resolve_revisions("old", "new")


def test_manifest_and_metrics_are_machine_readable(tmp_path):
  manifest_path = tmp_path / "manifest.json"
  metrics_path = tmp_path / "metrics.jsonl"

  prerelease.write_manifest(manifest_path, {"status": "passed", "rate": 12.5})
  metrics_path.write_text('{"depth": 2}\n{"depth": 0}\n', encoding="utf-8")

  assert json.loads(manifest_path.read_text(encoding="utf-8")) == {
    "rate": 12.5,
    "status": "passed",
  }
  assert prerelease.load_samples(metrics_path) == [{"depth": 2}, {"depth": 0}]
