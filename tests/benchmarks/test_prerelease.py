import json

import pytest

from bin import prerelease


class LatencyProbe:
  def __init__(self, x, y):
    self.values = {"X": x, "Y": y}
    self.windows = []

  def enqueue_latencies(self, _run_id, label, *, start, end):
    self.windows.append((label, start, end))
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
  assert prerelease.enforce_release_performance_gates(args) is False


def test_only_canonical_release_profile_enforces_performance_gates(tmp_path):
  base = [
    "--from-ref",
    "old",
    "--to-ref",
    "new",
    "--backend",
    "sqlite",
  ]
  release = prerelease.parse_args([*base, "--result-dir", str(tmp_path / "release")])
  short = prerelease.parse_args(
    [*base, "--result-dir", str(tmp_path / "short"), "--duration", "120"]
  )
  small_seed = prerelease.parse_args(
    [
      *base,
      "--result-dir",
      str(tmp_path / "small"),
      "--seed-jobs",
      "100",
      "--seed-recurring-executions",
      "100",
    ]
  )

  assert prerelease.enforce_release_performance_gates(release) is True
  assert prerelease.enforce_release_performance_gates(short) is False
  assert prerelease.enforce_release_performance_gates(small_seed) is False


def test_phase_plan_preserves_ten_minute_rollout_order():
  plan = prerelease.PhasePlan.for_duration(600)

  assert plan.migration_at == 180
  assert plan.y_start_at == 240
  assert plan.producer_switch_at == 300
  assert plan.x_stop_at == 360
  assert plan.producer_stop_at == 480


def test_postgres_diagnostics_bind_table_name_pattern():
  calls = []
  probe = object.__new__(prerelease.DatabaseProbe)
  probe.backend = "postgres"
  probe.scalar = lambda sql, params=(): calls.append((sql, params)) or 0

  result = probe._database_diagnostics()

  assert result == {"deadlocks": 0, "waiting_locks": 0, "dead_tuples": 0}
  assert calls[-1][1] == ["dj_queue_%"]


def test_mariadb_diagnostics_use_information_schema_lock_waits():
  calls = []
  probe = object.__new__(prerelease.DatabaseProbe)
  probe.backend = "mariadb"
  probe.rows = lambda _sql, _params=(): [("Innodb_deadlocks", "0")]
  probe.scalar = lambda sql, params=(): calls.append((sql, params)) or 0

  result = probe._database_diagnostics()

  assert result == {"deadlocks": 0, "waiting_locks": 0}
  assert calls == [("SELECT COUNT(*) FROM information_schema.INNODB_LOCK_WAITS", ())]


def test_performance_results_accept_valid_rollout():
  plan = prerelease.PhasePlan.for_duration(30)
  probe = LatencyProbe([8, 10], [10, 12])
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
    probe,
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
  assert probe.windows == [("X", 4.5, 9), ("Y", 19.5, 24)]


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
    LatencyProbe([10], [16]),
    run_id="run",
    plan=plan,
    migration_finished_at=31,
  )

  assert result["healthy"] is False
  assert any("throughput" in problem for problem in result["problems"])
  assert any("p95" in problem for problem in result["problems"])
  assert any("queue depth" in problem for problem in result["problems"])
  assert any("metrics collector" in problem for problem in result["problems"])
  assert result["deadlock_delta"] == 1
  assert not any("deadlock" in problem for problem in result["problems"])


def test_performance_results_allows_small_absolute_p95_increase():
  plan = prerelease.PhasePlan.for_duration(30)
  samples = [
    {"elapsed_seconds": 1.5, "completed_x": 0, "depth": 10, "deadlocks": 0},
    {"elapsed_seconds": 5, "completed_x": 0, "depth": 10, "deadlocks": 0},
    {"elapsed_seconds": 8, "completed_x": 30, "depth": 10, "deadlocks": 0},
    {"elapsed_seconds": 9, "completed_x": 40, "depth": 10, "deadlocks": 0},
    {"elapsed_seconds": 10, "depth": 10, "deadlocks": 0},
    {"elapsed_seconds": 11, "depth": 10, "deadlocks": 0},
    {"elapsed_seconds": 12, "depth": 10, "deadlocks": 0},
    {"elapsed_seconds": 19.5, "completed_y": 0, "depth": 10, "deadlocks": 0},
    {"elapsed_seconds": 24, "completed_y": 45, "depth": 10, "deadlocks": 0},
  ]

  result = prerelease.performance_results(
    samples,
    LatencyProbe([8], [12]),
    run_id="run",
    plan=plan,
    migration_finished_at=9.5,
  )

  assert result["healthy"] is True
  assert result["y_enqueue_p95_ms"] == result["x_enqueue_p95_ms"] + 4


def test_performance_results_rejects_one_sample_recovery():
  plan = prerelease.PhasePlan.for_duration(100)
  samples = [
    {"elapsed_seconds": 5, "completed_x": 0, "depth": 10, "deadlocks": 0},
    {"elapsed_seconds": 29, "completed_x": 240, "depth": 10, "deadlocks": 0},
    {"elapsed_seconds": 31, "depth": 10, "deadlocks": 0},
    {"elapsed_seconds": 32, "depth": 100, "deadlocks": 0},
    {"elapsed_seconds": 65, "completed_y": 0, "depth": 100, "deadlocks": 0},
    {"elapsed_seconds": 79, "completed_y": 140, "depth": 100, "deadlocks": 0},
  ]

  result = prerelease.performance_results(
    samples,
    LatencyProbe([10], [10]),
    run_id="run",
    plan=plan,
    migration_finished_at=30,
    enforce_performance=False,
  )

  assert result["queue_recovered_at_seconds"] is None
  assert any("queue depth" in problem for problem in result["problems"])


def test_performance_results_can_report_without_enforcing_short_smoke_ratios():
  plan = prerelease.PhasePlan.for_duration(30)
  samples = [
    {"elapsed_seconds": 2, "completed_x": 0, "depth": 10, "deadlocks": 0},
    {"elapsed_seconds": 5, "completed_x": 0, "depth": 10, "deadlocks": 0},
    {"elapsed_seconds": 8, "completed_x": 60, "depth": 10, "deadlocks": 0},
    {"elapsed_seconds": 10, "depth": 10, "deadlocks": 0},
    {"elapsed_seconds": 11, "depth": 10, "deadlocks": 0},
    {"elapsed_seconds": 12, "depth": 10, "deadlocks": 0},
    {"elapsed_seconds": 20, "completed_y": 0, "depth": 100, "deadlocks": 0},
    {"elapsed_seconds": 24, "completed_y": 20, "depth": 100, "deadlocks": 0},
  ]

  result = prerelease.performance_results(
    samples,
    LatencyProbe([10], [20]),
    run_id="run",
    plan=plan,
    migration_finished_at=9.5,
    enforce_performance=False,
  )

  assert result["healthy"] is True
  assert result["performance_gates_enforced"] is False
  assert result["y_throughput"] < result["x_throughput"] * 0.90
  assert result["y_enqueue_p95_ms"] > result["x_enqueue_p95_ms"] * 1.25
  assert result["queue_recovered_at_seconds"] == 10


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


def test_rollout_compatibility_requires_one_shared_protocol():
  compatible = prerelease.validate_rollout_compatibility(
    {"dj_queue_version": "0.13.1", "rollout_protocol": 1},
    {"dj_queue_version": "0.14.0", "rollout_protocol": 1},
  )

  assert compatible["rollout_protocol"] == 1
  with pytest.raises(TypeError, match="X does not publish"):
    prerelease.validate_rollout_compatibility(
      {"dj_queue_version": "0.13.0", "rollout_protocol": None},
      {"dj_queue_version": "0.14.0", "rollout_protocol": 1},
    )
  with pytest.raises(RuntimeError, match="incompatible rollout protocols"):
    prerelease.validate_rollout_compatibility(
      {"dj_queue_version": "0.13.1", "rollout_protocol": 1},
      {"dj_queue_version": "0.15.0", "rollout_protocol": 2},
    )


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
