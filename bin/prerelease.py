#!/usr/bin/env -S uv run --script

import argparse
import hashlib
import json
import os
import shutil
import signal
import sqlite3
import statistics
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RUNTIME_SCRIPT = PROJECT_ROOT / "benchmarks" / "prerelease_runtime.py"
DEFAULT_SEED = {
  "jobs": 100_000,
  "queues": 100,
  "semaphores": 1_000,
  "recurring_executions": 100_000,
  "calibration_jobs": 10_000,
}
SMOKE_SEED = {
  "jobs": 100,
  "queues": 10,
  "semaphores": 10,
  "recurring_executions": 100,
  "calibration_jobs": 100,
}


@dataclass(frozen=True, slots=True)
class PhasePlan:
  duration: float
  migration_at: float
  y_start_at: float
  producer_switch_at: float
  x_stop_at: float
  producer_stop_at: float

  @classmethod
  def for_duration(cls, duration):
    return cls(
      duration=duration,
      migration_at=duration * 0.30,
      y_start_at=duration * 0.40,
      producer_switch_at=duration * 0.50,
      x_stop_at=duration * 0.60,
      producer_stop_at=duration * 0.80,
    )


@dataclass(frozen=True, slots=True)
class RevisionRuntime:
  label: str
  revision: str
  wheel: Path
  wheel_sha256: str
  python: Path


class ManagedProcess:
  def __init__(self, name, command, *, env, log_path):
    self.name = name
    self.log_path = Path(log_path)
    self.log_path.parent.mkdir(parents=True, exist_ok=True)
    self._log = self.log_path.open("w", encoding="utf-8")
    self.process = subprocess.Popen(
      command,
      cwd=self.log_path.parent,
      env=env,
      stdout=self._log,
      stderr=subprocess.STDOUT,
      text=True,
    )
    self.stopped = False

  def assert_running(self):
    return_code = self.process.poll()
    if return_code is not None and not self.stopped:
      raise RuntimeError(f"{self.name} exited early with code {return_code}; see {self.log_path}")

  def stop(self, *, timeout=75):
    if self.stopped:
      return
    self.stopped = True
    if self.process.poll() is None:
      self.process.send_signal(signal.SIGTERM)
      try:
        self.process.wait(timeout=timeout)
      except subprocess.TimeoutExpired:
        self.process.kill()
        self.process.wait(timeout=5)
    self._log.close()
    if self.process.returncode != 0:
      raise RuntimeError(
        f"{self.name} stopped with code {self.process.returncode}; see {self.log_path}"
      )

  def close(self):
    try:
      self.stop()
    except RuntimeError:
      pass


class DatabaseProbe:
  def __init__(self, args):
    self.backend = args.backend
    self.database_name = args.database_name
    self.connection = self._connect(args)
    self.placeholder = "?" if self.backend == "sqlite" else "%s"

  def _connect(self, args):
    if args.backend == "sqlite":
      connection = sqlite3.connect(args.database_name, timeout=30, check_same_thread=False)
      connection.execute("PRAGMA busy_timeout = 30000")
      return connection
    if args.backend == "postgres":
      import psycopg

      return psycopg.connect(
        dbname=args.database_name,
        user=args.database_user,
        password=args.database_password,
        host=args.database_host,
        port=args.database_port,
        autocommit=True,
      )

    import pymysql

    return pymysql.connect(
      database=args.database_name,
      user=args.database_user,
      password=args.database_password,
      host=args.database_host,
      port=args.database_port,
      autocommit=True,
    )

  def close(self):
    self.connection.close()

  def scalar(self, sql, params=()):
    cursor = self.connection.cursor()
    try:
      cursor.execute(sql, params)
      row = cursor.fetchone()
      return row[0] if row else 0
    finally:
      cursor.close()

  def rows(self, sql, params=()):
    cursor = self.connection.cursor()
    try:
      cursor.execute(sql, params)
      return list(cursor.fetchall())
    finally:
      cursor.close()

  def database_version(self):
    if self.backend == "sqlite":
      return self.scalar("SELECT sqlite_version()")
    return self.scalar("SELECT version()")

  def sample(self, *, run_id, elapsed, phase):
    like = f"{run_id}:%"
    values = {
      "timestamp": datetime.now(UTC).isoformat(),
      "elapsed_seconds": elapsed,
      "phase": phase,
      "ready": self.scalar("SELECT COUNT(*) FROM dj_queue_ready_executions"),
      "scheduled": self.scalar("SELECT COUNT(*) FROM dj_queue_scheduled_executions"),
      "claimed": self.scalar("SELECT COUNT(*) FROM dj_queue_claimed_executions"),
      "blocked": self.scalar("SELECT COUNT(*) FROM dj_queue_blocked_executions"),
      "failed": self.scalar("SELECT COUNT(*) FROM dj_queue_failed_executions"),
      "accepted_x": self.scalar(
        "SELECT COUNT(*) FROM dj_queue_prerelease_accepted "
        f"WHERE token LIKE {self.placeholder} AND producer_version = 'X'",
        [like],
      ),
      "accepted_y": self.scalar(
        "SELECT COUNT(*) FROM dj_queue_prerelease_accepted "
        f"WHERE token LIKE {self.placeholder} AND producer_version = 'Y'",
        [like],
      ),
      "completed_x": self.scalar(
        "SELECT COUNT(*) FROM dj_queue_prerelease_effects "
        f"WHERE token LIKE {self.placeholder} AND completions = 1 AND last_version = 'X'",
        [like],
      ),
      "completed_y": self.scalar(
        "SELECT COUNT(*) FROM dj_queue_prerelease_effects "
        f"WHERE token LIKE {self.placeholder} AND completions = 1 AND last_version = 'Y'",
        [like],
      ),
    }
    values["depth"] = sum(
      values[name] for name in ("ready", "scheduled", "claimed", "blocked", "failed")
    )
    values.update(self._database_diagnostics())
    return values

  def _database_diagnostics(self):
    try:
      if self.backend == "postgres":
        return {
          "deadlocks": self.scalar(
            "SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()"
          ),
          "waiting_locks": self.scalar("SELECT COUNT(*) FROM pg_locks WHERE NOT granted"),
          "dead_tuples": self.scalar(
            "SELECT COALESCE(SUM(n_dead_tup), 0) FROM pg_stat_user_tables WHERE relname LIKE %s",
            ["dj_queue_%"],
          ),
        }
      if self.backend in {"mysql", "mariadb"}:
        deadlock_rows = self.rows("SHOW GLOBAL STATUS LIKE 'Innodb_deadlocks'")
        values = {"deadlocks": int(deadlock_rows[0][1]) if deadlock_rows else 0}
        lock_waits_table = (
          "information_schema.INNODB_LOCK_WAITS"
          if self.backend == "mariadb"
          else "performance_schema.data_lock_waits"
        )
        values["waiting_locks"] = self.scalar(f"SELECT COUNT(*) FROM {lock_waits_table}")
        return values
      return {"deadlocks": 0, "waiting_locks": 0}
    except Exception as error:  # noqa: BLE001
      return {"diagnostics_error": str(error)}

  def enqueue_latencies(self, run_id, label):
    return [
      float(row[0])
      for row in self.rows(
        "SELECT enqueue_ms FROM dj_queue_prerelease_accepted "
        f"WHERE token LIKE {self.placeholder} AND producer_version = {self.placeholder}",
        [f"{run_id}:%", label],
      )
    ]

  def category_counts(self, run_id):
    return {
      row[0]: int(row[1])
      for row in self.rows(
        "SELECT category, COUNT(*) FROM dj_queue_prerelease_effects "
        f"WHERE token LIKE {self.placeholder} AND completions = 1 GROUP BY category",
        [f"{run_id}:%"],
      )
    }


class MetricsCollector:
  def __init__(self, probe, *, run_id, output_path, started_at, phase):
    self.probe = probe
    self.run_id = run_id
    self.output_path = Path(output_path)
    self.started_at = started_at
    self.phase = phase
    self.samples = []
    self.stop_event = threading.Event()
    self.thread = threading.Thread(target=self._run, name="prerelease-metrics", daemon=True)

  def start(self):
    self.output_path.parent.mkdir(parents=True, exist_ok=True)
    self.thread.start()

  def stop(self):
    self.stop_event.set()
    self.thread.join(timeout=5)
    self.probe.close()

  def _run(self):
    next_sample_at = time.monotonic()
    with self.output_path.open("w", encoding="utf-8") as output:
      while not self.stop_event.is_set():
        try:
          sample = self.probe.sample(
            run_id=self.run_id,
            elapsed=time.monotonic() - self.started_at,
            phase=self.phase[0],
          )
        except Exception as error:  # noqa: BLE001
          sample = {
            "timestamp": datetime.now(UTC).isoformat(),
            "elapsed_seconds": time.monotonic() - self.started_at,
            "phase": self.phase[0],
            "collector_error": str(error),
          }
        self.samples.append(sample)
        output.write(json.dumps(sample, sort_keys=True, default=str) + "\n")
        output.flush()
        next_sample_at += 1
        self.stop_event.wait(max(0, next_sample_at - time.monotonic()))


def parse_args(argv):
  parser = argparse.ArgumentParser(
    description="Run the ten-minute dj_queue mixed-version migration load proof."
  )
  parser.add_argument("--from-ref", required=True, help="Old package revision X.")
  parser.add_argument("--to-ref", required=True, help="New package revision Y.")
  parser.add_argument(
    "--backend", choices=("postgres", "mysql", "mariadb", "sqlite"), default="postgres"
  )
  parser.add_argument(
    "--django", default=">=6.0,<6.1", help="One Django range for both revisions."
  )
  parser.add_argument("--duration", type=float, default=600)
  parser.add_argument("--load-factor", type=float, default=0.675)
  parser.add_argument("--seed-jobs", type=int, default=DEFAULT_SEED["jobs"])
  parser.add_argument("--seed-queues", type=int, default=DEFAULT_SEED["queues"])
  parser.add_argument("--seed-semaphores", type=int, default=DEFAULT_SEED["semaphores"])
  parser.add_argument(
    "--seed-recurring-executions",
    type=int,
    default=DEFAULT_SEED["recurring_executions"],
  )
  parser.add_argument("--calibration-jobs", type=int, default=DEFAULT_SEED["calibration_jobs"])
  parser.add_argument("--workers", type=int, default=2)
  parser.add_argument("--threads", type=int, default=4)
  parser.add_argument("--database-name")
  parser.add_argument("--database-host", default="127.0.0.1")
  parser.add_argument("--database-port", type=int)
  parser.add_argument("--database-user")
  parser.add_argument("--database-password")
  parser.add_argument("--database-image", help="Database image name or immutable digest.")
  parser.add_argument("--result-dir")
  parser.add_argument("--keep-database", action="store_true")
  parser.add_argument(
    "--smoke",
    action="store_true",
    help="Use small seed sizes and a 30-second phase plan unless explicitly overridden.",
  )
  args = parser.parse_args(argv)
  apply_defaults(args, argv)
  validate_args(args)
  return args


def apply_defaults(args, argv):
  explicit = {item.split("=", 1)[0] for item in argv if item.startswith("--")}
  if args.smoke:
    if "--duration" not in explicit:
      args.duration = 30
    for option, key in (
      ("--seed-jobs", "jobs"),
      ("--seed-queues", "queues"),
      ("--seed-semaphores", "semaphores"),
      ("--seed-recurring-executions", "recurring_executions"),
      ("--calibration-jobs", "calibration_jobs"),
    ):
      if option not in explicit:
        setattr(args, option.removeprefix("--").replace("-", "_"), SMOKE_SEED[key])

  default_ports = {"postgres": 17432, "mysql": 17312, "mariadb": 17306}
  if args.database_port is None:
    args.database_port = default_ports.get(args.backend, 0)
  if args.database_user is None:
    args.database_user = "dj_queue" if args.backend == "postgres" else "root"
  if args.database_password is None:
    args.database_password = "dj_queue" if args.backend == "postgres" else "root"
  timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
  if args.result_dir is None:
    args.result_dir = str(
      PROJECT_ROOT / "benchmark-results" / f"prerelease-{args.backend}-{timestamp}"
    )
  if args.database_name is None:
    if args.backend == "sqlite":
      args.database_name = str(Path(args.result_dir) / "dj_queue_prerelease.sqlite3")
    else:
      args.database_name = f"dj_queue_prerelease_{timestamp.lower()}"


def validate_args(args):
  if args.duration < 20:
    raise ValueError("duration must be at least 20 seconds")
  if not 0.65 <= args.load_factor <= 0.70:
    raise ValueError("load-factor must stay between 0.65 and 0.70")
  for name in (
    "seed_jobs",
    "seed_queues",
    "seed_semaphores",
    "seed_recurring_executions",
    "calibration_jobs",
    "workers",
    "threads",
  ):
    if getattr(args, name) <= 0:
      raise ValueError(f"{name.replace('_', '-')} must be positive")
  if args.seed_recurring_executions > args.seed_jobs:
    raise ValueError("seed-recurring-executions cannot exceed seed-jobs")
  if "prerelease" not in args.database_name.lower():
    raise ValueError("database-name must contain 'prerelease'")


def resolve_revisions(from_ref, to_ref):
  from_revision = git_output("rev-parse", "--verify", f"{from_ref}^{{commit}}")
  to_revision = git_output("rev-parse", "--verify", f"{to_ref}^{{commit}}")
  ancestry = subprocess.run(
    ["git", "merge-base", "--is-ancestor", from_revision, to_revision],
    cwd=PROJECT_ROOT,
    check=False,
  )
  if ancestry.returncode != 0:
    raise ValueError(f"from revision {from_ref!r} is not an ancestor of {to_ref!r}")
  return from_revision, to_revision


def git_output(*args):
  return subprocess.run(
    ["git", *args],
    cwd=PROJECT_ROOT,
    check=True,
    capture_output=True,
    text=True,
  ).stdout.strip()


def build_runtime(label, revision, *, django_range, backend, result_dir, work_dir):
  source_dir = Path(work_dir) / f"source-{label.lower()}"
  source_dir.mkdir()
  archive_path = Path(work_dir) / f"source-{label.lower()}.tar"
  with archive_path.open("wb") as archive:
    subprocess.run(
      ["git", "archive", revision],
      cwd=PROJECT_ROOT,
      check=True,
      stdout=archive,
    )
  with tarfile.open(archive_path) as archive:
    archive.extractall(source_dir, filter="data")

  artifact_dir = Path(result_dir) / "artifacts" / label.lower()
  artifact_dir.mkdir(parents=True)
  subprocess.run(
    ["uv", "build", "--no-sources", "--wheel", "--out-dir", str(artifact_dir)],
    cwd=source_dir,
    check=True,
  )
  wheels = list(artifact_dir.glob("*.whl"))
  if len(wheels) != 1:
    raise RuntimeError(f"expected one {label} wheel, found {len(wheels)}")
  wheel = wheels[0]

  venv = Path(work_dir) / f"venv-{label.lower()}"
  subprocess.run(["uv", "venv", "--python", sys.executable, str(venv)], check=True)
  python = venv / "bin" / "python"
  dependencies = [str(wheel), f"django{django_range}"]
  if backend == "postgres":
    dependencies.append("psycopg>=3.3.3")
  elif backend in {"mysql", "mariadb"}:
    dependencies.append("pymysql>=1.1.2")
  subprocess.run(
    ["uv", "pip", "install", "--python", str(python), *dependencies],
    check=True,
  )
  return RevisionRuntime(
    label=label,
    revision=revision,
    wheel=wheel,
    wheel_sha256=sha256(wheel),
    python=python,
  )


def sha256(path):
  digest = hashlib.sha256()
  with Path(path).open("rb") as handle:
    for block in iter(lambda: handle.read(1024 * 1024), b""):
      digest.update(block)
  return digest.hexdigest()


def runtime_env(args, label):
  return {
    **os.environ,
    "DJANGO_SETTINGS_MODULE": "prerelease_settings",
    "PRERELEASE_BACKEND": args.backend,
    "PRERELEASE_DB_NAME": args.database_name,
    "PRERELEASE_DB_HOST": args.database_host,
    "PRERELEASE_DB_PORT": str(args.database_port),
    "PRERELEASE_DB_USER": args.database_user,
    "PRERELEASE_DB_PASSWORD": args.database_password,
    "PRERELEASE_RUNTIME_LABEL": label,
    "PRERELEASE_WORKERS": str(args.workers),
    "PRERELEASE_THREADS": str(args.threads),
  }


def run_runtime(runtime, args, *command, log_name, check=True):
  result = subprocess.run(
    [str(runtime.python), str(RUNTIME_SCRIPT), *map(str, command)],
    cwd=Path(args.result_dir),
    env=runtime_env(args, runtime.label),
    check=False,
    capture_output=True,
    text=True,
  )
  log_path = Path(args.result_dir) / "logs" / log_name
  log_path.parent.mkdir(parents=True, exist_ok=True)
  log_path.write_text(result.stdout + result.stderr, encoding="utf-8")
  if check and result.returncode != 0:
    raise RuntimeError(f"runtime command failed with code {result.returncode}; see {log_path}")
  return result


def runtime_json(runtime, args, *command, log_name, check=True):
  result = run_runtime(runtime, args, *command, log_name=log_name, check=check)
  lines = [line for line in result.stdout.splitlines() if line.startswith("{")]
  if not lines:
    raise RuntimeError(f"runtime command returned no JSON; see logs/{log_name}")
  return json.loads(lines[-1])


def start_runtime_process(runtime, args, name, *command):
  return ManagedProcess(
    name,
    [str(runtime.python), str(RUNTIME_SCRIPT), *map(str, command)],
    env=runtime_env(args, runtime.label),
    log_path=Path(args.result_dir) / "logs" / f"{name}.log",
  )


def wait_until(started_at, offset, processes):
  while True:
    for process in processes:
      process.assert_running()
    remaining = started_at + offset - time.monotonic()
    if remaining <= 0:
      return
    time.sleep(min(0.2, remaining))


def wait_for_drain(runtime, args, run_id, *, started_at, deadline, processes):
  index = 0
  while time.monotonic() < deadline:
    for process in processes:
      process.assert_running()
    status = runtime_json(
      runtime,
      args,
      "status",
      "--run-id",
      run_id,
      log_name=f"drain-{index:04d}.log",
    )
    if status["depth"] == 0:
      return status
    index += 1
    time.sleep(1)
  elapsed = time.monotonic() - started_at
  raise RuntimeError(f"queue did not drain by {elapsed:.1f}s")


def percentile(values, percent):
  if not values:
    return None
  ordered = sorted(values)
  index = round((len(ordered) - 1) * percent / 100)
  return ordered[index]


def sample_throughput(samples, field, start, end):
  selected = [
    sample
    for sample in samples
    if start <= sample.get("elapsed_seconds", -1) <= end and field in sample
  ]
  if len(selected) < 2:
    return None
  elapsed = selected[-1]["elapsed_seconds"] - selected[0]["elapsed_seconds"]
  if elapsed <= 0:
    return None
  return (selected[-1][field] - selected[0][field]) / elapsed


def performance_results(samples, probe, *, run_id, plan, migration_finished_at):
  warmup = max(1, plan.duration * 0.05)
  x_throughput = sample_throughput(
    samples,
    "completed_x",
    warmup,
    plan.migration_at,
  )
  y_throughput = sample_throughput(
    samples,
    "completed_y",
    plan.x_stop_at + warmup,
    plan.producer_stop_at,
  )
  x_p95 = percentile(probe.enqueue_latencies(run_id, "X"), 95)
  y_p95 = percentile(probe.enqueue_latencies(run_id, "Y"), 95)

  pre_window = max(2, min(60, plan.duration * 0.10))
  pre_depths = [
    sample["depth"]
    for sample in samples
    if plan.migration_at - pre_window <= sample.get("elapsed_seconds", -1) < plan.migration_at
    and "depth" in sample
  ]
  baseline_depth = statistics.median(pre_depths) if pre_depths else None
  recovery_window = min(60, max(2, plan.duration - migration_finished_at))
  recovery_threshold = (
    baseline_depth + max(10, baseline_depth * 0.20) if baseline_depth is not None else None
  )
  recovered_at = next(
    (
      sample["elapsed_seconds"]
      for sample in samples
      if migration_finished_at
      <= sample.get("elapsed_seconds", -1)
      <= migration_finished_at + recovery_window
      and recovery_threshold is not None
      and sample.get("depth", recovery_threshold + 1) <= recovery_threshold
    ),
    None,
  )
  deadlock_values = [sample["deadlocks"] for sample in samples if "deadlocks" in sample]
  deadlock_delta = deadlock_values[-1] - deadlock_values[0] if len(deadlock_values) >= 2 else None

  problems = []
  if x_throughput is None or y_throughput is None:
    problems.append("insufficient steady-state samples for throughput gate")
  elif y_throughput < x_throughput * 0.90:
    problems.append(f"Y throughput {y_throughput:.2f}/s is below 90% of X {x_throughput:.2f}/s")
  if x_p95 is None or y_p95 is None:
    problems.append("insufficient enqueue samples for latency gate")
  elif y_p95 > x_p95 * 1.25:
    problems.append(f"Y enqueue p95 {y_p95:.2f}ms exceeds 1.25x X {x_p95:.2f}ms")
  if recovered_at is None:
    problems.append("queue depth did not return to its pre-migration trend in 60 seconds")
  if deadlock_delta not in (None, 0):
    problems.append(f"database deadlock count increased by {deadlock_delta}")
  if any("collector_error" in sample for sample in samples):
    problems.append("the one-second metrics collector reported errors")
  if any("diagnostics_error" in sample for sample in samples):
    problems.append("database diagnostics reported errors")

  return {
    "healthy": not problems,
    "problems": problems,
    "x_throughput": x_throughput,
    "y_throughput": y_throughput,
    "x_enqueue_p95_ms": x_p95,
    "y_enqueue_p95_ms": y_p95,
    "baseline_queue_depth": baseline_depth,
    "queue_recovered_at_seconds": recovered_at,
    "deadlock_delta": deadlock_delta,
  }


def log_problems(result_dir):
  problems = []
  for path in (Path(result_dir) / "logs").glob("*.log"):
    content = path.read_text(encoding="utf-8", errors="replace")
    if "dj_queue infrastructure error" in content:
      problems.append(f"unexpected runtime error in {path.name}")
  return problems


def artifact_metadata(runtime):
  return {
    "label": runtime.label,
    "revision": runtime.revision,
    "wheel": str(runtime.wheel),
    "wheel_sha256": runtime.wheel_sha256,
  }


def write_manifest(path, value):
  Path(path).write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n")


def run(args):
  result_dir = Path(args.result_dir).resolve()
  result_dir.mkdir(parents=True, exist_ok=False)
  from_revision, to_revision = resolve_revisions(args.from_ref, args.to_ref)
  plan = PhasePlan.for_duration(args.duration)
  run_id = f"prerelease-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}-{os.getpid()}"
  manifest_path = result_dir / "manifest.json"
  processes = []
  collector = None
  database_created = False
  outcome = {
    "status": "running",
    "started_at": datetime.now(UTC).isoformat(),
    "run_id": run_id,
    "backend": args.backend,
    "database_name": args.database_name,
    "database_image": args.database_image,
    "django": args.django,
    "plan": asdict(plan),
  }
  write_manifest(manifest_path, outcome)

  try:
    with tempfile.TemporaryDirectory(prefix="dj-queue-prerelease-", delete=False) as work_dir:
      runtime_x = build_runtime(
        "X",
        from_revision,
        django_range=args.django,
        backend=args.backend,
        result_dir=result_dir,
        work_dir=work_dir,
      )
      runtime_y = build_runtime(
        "Y",
        to_revision,
        django_range=args.django,
        backend=args.backend,
        result_dir=result_dir,
        work_dir=work_dir,
      )
      outcome["artifacts"] = [artifact_metadata(runtime_x), artifact_metadata(runtime_y)]
      write_manifest(manifest_path, outcome)

      run_runtime(runtime_x, args, "create-database", log_name="create-database.log")
      database_created = True
      run_runtime(runtime_x, args, "migrate", log_name="migrate-x.log")
      seed = runtime_json(
        runtime_x,
        args,
        "seed",
        "--jobs",
        args.seed_jobs,
        "--queues",
        args.seed_queues,
        "--semaphores",
        args.seed_semaphores,
        "--recurring-executions",
        args.seed_recurring_executions,
        log_name="seed.log",
      )

      x_supervisor = start_runtime_process(runtime_x, args, "supervisor-x", "supervise")
      x_retry = start_runtime_process(runtime_x, args, "retry-x", "retry")
      processes.extend((x_supervisor, x_retry))
      time.sleep(1)
      calibration = runtime_json(
        runtime_x,
        args,
        "calibrate",
        "--run-id",
        run_id,
        "--jobs",
        args.calibration_jobs,
        log_name="calibration.log",
      )
      target_rate = calibration["capacity_jobs_per_second"] * args.load_factor

      probe = DatabaseProbe(args)
      outcome["database_version"] = probe.database_version()
      phase = ["x-steady"]
      started_at = time.monotonic()
      collector = MetricsCollector(
        probe,
        run_id=run_id,
        output_path=result_dir / "metrics.jsonl",
        started_at=started_at,
        phase=phase,
      )
      collector.start()
      x_producer = start_runtime_process(
        runtime_x,
        args,
        "producer-x",
        "produce",
        "--run-id",
        run_id,
        "--rate",
        target_rate,
      )
      processes.append(x_producer)

      wait_until(started_at, plan.migration_at, processes)
      phase[0] = "migrating-y"
      migration_started = time.monotonic()
      run_runtime(runtime_y, args, "migrate", log_name="migrate-y.log")
      migration_duration = time.monotonic() - migration_started
      migration_finished_at = time.monotonic() - started_at
      phase[0] = "x-expanded-schema"

      wait_until(started_at, plan.y_start_at, processes)
      y_supervisor = start_runtime_process(runtime_y, args, "supervisor-y", "supervise")
      y_retry = start_runtime_process(runtime_y, args, "retry-y", "retry")
      processes.extend((y_supervisor, y_retry))
      phase[0] = "mixed-workers"

      wait_until(started_at, plan.producer_switch_at, processes)
      y_producer = start_runtime_process(
        runtime_y,
        args,
        "producer-y",
        "produce",
        "--run-id",
        run_id,
        "--rate",
        target_rate,
      )
      processes.append(y_producer)
      x_producer.stop()
      phase[0] = "mixed-workers-y-producer"

      wait_until(
        started_at,
        plan.x_stop_at,
        [y_supervisor, y_retry, y_producer],
      )
      x_retry.stop()
      x_supervisor.stop()
      phase[0] = "y-steady"

      wait_until(
        started_at,
        plan.producer_stop_at,
        [y_supervisor, y_retry, y_producer],
      )
      y_producer.stop()
      run_runtime(
        runtime_y,
        args,
        "stop-recurring",
        "--run-id",
        run_id,
        log_name="stop-recurring.log",
      )
      phase[0] = "y-drain"
      drained = wait_for_drain(
        runtime_y,
        args,
        run_id,
        started_at=started_at,
        deadline=started_at + plan.duration,
        processes=[y_supervisor, y_retry],
      )
      y_retry.stop()
      y_supervisor.stop()
      verification = runtime_json(
        runtime_y,
        args,
        "verify",
        "--run-id",
        run_id,
        log_name="verify.log",
        check=False,
      )
      verification_finished_at = time.monotonic() - started_at
      collector.stop()
      collector = None

      analysis_probe = DatabaseProbe(args)
      try:
        performance = performance_results(
          samples=load_samples(result_dir / "metrics.jsonl"),
          probe=analysis_probe,
          run_id=run_id,
          plan=plan,
          migration_finished_at=migration_finished_at,
        )
        category_counts = analysis_probe.category_counts(run_id)
      finally:
        analysis_probe.close()

      outcome.update(
        {
          "seed": seed,
          "calibration": calibration,
          "target_rate": target_rate,
          "migration_duration_seconds": migration_duration,
          "verification_finished_at_seconds": verification_finished_at,
          "drained": drained,
          "verification": verification,
          "performance": performance,
          "category_counts": category_counts,
        }
      )
      write_manifest(manifest_path, outcome)
      problems = [*verification["problems"], *performance["problems"], *log_problems(result_dir)]
      if verification_finished_at > plan.duration:
        problems.append("drain and verification did not finish within the timed window")
      if problems:
        raise RuntimeError("; ".join(problems))

      outcome.update(
        {
          "status": "passed",
          "finished_at": datetime.now(UTC).isoformat(),
        }
      )
      write_manifest(manifest_path, outcome)
      print(json.dumps(outcome, indent=2, sort_keys=True, default=str))
      return 0
  except Exception as error:  # noqa: BLE001
    outcome.update(
      {
        "status": "failed",
        "finished_at": datetime.now(UTC).isoformat(),
        "error": str(error),
      }
    )
    write_manifest(manifest_path, outcome)
    print(str(error), file=sys.stderr)
    return 1
  finally:
    if collector is not None:
      try:
        collector.stop()
      except Exception as error:  # noqa: BLE001
        print(f"metrics collector cleanup failed: {error}", file=sys.stderr)
    for process in reversed(processes):
      process.close()
    if database_created and not args.keep_database:
      runtime = locals().get("runtime_y") or locals().get("runtime_x")
      if runtime is not None:
        run_runtime(runtime, args, "drop-database", log_name="drop-database.log", check=False)
    if "work_dir" in locals():
      shutil.rmtree(work_dir, ignore_errors=True)


def load_samples(path):
  with Path(path).open(encoding="utf-8") as source:
    return [json.loads(line) for line in source if line.strip()]


def main(argv):
  try:
    args = parse_args(argv)
    return run(args)
  except (OSError, subprocess.CalledProcessError, ValueError) as error:
    print(error, file=sys.stderr)
    return 2


if __name__ == "__main__":
  raise SystemExit(main(sys.argv[1:]))
