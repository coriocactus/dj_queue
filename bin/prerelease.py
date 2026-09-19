#!/usr/bin/env -S uv run --script

import argparse
import hashlib
import json
import os
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from benchmarks.prerelease_runtime import PHASE_WORKERS, batch_problems

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RUNTIME_SCRIPT = PROJECT_ROOT / "benchmarks" / "prerelease_runtime.py"
PHASE_TIMEOUT = 60


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
    try:
      self.process = subprocess.Popen(
        command,
        cwd=self.log_path.parent,
        env=env,
        stdout=self._log,
        stderr=subprocess.STDOUT,
        text=True,
      )
    except OSError:
      self._log.close()
      raise
    self.stopped = False

  def assert_running(self):
    if not self.stopped and self.process.poll() is not None:
      raise RuntimeError(f"{self.name} exited early; see {self.log_path}")

  def stop(self):
    if self.stopped:
      return
    try:
      self.assert_running()
      self.process.send_signal(signal.SIGTERM)
      try:
        self.process.wait(timeout=15)
      except subprocess.TimeoutExpired:
        self.process.kill()
        self.process.wait(timeout=5)
        raise RuntimeError(f"{self.name} did not stop gracefully; see {self.log_path}") from None
      if self.process.returncode != 0:
        raise RuntimeError(
          f"{self.name} stopped with {self.process.returncode}; see {self.log_path}"
        )
    finally:
      self.stopped = True
      self._log.close()


def parse_args(argv):
  parser = argparse.ArgumentParser(description="Check a mixed-version dj_queue upgrade.")
  parser.add_argument("--from-ref", required=True, help="Compatible old package revision X.")
  parser.add_argument("--to-ref", required=True, help="New package revision Y.")
  parser.add_argument(
    "--backend", choices=("postgres", "mysql", "mariadb", "sqlite"), default="postgres"
  )
  parser.add_argument("--django", default=">=6.0,<6.1", help="Django range for both revisions.")
  parser.add_argument("--database-host", default="127.0.0.1")
  parser.add_argument("--database-port", type=int)
  parser.add_argument("--database-user")
  parser.add_argument("--database-password")
  parser.add_argument("--result-dir")
  parser.add_argument(
    "--keep-database", action="store_true", help="Keep the isolated database for inspection."
  )
  args = parser.parse_args(argv)
  postgres = args.backend == "postgres"
  args.database_port = (
    args.database_port
    or {"postgres": 17432, "mysql": 17312, "mariadb": 17306, "sqlite": 0}[args.backend]
  )
  args.database_user = args.database_user or ("dj_queue" if postgres else "root")
  if args.database_password is None:
    args.database_password = "dj_queue" if postgres else "root"
  run_id = f"prerelease-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%f')}-{os.getpid()}"
  args.result_dir = Path(args.result_dir or PROJECT_ROOT / "benchmark-results" / run_id).resolve()
  args.database_name = (
    str(args.result_dir / "prerelease.sqlite3")
    if args.backend == "sqlite"
    else run_id.replace("-", "_").lower()
  )
  return args


def git_output(*args):
  return subprocess.run(
    ["git", *args],
    cwd=PROJECT_ROOT,
    check=True,
    capture_output=True,
    text=True,
  ).stdout.strip()


def resolve_revisions(from_ref, to_ref):
  revisions = [
    git_output("rev-parse", "--verify", f"{ref}^{{commit}}") for ref in (from_ref, to_ref)
  ]
  result = subprocess.run(
    ["git", "merge-base", "--is-ancestor", *revisions],
    cwd=PROJECT_ROOT,
    check=False,
  )
  if result.returncode != 0:
    raise ValueError(f"from revision {from_ref!r} is not an ancestor of {to_ref!r}")
  return revisions


def validate_rollout_compatibility(runtime_x, runtime_y):
  protocols = [runtime.get("rollout_protocol") for runtime in (runtime_x, runtime_y)]
  if any(type(protocol) is not int for protocol in protocols):
    raise ValueError("both revisions must publish a rollout protocol")
  if protocols[0] != protocols[1]:
    raise ValueError(f"incompatible rollout protocols: {protocols}")
  if runtime_x["django_version"] != runtime_y["django_version"]:
    raise ValueError("both revisions must use the same Django version")
  return {"x": runtime_x, "y": runtime_y}


def build_runtime(label, revision, *, django_range, backend, result_dir, work_dir):
  source_dir = Path(work_dir) / f"source-{label.lower()}"
  source_dir.mkdir()
  archive_path = Path(work_dir) / f"source-{label.lower()}.tar"
  with archive_path.open("wb") as archive:
    subprocess.run(["git", "archive", revision], cwd=PROJECT_ROOT, check=True, stdout=archive)
  with tarfile.open(archive_path) as archive:
    archive.extractall(source_dir, filter="data")
  artifact_dir = Path(result_dir) / "artifacts" / label.lower()
  artifact_dir.mkdir(parents=True)
  subprocess.run(
    ["uv", "build", "--no-sources", "--wheel", "--out-dir", str(artifact_dir)],
    cwd=source_dir,
    check=True,
  )
  (wheel,) = artifact_dir.glob("*.whl")
  venv = Path(work_dir) / f"venv-{label.lower()}"
  subprocess.run(["uv", "venv", "--python", sys.executable, str(venv)], check=True)
  python = venv / "bin" / "python"
  dependencies = [str(wheel), f"django{django_range}"]
  if backend == "postgres":
    dependencies.append("psycopg>=3.3.3")
  elif backend in {"mysql", "mariadb"}:
    dependencies.extend(["pymysql>=1.1.2", "cryptography>=46.0.6"])
  subprocess.run(["uv", "pip", "install", "--python", str(python), *dependencies], check=True)
  with wheel.open("rb") as artifact:
    digest = hashlib.file_digest(artifact, "sha256").hexdigest()
  return RevisionRuntime(label, revision, wheel, digest, python)


def runtime_env(args, label):
  env = {
    key: value for key, value in os.environ.items() if key not in {"PYTHONPATH", "PYTHONHOME"}
  }
  return {
    **env,
    "DJANGO_SETTINGS_MODULE": "prerelease_settings",
    "PRERELEASE_BACKEND": args.backend,
    "PRERELEASE_DB_NAME": args.database_name,
    "PRERELEASE_DB_HOST": args.database_host,
    "PRERELEASE_DB_PORT": str(args.database_port),
    "PRERELEASE_DB_USER": args.database_user,
    "PRERELEASE_DB_PASSWORD": args.database_password,
    "PRERELEASE_RUNTIME_LABEL": label,
  }


def run_runtime(runtime, args, *command, log_name):
  log_path = args.result_dir / "logs" / log_name
  log_path.parent.mkdir(parents=True, exist_ok=True)
  with log_path.open("w", encoding="utf-8") as log:
    result = subprocess.run(
      [str(runtime.python), str(RUNTIME_SCRIPT), *command],
      cwd=args.result_dir,
      env=runtime_env(args, runtime.label),
      stdout=log,
      stderr=subprocess.STDOUT,
      check=False,
      timeout=90,
    )
  if result.returncode != 0:
    raise RuntimeError(f"{command[0]} failed with {result.returncode}; see {log_path}")
  lines = [line for line in log_path.read_text().splitlines() if line.startswith("{")]
  return json.loads(lines[-1]) if lines else None


def start_runtime_process(runtime, args, name, command, processes):
  process = ManagedProcess(
    name,
    [str(runtime.python), str(RUNTIME_SCRIPT), command],
    env=runtime_env(args, runtime.label),
    log_path=args.result_dir / "logs" / f"{name}.log",
  )
  processes.append(process)
  return process


def wait_for(runtime, args, processes, name, ready):
  deadline = time.monotonic() + PHASE_TIMEOUT
  while time.monotonic() < deadline:
    for process in processes:
      process.assert_running()
    snapshot = run_runtime(runtime, args, "progress", log_name=f"{name}.log")
    for process in processes:
      process.assert_running()
    if ready(snapshot):
      return snapshot
    time.sleep(0.2)
  raise RuntimeError(f"{name} made insufficient progress; see logs/{name}.log")


def check_upgrade(runtime_x, runtime_y, args, processes, outcome):
  run_runtime(runtime_x, args, "migrate", log_name="migrate-x.log")
  run_runtime(runtime_x, args, "init", log_name="init.log")
  x_supervisor = start_runtime_process(runtime_x, args, "supervisor-x", "supervise", processes)

  def enqueue(producer, phase, queue):
    run_runtime(
      producer,
      args,
      "enqueue",
      "--phase",
      phase,
      "--queue",
      queue,
      log_name=f"enqueue-{phase}.log",
    )

  def wait_batch(observer, phase):
    snapshot = wait_for(
      observer,
      args,
      processes,
      phase,
      lambda value: not batch_problems(value, phase, PHASE_WORKERS[phase]),
    )
    outcome["phases"].append(phase)
    write_manifest(args.result_dir / "manifest.json", outcome)
    return snapshot

  enqueue(runtime_x, "old", "x")
  wait_batch(runtime_x, "old")
  writer = start_runtime_process(runtime_x, args, "producer-x", "produce", processes)
  before = wait_for(
    runtime_x, args, processes, "writer-started", lambda value: value["live_completed"] > 0
  )
  started = time.monotonic()
  run_runtime(runtime_y, args, "migrate", log_name="migrate-y.log")
  outcome["migration_seconds"] = time.monotonic() - started
  # sample after DDL so the next witness cannot come solely from pre-migration work
  after = run_runtime(runtime_x, args, "progress", log_name="after-migration.log")
  live_planned = sum(token.startswith("live:") for token in after["effects"])
  resumed = wait_for(
    runtime_x,
    args,
    processes,
    "writer-resumed",
    lambda value: value["live_completed"] > live_planned,
  )
  outcome["live_writer"] = {"before": before["live_completed"], "after": resumed["live_completed"]}

  enqueue(runtime_x, "old-to-new", "y")
  y_supervisor = start_runtime_process(runtime_y, args, "supervisor-y", "supervise", processes)
  enqueue(runtime_y, "new-to-old", "x")
  wait_batch(runtime_y, "old-to-new")
  wait_batch(runtime_y, "new-to-old")
  writer.stop()
  wait_for(runtime_y, args, processes, "mixed-drain", lambda value: value["depth"] == 0)
  x_supervisor.stop()
  enqueue(runtime_y, "new", "y")
  wait_batch(runtime_y, "new")
  wait_for(runtime_y, args, processes, "final-drain", lambda value: value["depth"] == 0)
  y_supervisor.stop()
  verification = run_runtime(runtime_y, args, "verify", log_name="verify.log")
  outcome["verification"] = verification
  outcome["problems"].extend(verification["problems"])
  for path in (args.result_dir / "logs").glob("*.log"):
    if "dj_queue infrastructure error" in path.read_text(errors="replace"):
      outcome["problems"].append(f"runtime infrastructure error in {path.name}")


def write_manifest(path, value):
  Path(path).write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n")


def run(args):
  args.result_dir.mkdir(parents=True, exist_ok=False)
  outcome = {
    "status": "running",
    "started_at": datetime.now(UTC).isoformat(),
    "backend": args.backend,
    "database_name": args.database_name,
    "phases": [],
    "problems": [],
    "database_cleanup": "not-created",
  }
  manifest_path = args.result_dir / "manifest.json"
  write_manifest(manifest_path, outcome)
  processes = []
  database_created = False
  with tempfile.TemporaryDirectory(prefix="dj-queue-prerelease-") as work_dir:
    try:
      from_revision, to_revision = resolve_revisions(args.from_ref, args.to_ref)
      runtimes = [
        build_runtime(
          label,
          revision,
          django_range=args.django,
          backend=args.backend,
          result_dir=args.result_dir,
          work_dir=work_dir,
        )
        for label, revision in (("X", from_revision), ("Y", to_revision))
      ]
      runtime_x, runtime_y = runtimes
      outcome["artifacts"] = [
        {
          "label": runtime.label,
          "revision": runtime.revision,
          "wheel": str(runtime.wheel.relative_to(args.result_dir)),
          "wheel_sha256": runtime.wheel_sha256,
        }
        for runtime in runtimes
      ]
      outcome["compatibility"] = validate_rollout_compatibility(
        *[
          run_runtime(
            runtime, args, "compatibility", log_name=f"compatibility-{runtime.label}.log"
          )
          for runtime in runtimes
        ]
      )
      write_manifest(manifest_path, outcome)
      run_runtime(runtime_x, args, "create-database", log_name="create-database.log")
      database_created = True
      check_upgrade(runtime_x, runtime_y, args, processes, outcome)
    except Exception as error:  # noqa: BLE001
      outcome["problems"].append(str(error))
    finally:
      for process in reversed(processes):
        try:
          process.stop()
        except Exception as error:  # noqa: BLE001
          outcome["problems"].append(str(error))
      if database_created:
        outcome["database_cleanup"] = "retained"
        if not args.keep_database:
          try:
            run_runtime(runtime_x, args, "drop-database", log_name="drop-database.log")
            outcome["database_cleanup"] = "dropped"
          except Exception as error:  # noqa: BLE001
            outcome["problems"].append(str(error))
            outcome["database_cleanup"] = "failed"
  outcome["status"] = "failed" if outcome["problems"] else "passed"
  outcome["finished_at"] = datetime.now(UTC).isoformat()
  write_manifest(manifest_path, outcome)
  print(json.dumps(outcome, indent=2, sort_keys=True))
  return int(bool(outcome["problems"]))


def main(argv):
  try:
    return run(parse_args(argv))
  except (OSError, ValueError) as error:
    print(error, file=sys.stderr)
    return 2


if __name__ == "__main__":
  raise SystemExit(main(sys.argv[1:]))
