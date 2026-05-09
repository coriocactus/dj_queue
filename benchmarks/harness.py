import json
import os
import platform
import statistics
import subprocess
import sys
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from time import perf_counter

import django
from django.conf import settings
from django.core.management import call_command
from django.db import connection, connections

from dj_queue.runtime.connection_budget import (
  estimate_persistent_worker_connections,
  persistent_connections_enabled,
  postgres_connection_capacity,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOCAL_RESULTS_DIR = PROJECT_ROOT / "benchmark-results"


class Timer:
  def __enter__(self):
    self.started_at = perf_counter()
    return self

  def __exit__(self, exc_type, exc, tb):
    self.duration = perf_counter() - self.started_at
    return False


class ResultWriter:
  def __init__(self, path):
    self.path = Path(path)
    self.path.parent.mkdir(parents=True, exist_ok=True)

  def write(self, result):
    line = json.dumps(result, sort_keys=True, default=str)
    with self.path.open("a", encoding="utf-8") as handle:
      handle.write(f"{line}\n")
    print(line)


def default_output_path(backend):
  timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
  return LOCAL_RESULTS_DIR / f"{backend}-{timestamp}.jsonl"


def parse_sizes(value, *, default):
  if value in (None, ""):
    return default
  sizes = [int(item.strip()) for item in value.split(",") if item.strip()]
  if not sizes or any(size <= 0 for size in sizes):
    raise ValueError("sizes must be positive integers")
  return sizes


def percentile(values, percent):
  if not values:
    return None
  ordered = sorted(values)
  index = round((len(ordered) - 1) * (percent / 100))
  return ordered[index]


def latency_summary(seconds_values):
  if not seconds_values:
    return {}
  return {
    "latency_p50_ms": percentile(seconds_values, 50) * 1000,
    "latency_p95_ms": percentile(seconds_values, 95) * 1000,
    "latency_p99_ms": percentile(seconds_values, 99) * 1000,
    "latency_mean_ms": statistics.fmean(seconds_values) * 1000,
  }


def throughput(count, duration):
  if duration <= 0:
    return None
  return count / duration


def ensure_database_exists(backend):
  if backend == "postgres":
    ensure_postgres_database_exists()
  elif backend in {"mysql", "mariadb"}:
    ensure_mysql_family_database_exists(backend)


def ensure_postgres_database_exists():

  db_name = os.environ.get("BENCHMARK_DB_NAME", "dj_queue_benchmark")
  if "benchmark" not in db_name.lower():
    raise RuntimeError("refusing to create a PostgreSQL database without 'benchmark' in its name")

  try:
    import psycopg
  except ImportError as exc:
    raise RuntimeError("creating PostgreSQL benchmark databases requires psycopg") from exc

  conninfo = {
    "dbname": os.environ.get("BENCHMARK_MAINTENANCE_DB", "postgres"),
    "user": os.environ.get("BENCHMARK_DB_USER", "dj_queue"),
    "password": os.environ.get("BENCHMARK_DB_PASSWORD", "dj_queue"),
    "host": os.environ.get("BENCHMARK_DB_HOST", "127.0.0.1"),
    "port": os.environ.get("BENCHMARK_DB_PORT", "17432"),
    "autocommit": True,
  }
  with psycopg.connect(**conninfo) as conn:
    with conn.cursor() as cursor:
      cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", [db_name])
      if cursor.fetchone() is None:
        cursor.execute(
          psycopg.sql.SQL("CREATE DATABASE {}").format(psycopg.sql.Identifier(db_name))
        )


def ensure_mysql_family_database_exists(backend):
  db_name = os.environ.get("BENCHMARK_DB_NAME", "dj_queue_benchmark")
  if "benchmark" not in db_name.lower():
    raise RuntimeError(f"refusing to create a {backend} database without 'benchmark' in its name")

  try:
    import pymysql
  except ImportError as exc:
    raise RuntimeError("creating MySQL-family benchmark databases requires pymysql") from exc

  default_port = "17306" if backend == "mariadb" else "17312"
  conninfo = {
    "user": os.environ.get("BENCHMARK_DB_USER", "root"),
    "password": os.environ.get("BENCHMARK_DB_PASSWORD", "root"),
    "host": os.environ.get("BENCHMARK_DB_HOST", "127.0.0.1"),
    "port": int(os.environ.get("BENCHMARK_DB_PORT", default_port)),
    "database": os.environ.get("BENCHMARK_MAINTENANCE_DB", "mysql"),
    "autocommit": True,
  }
  with pymysql.connect(**conninfo) as conn:
    with conn.cursor() as cursor:
      cursor.execute(
        f"CREATE DATABASE IF NOT EXISTS `{mysql_identifier(db_name)}` "
        "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
      )


def mysql_identifier(value):
  return value.replace("`", "``")


def prepare_database(*, migrate=True):
  assert_benchmark_database()
  ensure_database_path()
  if migrate:
    call_command("migrate", verbosity=0, interactive=False)


def preflight_persistent_connection_budget(*, backend):
  if backend != "postgres":
    return None
  conn_max_age = settings.DATABASES["default"].get("CONN_MAX_AGE", 0)
  if not persistent_connections_enabled(conn_max_age):
    return None

  options = settings.TASKS["default"]["OPTIONS"]
  workers = options["workers"]
  worker_processes = sum(worker.get("processes", 1) for worker in workers)
  worker_threads = sum(worker.get("processes", 1) * worker["threads"] for worker in workers)
  estimated_connections = estimate_persistent_worker_connections(
    worker_processes=worker_processes,
    worker_threads=worker_threads,
  )
  capacity = postgres_connection_capacity("default")
  if capacity is None:
    return None
  assert_persistent_connection_budget(
    estimated_connections=estimated_connections,
    available_connections=capacity.available_connections,
  )
  return {
    "estimated_connections": estimated_connections,
    "available_connections": capacity.available_connections,
  }


def assert_persistent_connection_budget(*, estimated_connections, available_connections):
  if estimated_connections < available_connections:
    return None
  raise RuntimeError(
    "benchmark persistent connection preflight failed: "
    f"estimated {estimated_connections} worker connections but only "
    f"{available_connections} PostgreSQL connections are available"
  )


def reset_database():
  assert_benchmark_database()
  call_command("flush", verbosity=0, interactive=False)


def assert_benchmark_database():
  db_name = str(connections["default"].settings_dict.get("NAME", ""))
  if "benchmark" not in db_name.lower():
    raise RuntimeError(
      f"refusing to reset database {db_name!r}; benchmark database names must contain 'benchmark'"
    )


def ensure_database_path():
  if connections["default"].settings_dict.get("ENGINE") != "django.db.backends.sqlite3":
    return
  db_name = str(connections["default"].settings_dict.get("NAME", ""))
  if db_name not in ("", ":memory:"):
    Path(db_name).parent.mkdir(parents=True, exist_ok=True)


def database_info():
  connection.ensure_connection()
  vendor = connection.vendor
  with connection.cursor() as cursor:
    if vendor == "sqlite":
      cursor.execute("select sqlite_version()")
    else:
      cursor.execute("select version()")
    database_version = cursor.fetchone()[0]
  return {
    "alias": connection.alias,
    "vendor": vendor,
    "name": str(connection.settings_dict.get("NAME", "")),
    "version": database_version,
  }


def environment_metadata(*, backend):
  return {
    "backend": backend,
    "database": database_info(),
    "benchmark": benchmark_settings_info(),
    "python": sys.version.split()[0],
    "django": django.get_version(),
    "dj_queue": package_version("dj-queue"),
    "platform": platform.platform(),
    "processor": platform.processor(),
    "machine": platform.machine(),
    "git_revision": revision(),
  }


def benchmark_settings_info():
  options = settings.TASKS["default"]["OPTIONS"]
  workers = options["workers"]
  worker_threads = sorted({worker["threads"] for worker in workers})
  return {
    "worker_count": len(workers),
    "worker_threads": worker_threads[0] if len(worker_threads) == 1 else worker_threads,
    "preserve_finished_jobs": options["preserve_finished_jobs"],
    "conn_max_age": settings.DATABASES["default"].get("CONN_MAX_AGE", 0),
  }


def package_version(package_name):
  try:
    return version(package_name)
  except PackageNotFoundError:
    return None


def revision():
  for command in (
    ["jj", "log", "-r", "@-", "--no-graph", "-T", "commit_id.short()"],
    ["git", "rev-parse", "--short", "HEAD"],
  ):
    try:
      result = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        capture_output=True,
        check=True,
        text=True,
      )
    except (OSError, subprocess.CalledProcessError):
      continue
    value = result.stdout.strip()
    if value:
      return value
  return None


def benchmark_result(*, scenario, size, run_index, metrics, metadata):
  return {
    "schema_version": 1,
    "recorded_at": datetime.now(UTC).isoformat(),
    "scenario": scenario,
    "size": size,
    "run_index": run_index,
    "metrics": metrics,
    "metadata": metadata,
  }
