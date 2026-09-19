import argparse
import json
import logging
import os
import signal
import sys
import time
from contextlib import contextmanager
from datetime import timedelta
from importlib.metadata import version
from pathlib import Path
from threading import Event

LOGGER = logging.getLogger("dj_queue.prerelease")
PHASE_WORKERS = {"old": "X", "old-to-new": "Y", "new-to-old": "X", "new": "Y"}
BATCH_NAMES = (
  "immediate",
  "scheduled",
  "limited-0",
  "limited-1",
  "bulk-0",
  "bulk-1",
  "retry",
  "recurring",
)


def assert_prerelease_database_name():
  name = os.environ["PRERELEASE_DB_NAME"]
  if "prerelease" not in name.lower():
    raise RuntimeError("pre-release database names must contain 'prerelease'")
  return name


@contextmanager
def maintenance_cursor():
  name = assert_prerelease_database_name()
  options = {
    "user": os.environ["PRERELEASE_DB_USER"],
    "password": os.environ["PRERELEASE_DB_PASSWORD"],
    "host": os.environ["PRERELEASE_DB_HOST"],
    "port": int(os.environ["PRERELEASE_DB_PORT"]),
    "autocommit": True,
  }
  if os.environ["PRERELEASE_BACKEND"] == "postgres":
    import psycopg
    from psycopg import sql

    connection = psycopg.connect(dbname="postgres", **options)
    quoted_name = sql.Identifier(name).as_string(connection)
  else:
    import pymysql

    connection = pymysql.connect(database="mysql", **options)
    quoted_name = f"`{name.replace('`', '``')}`"
  try:
    with connection.cursor() as cursor:
      yield cursor, quoted_name
  finally:
    connection.close()


def create_database():
  name = assert_prerelease_database_name()
  if os.environ["PRERELEASE_BACKEND"] == "sqlite":
    path = Path(name)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=False)
    return
  with maintenance_cursor() as (cursor, quoted_name):
    suffix = ""
    if os.environ["PRERELEASE_BACKEND"] in {"mysql", "mariadb"}:
      suffix = " CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
    cursor.execute(f"CREATE DATABASE {quoted_name}{suffix}")


def drop_database():
  name = assert_prerelease_database_name()
  if os.environ["PRERELEASE_BACKEND"] == "sqlite":
    Path(name).unlink()
    return
  with maintenance_cursor() as (cursor, quoted_name):
    cursor.execute(f"DROP DATABASE {quoted_name}")


def migrate():
  from django.core.management import call_command
  from django.db import connection, connections
  from django.db.utils import OperationalError

  from dj_queue.db import is_transient_database_error

  deadline = time.monotonic() + 60
  while True:
    try:
      _set_migration_lock_timeout(connection)
      call_command("migrate", verbosity=1, interactive=False)
      return
    except OperationalError as error:
      if not is_transient_database_error(error) or time.monotonic() >= deadline:
        raise
      LOGGER.warning("migration lock conflict; retrying")
      connections.close_all()
      time.sleep(0.25)


def _set_migration_lock_timeout(connection):
  with connection.cursor() as cursor:
    if connection.vendor == "postgresql":
      cursor.execute("SET SESSION lock_timeout = '2s'")
    elif connection.vendor == "mysql":
      cursor.execute("SET SESSION lock_wait_timeout = 2")
      cursor.execute("SET SESSION innodb_lock_wait_timeout = 2")


def create_control_tables():
  from django.db import connection

  with connection.cursor() as cursor:
    cursor.execute(
      "CREATE TABLE dj_queue_prerelease_effects ("
      "token varchar(255) PRIMARY KEY, attempts integer NOT NULL DEFAULT 0, "
      "completions integer NOT NULL DEFAULT 0, worker varchar(1) NOT NULL DEFAULT '')"
    )


def expect_tokens(tokens):
  from django.db import connection

  with connection.cursor() as cursor:
    cursor.executemany(
      "INSERT INTO dj_queue_prerelease_effects (token) VALUES (%s)",
      [(token,) for token in tokens],
    )


def supervise():
  from django.core.management import call_command

  call_command("dj_queue", mode="async")


def enqueue_batch(phase, queue):
  from django.utils import timezone
  from prerelease_tasks import fail_once, record, record_limited

  from dj_queue.models import RecurringTask

  expect_tokens(f"{phase}:{name}" for name in BATCH_NAMES)
  record.using(queue_name=queue).enqueue(f"{phase}:immediate")
  record.using(queue_name=queue, run_after=timezone.now() + timedelta(seconds=1)).enqueue(
    f"{phase}:scheduled"
  )
  for index in range(2):
    record_limited.using(queue_name=queue).enqueue(queue, f"{phase}:limited-{index}")
  record.get_backend().enqueue_all(
    [(record.using(queue_name=queue), (f"{phase}:bulk-{index}",), {}) for index in range(2)]
  )
  fail_once.using(queue_name=queue).enqueue(f"{phase}:retry")
  # one due annual slot gives the scheduler work without an open-ended schedule
  RecurringTask.objects.create(
    backend_alias="default",
    key=f"{phase}:recurring",
    task_path="prerelease_tasks.record",
    payload={"args": [f"{phase}:recurring"], "kwargs": {}},
    schedule="0 0 1 1 *",
    queue_name=queue,
    static=False,
  )


def produce():
  from prerelease_tasks import record

  stop = Event()
  signal.signal(signal.SIGTERM, lambda *_args: stop.set())
  signal.signal(signal.SIGINT, lambda *_args: stop.set())
  index = 0
  while not stop.is_set():
    token = f"live:{index}"
    expect_tokens([token])
    record.using(queue_name="shared").enqueue(token)
    index += 1
    stop.wait(0.05)


def retry_expected_failures():
  from dj_queue.models import FailedExecution
  from dj_queue.operations.jobs import retry_failed_jobs

  job_ids = list(
    FailedExecution.objects.filter(job__task_path="prerelease_tasks.fail_once")
    .order_by("id")
    .values_list("job_id", flat=True)[:100]
  )
  if job_ids:
    retry_failed_jobs(job_ids=job_ids, batch_size=len(job_ids))


def status():
  from django.db import connection

  from dj_queue.models import (
    BlockedExecution,
    ClaimedExecution,
    FailedExecution,
    ReadyExecution,
    RecurringExecution,
    ScheduledExecution,
  )

  with connection.cursor() as cursor:
    cursor.execute("SELECT token, attempts, completions, worker FROM dj_queue_prerelease_effects")
    effects = {
      token: [attempts, completions, worker] for token, attempts, completions, worker in cursor
    }
  return {
    "effects": effects,
    "depth": sum(
      model.objects.count()
      for model in (
        ReadyExecution,
        ScheduledExecution,
        ClaimedExecution,
        BlockedExecution,
        FailedExecution,
      )
    ),
    "recurring": {
      key: str(job_id) if job_id else None
      for key, job_id in RecurringExecution.objects.values_list("task_key", "job_id")
    },
    "live_completed": sum(
      token.startswith("live:") and row[1] == 1 for token, row in effects.items()
    ),
  }


def batch_problems(snapshot, phase, worker):
  problems = []
  for name in BATCH_NAMES:
    token = f"{phase}:{name}"
    expected = [2 if name == "retry" else 1, 1, worker]
    actual = snapshot["effects"].get(token)
    if actual != expected:
      problems.append(f"{token}: expected {expected}, found {actual}")
  if not snapshot["recurring"].get(f"{phase}:recurring"):
    problems.append(f"{phase}:recurring reservation has no job")
  return problems


def verify():
  from django.db import connection

  from dj_queue import observability
  from dj_queue.models import Job, Process, RecurringExecution

  snapshot = status()
  problems = list(observability.deep_health_problems(backend_alias="default"))
  for phase, worker in PHASE_WORKERS.items():
    problems.extend(batch_problems(snapshot, phase, worker))
  for token, (attempts, completions, worker) in snapshot["effects"].items():
    if attempts != (2 if token.endswith(":retry") else 1) or completions != 1:
      problems.append(
        f"incorrect side effect: {token} ({attempts} attempts, {completions} completions)"
      )
  if not snapshot["live_completed"]:
    problems.append("old producer made no live progress")
  if snapshot["depth"] or Job.objects.filter(finished_at__isnull=True).exists():
    problems.append("queue did not drain")
  if Process.objects.exists():
    problems.append("runtime process rows remain after shutdown")
  if RecurringExecution.objects.count() != len(PHASE_WORKERS):
    problems.append("expected one recurring reservation per phase")
  with connection.cursor() as cursor:
    cursor.execute(
      "SELECT sqlite_version()" if connection.vendor == "sqlite" else "SELECT version()"
    )
    database_version = cursor.fetchone()[0]
  return {"problems": problems, "status": snapshot, "database_version": database_version}


def main(argv):
  parser = argparse.ArgumentParser(description="Run one isolated upgrade-check process.")
  parser.add_argument(
    "command",
    choices=(
      "create-database",
      "drop-database",
      "migrate",
      "compatibility",
      "init",
      "supervise",
      "enqueue",
      "produce",
      "progress",
      "verify",
    ),
  )
  parser.add_argument("--phase", choices=tuple(PHASE_WORKERS))
  parser.add_argument("--queue", choices=("x", "y"))
  args = parser.parse_args(argv)
  logging.basicConfig(level=logging.WARNING)
  if args.command == "create-database":
    create_database()
    return 0
  if args.command == "drop-database":
    drop_database()
    return 0

  os.environ.setdefault("DJANGO_SETTINGS_MODULE", "prerelease_settings")
  import django

  django.setup()
  result = None
  if args.command == "compatibility":
    from dj_queue.runtime.base import ROLLOUT_PROTOCOL_VERSION

    result = {
      "dj_queue_version": version("dj-queue"),
      "django_version": django.get_version(),
      "rollout_protocol": ROLLOUT_PROTOCOL_VERSION,
    }
  elif args.command == "migrate":
    migrate()
  elif args.command == "init":
    create_control_tables()
  elif args.command == "supervise":
    supervise()
  elif args.command == "enqueue":
    if args.phase is None or args.queue is None:
      parser.error("enqueue requires --phase and --queue")
    enqueue_batch(args.phase, args.queue)
  elif args.command == "produce":
    produce()
  elif args.command == "progress":
    retry_expected_failures()
    result = status()
  elif args.command == "verify":
    result = verify()
  if result is not None:
    print(json.dumps(result, sort_keys=True), flush=True)
  return 0


if __name__ == "__main__":
  raise SystemExit(main(sys.argv[1:]))
