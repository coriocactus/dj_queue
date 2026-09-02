import argparse
import json
import logging
import os
import signal
import sys
import time
from datetime import timedelta
from pathlib import Path
from threading import Event

LOGGER = logging.getLogger("dj_queue.prerelease")
STOP = Event()
TASK_PATHS = (
  "prerelease_tasks.record",
  "prerelease_tasks.record_limited",
  "prerelease_tasks.fail_once",
  "prerelease_tasks.record_recurring",
)


def parse_args(argv):
  parser = argparse.ArgumentParser(description="Run one isolated pre-release load process.")
  subparsers = parser.add_subparsers(dest="command", required=True)

  subparsers.add_parser("create-database")
  subparsers.add_parser("drop-database")
  subparsers.add_parser("migrate")

  seed = subparsers.add_parser("seed")
  seed.add_argument("--jobs", type=int, required=True)
  seed.add_argument("--queues", type=int, required=True)
  seed.add_argument("--semaphores", type=int, required=True)
  seed.add_argument("--recurring-executions", type=int, required=True)

  supervise = subparsers.add_parser("supervise")
  supervise.add_argument("--duration", type=float)

  calibrate = subparsers.add_parser("calibrate")
  calibrate.add_argument("--run-id", required=True)
  calibrate.add_argument("--jobs", type=int, required=True)
  calibrate.add_argument("--timeout", type=float, default=180)

  produce = subparsers.add_parser("produce")
  produce.add_argument("--run-id", required=True)
  produce.add_argument("--rate", type=float, required=True)
  produce.add_argument("--duration", type=float)

  retry = subparsers.add_parser("retry")
  retry.add_argument("--interval", type=float, default=0.1)
  retry.add_argument("--duration", type=float)

  recurring = subparsers.add_parser("stop-recurring")
  recurring.add_argument("--run-id", required=True)

  status = subparsers.add_parser("status")
  status.add_argument("--run-id", required=True)

  verify = subparsers.add_parser("verify")
  verify.add_argument("--run-id", required=True)

  return parser.parse_args(argv)


def configure_logging():
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
  )
  logging.getLogger("dj_queue").setLevel(logging.WARNING)
  LOGGER.setLevel(logging.INFO)


def install_signal_handlers():
  def request_stop(_signum, _frame):
    STOP.set()

  signal.signal(signal.SIGINT, request_stop)
  signal.signal(signal.SIGTERM, request_stop)


def setup_django():
  os.environ.setdefault("DJANGO_SETTINGS_MODULE", "prerelease_settings")
  import django

  django.setup()


def assert_prerelease_database_name():
  name = os.environ["PRERELEASE_DB_NAME"]
  if "prerelease" not in name.lower():
    raise RuntimeError("pre-release database names must contain 'prerelease'")
  return name


def create_database():
  name = assert_prerelease_database_name()
  backend = os.environ["PRERELEASE_BACKEND"]
  if backend == "sqlite":
    path = Path(name)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.unlink(missing_ok=True)
    return
  if backend == "postgres":
    import psycopg
    from psycopg import sql

    with (
      psycopg.connect(
        dbname=os.environ.get("PRERELEASE_MAINTENANCE_DB", "postgres"),
        user=os.environ.get("PRERELEASE_DB_USER", "dj_queue"),
        password=os.environ.get("PRERELEASE_DB_PASSWORD", "dj_queue"),
        host=os.environ.get("PRERELEASE_DB_HOST", "127.0.0.1"),
        port=os.environ.get("PRERELEASE_DB_PORT", "5432"),
        autocommit=True,
      ) as connection,
      connection.cursor() as cursor,
    ):
      cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", [name])
      if cursor.fetchone() is None:
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    return

  import pymysql

  with (
    pymysql.connect(
      user=os.environ.get("PRERELEASE_DB_USER", "root"),
      password=os.environ.get("PRERELEASE_DB_PASSWORD", "root"),
      host=os.environ.get("PRERELEASE_DB_HOST", "127.0.0.1"),
      port=int(os.environ.get("PRERELEASE_DB_PORT", "3306")),
      database=os.environ.get("PRERELEASE_MAINTENANCE_DB", "mysql"),
      autocommit=True,
    ) as connection,
    connection.cursor() as cursor,
  ):
    cursor.execute(
      f"CREATE DATABASE `{name.replace('`', '``')}` "
      "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
    )


def drop_database():
  name = assert_prerelease_database_name()
  backend = os.environ["PRERELEASE_BACKEND"]
  if backend == "sqlite":
    Path(name).unlink(missing_ok=True)
    return
  if backend == "postgres":
    import psycopg
    from psycopg import sql

    with (
      psycopg.connect(
        dbname=os.environ.get("PRERELEASE_MAINTENANCE_DB", "postgres"),
        user=os.environ.get("PRERELEASE_DB_USER", "dj_queue"),
        password=os.environ.get("PRERELEASE_DB_PASSWORD", "dj_queue"),
        host=os.environ.get("PRERELEASE_DB_HOST", "127.0.0.1"),
        port=os.environ.get("PRERELEASE_DB_PORT", "5432"),
        autocommit=True,
      ) as connection,
      connection.cursor() as cursor,
    ):
      cursor.execute(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
        "WHERE datname = %s AND pid <> pg_backend_pid()",
        [name],
      )
      cursor.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(name)))
    return

  import pymysql

  with (
    pymysql.connect(
      user=os.environ.get("PRERELEASE_DB_USER", "root"),
      password=os.environ.get("PRERELEASE_DB_PASSWORD", "root"),
      host=os.environ.get("PRERELEASE_DB_HOST", "127.0.0.1"),
      port=int(os.environ.get("PRERELEASE_DB_PORT", "3306")),
      database=os.environ.get("PRERELEASE_MAINTENANCE_DB", "mysql"),
      autocommit=True,
    ) as connection,
    connection.cursor() as cursor,
  ):
    cursor.execute(f"DROP DATABASE IF EXISTS `{name.replace('`', '``')}`")


def migrate():
  from django.core.management import call_command

  call_command("migrate", verbosity=1, interactive=False)


def create_control_tables():
  from django.db import connection

  accepted = connection.ops.quote_name("dj_queue_prerelease_accepted")
  effects = connection.ops.quote_name("dj_queue_prerelease_effects")
  if connection.vendor == "mysql":
    token_type = "varchar(255)"
    category_type = "varchar(32)"
    label_type = "varchar(16)"
    timestamp_type = "datetime(6)"
    float_type = "double"
  else:
    token_type = "varchar(255)"
    category_type = "varchar(32)"
    label_type = "varchar(16)"
    timestamp_type = "timestamp"
    float_type = "double precision" if connection.vendor == "postgresql" else "real"

  with connection.cursor() as cursor:
    cursor.execute(
      f"""
      CREATE TABLE {accepted} (
        token {token_type} PRIMARY KEY,
        category {category_type} NOT NULL,
        producer_version {label_type} NOT NULL,
        enqueue_ms {float_type} NOT NULL,
        accepted_at {timestamp_type} NOT NULL
      )
      """
    )
    cursor.execute(
      f"""
      CREATE TABLE {effects} (
        token {token_type} PRIMARY KEY,
        category {category_type} NOT NULL,
        attempts integer NOT NULL,
        completions integer NOT NULL,
        first_version {label_type} NOT NULL,
        last_version {label_type} NOT NULL,
        completed_at {timestamp_type} NOT NULL
      )
      """
    )


def seed_database(*, jobs, queues, semaphores, recurring_executions):
  from django.utils import timezone

  from dj_queue.models import Job, RecurringExecution, Semaphore

  if recurring_executions > jobs:
    raise ValueError("recurring execution seed count cannot exceed job seed count")
  create_control_tables()
  now = timezone.now()
  expires_at = now + timedelta(days=1)
  batch_size = 1000
  seeded_jobs = []
  for offset in range(0, jobs, batch_size):
    batch = [
      Job(
        task_path="prerelease_tasks.baseline",
        queue_name=f"queue-{index % queues:03d}",
        priority=0,
        payload={"args": [index], "kwargs": {}},
        backend_alias="default",
        finished_at=now,
        return_value=index,
        created_at=now,
        updated_at=now,
      )
      for index in range(offset, min(offset + batch_size, jobs))
    ]
    Job.objects.bulk_create(batch, batch_size=batch_size)
    seeded_jobs.extend(batch)

  for offset in range(0, recurring_executions, batch_size):
    RecurringExecution.objects.bulk_create(
      [
        RecurringExecution(
          backend_alias="default",
          task_key=f"baseline-{index}",
          run_at=now,
          job=seeded_jobs[index],
        )
        for index in range(offset, min(offset + batch_size, recurring_executions))
      ],
      batch_size=batch_size,
    )

  Semaphore.objects.bulk_create(
    [
      Semaphore(
        key=f"prerelease:{index}",
        value=2,
        limit=2,
        expires_at=expires_at,
      )
      for index in range(semaphores)
    ],
    batch_size=batch_size,
  )
  print_json(
    {
      "seeded_jobs": jobs,
      "seeded_queues": queues,
      "seeded_semaphores": semaphores,
      "seeded_recurring_executions": recurring_executions,
    }
  )


def supervise(*, duration=None):
  from dj_queue.runtime.supervisor import AsyncSupervisor

  install_signal_handlers()
  deadline = time.monotonic() + duration if duration is not None else None
  supervisor = AsyncSupervisor.from_backend_config(backend_alias="default", standalone=False)
  supervisor.start()
  LOGGER.info("supervisor started runners=%s", len(supervisor.runners))
  try:
    while not STOP.wait(0.2):
      if deadline is not None and time.monotonic() >= deadline:
        break
  finally:
    supervisor.stop()
    LOGGER.info("supervisor stopped")


def calibrate(*, run_id, jobs, timeout):
  from django.db import connection
  from prerelease_tasks import record

  started = time.monotonic()
  backend = record.get_backend()
  for offset in range(0, jobs, 500):
    backend.enqueue_all(
      [
        (record, (f"{run_id}:calibration:{index}", "calibration"), {})
        for index in range(offset, min(offset + 500, jobs))
      ]
    )

  table = connection.ops.quote_name("dj_queue_prerelease_effects")
  deadline = started + timeout
  completed = 0
  while time.monotonic() < deadline:
    with connection.cursor() as cursor:
      cursor.execute(
        f"SELECT COUNT(*) FROM {table} WHERE token LIKE %s AND completions = 1",
        [f"{run_id}:calibration:%"],
      )
      completed = cursor.fetchone()[0]
    if completed == jobs:
      break
    time.sleep(0.05)
  duration = time.monotonic() - started
  if completed != jobs:
    raise RuntimeError(f"calibration drained {completed}/{jobs} jobs before timeout")
  print_json(
    {
      "jobs": jobs,
      "duration_seconds": duration,
      "capacity_jobs_per_second": jobs / duration,
    }
  )


def produce(*, run_id, rate, duration=None):
  from django.db import connection
  from django.utils import timezone
  from prerelease_tasks import fail_once, record, record_limited

  install_signal_handlers()
  runtime_label = os.environ.get("PRERELEASE_RUNTIME_LABEL", "unknown")
  recurring_count = min(500, max(1, round(rate * 0.05)))
  ensure_recurring_tasks(run_id, recurring_count)
  direct_rate = max(1.0, rate - recurring_count)
  pattern = (
    ["immediate"] * 45
    + ["concurrency"] * 20
    + ["scheduled"] * 15
    + ["bulk"] * 10
    + ["failure"] * 5
  )
  budget = 0.0
  sequence = 0
  last_tick = time.monotonic()
  deadline = last_tick + duration if duration is not None else None
  accepted_table = connection.ops.quote_name("dj_queue_prerelease_accepted")
  LOGGER.info(
    "producer started rate=%.2f direct_rate=%.2f recurring_tasks=%s",
    rate,
    direct_rate,
    recurring_count,
  )

  while not STOP.wait(0.01):
    now = time.monotonic()
    if deadline is not None and now >= deadline:
      break
    budget += direct_rate * (now - last_tick)
    last_tick = now
    count = min(int(budget), 100)
    if count <= 0:
      continue
    budget -= count

    bulk = []
    for _index in range(count):
      category = pattern[sequence % len(pattern)]
      token = f"{run_id}:{category}:{runtime_label}:{sequence}"
      queue_name = f"queue-{sequence % 100:03d}"
      started = time.perf_counter()
      if category == "immediate":
        record.using(queue_name=queue_name).enqueue(token, category)
        record_accepted(
          accepted_table,
          token,
          category,
          runtime_label,
          started,
          accepted_at=timezone.now(),
        )
      elif category == "concurrency":
        record_limited.using(queue_name=queue_name).enqueue(sequence % 1000, token)
        record_accepted(
          accepted_table,
          token,
          category,
          runtime_label,
          started,
          accepted_at=timezone.now(),
        )
      elif category == "scheduled":
        record.using(
          queue_name=queue_name,
          run_after=timezone.now() + timedelta(seconds=1),
        ).enqueue(token, category)
        record_accepted(
          accepted_table,
          token,
          category,
          runtime_label,
          started,
          accepted_at=timezone.now(),
        )
      elif category == "failure":
        fail_once.using(queue_name=queue_name).enqueue(token)
        record_accepted(
          accepted_table,
          token,
          category,
          runtime_label,
          started,
          accepted_at=timezone.now(),
        )
      else:
        bulk.append((token, queue_name, started))
      sequence += 1

    if bulk:
      bulk_started = time.perf_counter()
      record.get_backend().enqueue_all(
        [
          (record.using(queue_name=queue_name), (token, "bulk"), {})
          for token, queue_name, _started in bulk
        ]
      )
      accepted_at = timezone.now()
      for token, _queue_name, _started in bulk:
        record_accepted(
          accepted_table,
          token,
          "bulk",
          runtime_label,
          bulk_started,
          accepted_at=accepted_at,
        )

  LOGGER.info("producer stopped accepted_sequence=%s", sequence)


def record_accepted(table, token, category, runtime_label, started, *, accepted_at):
  from django.db import connection

  enqueue_ms = (time.perf_counter() - started) * 1000
  with connection.cursor() as cursor:
    cursor.execute(
      f"""
      INSERT INTO {table} (
        token, category, producer_version, enqueue_ms, accepted_at
      ) VALUES (%s, %s, %s, %s, %s)
      """,
      [token, category, runtime_label, enqueue_ms, accepted_at],
    )


def ensure_recurring_tasks(run_id, count):
  from django.utils import timezone

  from dj_queue.models import RecurringTask

  now = timezone.now()
  for index in range(count):
    RecurringTask.objects.update_or_create(
      backend_alias="default",
      key=f"{run_id}:recurring:{index}",
      defaults={
        "task_path": "prerelease_tasks.record_recurring",
        "payload": {"args": [run_id], "kwargs": {}},
        "schedule": "* * * * * *",
        "queue_name": f"queue-{index % 100:03d}",
        "priority": 0,
        "description": "pre-release mixed-version load",
        "static": False,
        "next_run_at": now + timedelta(seconds=1),
      },
    )


def retry_expected_failures(*, interval, duration=None):
  from dj_queue.models import FailedExecution
  from dj_queue.operations.jobs import retry_failed_jobs

  install_signal_handlers()
  deadline = time.monotonic() + duration if duration is not None else None
  while not STOP.wait(interval):
    if deadline is not None and time.monotonic() >= deadline:
      break
    job_ids = list(
      FailedExecution.objects.filter(job__task_path="prerelease_tasks.fail_once")
      .order_by("id")
      .values_list("job_id", flat=True)[:500]
    )
    if job_ids:
      retry_failed_jobs(job_ids=job_ids, batch_size=len(job_ids))


def stop_recurring(run_id):
  from dj_queue.models import RecurringTask

  deleted, _details = RecurringTask.objects.filter(
    backend_alias="default",
    key__startswith=f"{run_id}:recurring:",
  ).delete()
  print_json({"deleted_recurring_tasks": deleted})


def verify(run_id):
  from django.db import connection
  from django.db.models import F, Q

  from dj_queue import observability
  from dj_queue.models import FailedExecution, Job, RecurringExecution

  problems = list(observability.deep_health_problems(backend_alias="default"))
  status = queue_status_data(run_id)
  if status["depth"]:
    problems.append(f"queue did not drain: {status}")

  accepted = connection.ops.quote_name("dj_queue_prerelease_accepted")
  effects = connection.ops.quote_name("dj_queue_prerelease_effects")
  with connection.cursor() as cursor:
    cursor.execute(
      f"""
      SELECT COUNT(*)
      FROM {accepted} accepted
      LEFT JOIN {effects} effects ON effects.token = accepted.token
      WHERE accepted.token LIKE %s AND COALESCE(effects.completions, 0) <> 1
      """,
      [f"{run_id}:%"],
    )
    lost = cursor.fetchone()[0]
    cursor.execute(
      f"SELECT COUNT(*) FROM {effects} WHERE token LIKE %s AND completions > 1",
      [f"{run_id}:%"],
    )
    duplicates = cursor.fetchone()[0]
    cursor.execute(
      f"""
      SELECT COUNT(*) FROM {effects}
      WHERE token LIKE %s AND category = 'failure' AND (attempts <> 2 OR completions <> 1)
      """,
      [f"{run_id}:%"],
    )
    bad_retries = cursor.fetchone()[0]
  if lost:
    problems.append(f"{lost} accepted jobs have no single completed side effect")
  if duplicates:
    problems.append(f"{duplicates} jobs produced duplicate side effects")
  if bad_retries:
    problems.append(f"{bad_retries} expected failure jobs did not complete on the second attempt")

  load_failures = FailedExecution.objects.filter(job__task_path__in=TASK_PATHS).count()
  if load_failures:
    problems.append(f"{load_failures} load jobs remain failed")

  recurring = RecurringExecution.objects.filter(task_key__startswith=f"{run_id}:recurring:")
  recurring_without_job = recurring.filter(job__isnull=True).count()
  recurring_unfinished = recurring.filter(job__finished_at__isnull=True).count()
  known_mismatch = (
    recurring.filter(intended_job_id__isnull=False).filter(~Q(intended_job_id=F("job_id"))).count()
  )
  recurring_effects = 0
  with connection.cursor() as cursor:
    cursor.execute(
      f"SELECT COUNT(*) FROM {effects} WHERE token LIKE %s AND completions = 1",
      [f"{run_id}:recurring:%"],
    )
    recurring_effects = cursor.fetchone()[0]
  if recurring_without_job:
    problems.append(f"{recurring_without_job} load recurring reservations have no job")
  if recurring_unfinished:
    problems.append(f"{recurring_unfinished} load recurring jobs are unfinished")
  if known_mismatch:
    problems.append(f"{known_mismatch} load recurring reservations have mismatched identity")
  if recurring_effects != recurring.count():
    problems.append(
      f"recurring side effects do not match reservations: {recurring_effects}/{recurring.count()}"
    )

  unfinished_load_jobs = Job.objects.filter(
    task_path__in=TASK_PATHS,
    finished_at__isnull=True,
  ).count()
  if unfinished_load_jobs:
    problems.append(f"{unfinished_load_jobs} load jobs are unfinished")

  result = {
    "healthy": not problems,
    "problems": problems,
    "status": status,
    "accepted_loss": lost,
    "duplicate_side_effects": duplicates,
    "bad_retries": bad_retries,
    "recurring_reservations": recurring.count(),
    "recurring_effects": recurring_effects,
  }
  print_json(result)
  if problems:
    raise SystemExit(1)


def queue_status_data(run_id):
  from django.db import connection

  from dj_queue.models import (
    BlockedExecution,
    ClaimedExecution,
    FailedExecution,
    ReadyExecution,
    ScheduledExecution,
  )

  accepted = connection.ops.quote_name("dj_queue_prerelease_accepted")
  effects = connection.ops.quote_name("dj_queue_prerelease_effects")
  with connection.cursor() as cursor:
    cursor.execute(f"SELECT COUNT(*) FROM {accepted} WHERE token LIKE %s", [f"{run_id}:%"])
    accepted_count = cursor.fetchone()[0]
    cursor.execute(
      f"SELECT COUNT(*) FROM {effects} WHERE token LIKE %s AND completions = 1",
      [f"{run_id}:%"],
    )
    completed_count = cursor.fetchone()[0]
  values = {
    "accepted": accepted_count,
    "completed": completed_count,
    "ready": ReadyExecution.objects.count(),
    "scheduled": ScheduledExecution.objects.count(),
    "claimed": ClaimedExecution.objects.count(),
    "blocked": BlockedExecution.objects.count(),
    "failed": FailedExecution.objects.count(),
  }
  values["depth"] = sum(
    values[name] for name in ("ready", "scheduled", "claimed", "blocked", "failed")
  )
  return values


def print_json(value):
  print(json.dumps(value, sort_keys=True, default=str), flush=True)


def main(argv):
  configure_logging()
  args = parse_args(argv)
  if args.command == "create-database":
    create_database()
    return 0
  if args.command == "drop-database":
    drop_database()
    return 0

  setup_django()
  if args.command == "migrate":
    migrate()
  elif args.command == "seed":
    seed_database(
      jobs=args.jobs,
      queues=args.queues,
      semaphores=args.semaphores,
      recurring_executions=args.recurring_executions,
    )
  elif args.command == "supervise":
    supervise(duration=args.duration)
  elif args.command == "calibrate":
    calibrate(run_id=args.run_id, jobs=args.jobs, timeout=args.timeout)
  elif args.command == "produce":
    produce(run_id=args.run_id, rate=args.rate, duration=args.duration)
  elif args.command == "retry":
    retry_expected_failures(interval=args.interval, duration=args.duration)
  elif args.command == "stop-recurring":
    stop_recurring(args.run_id)
  elif args.command == "status":
    print_json(queue_status_data(args.run_id))
  elif args.command == "verify":
    verify(args.run_id)
  return 0


if __name__ == "__main__":
  raise SystemExit(main(sys.argv[1:]))
