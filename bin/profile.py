#!/usr/bin/env -S uv run --script

import argparse
import json
import os
import sys
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import perf_counter

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
  sys.path.insert(0, str(PROJECT_ROOT))

PROFILE_SHAPES = {
  "small": {
    "jobs": 10_000,
    "queues": 20,
    "processes": 10,
    "recurring": 50,
    "semaphores": 50,
  },
  "medium": {
    "jobs": 100_000,
    "queues": 100,
    "processes": 50,
    "recurring": 500,
    "semaphores": 1_000,
  },
  "large": {
    "jobs": 1_000_000,
    "queues": 500,
    "processes": 200,
    "recurring": 5_000,
    "semaphores": 10_000,
  },
}
SCENARIO_ORDER = (
  "backend-snapshot",
  "all-backend-snapshots",
  "stats-payload",
  "metric-families",
  "dashboard-overview",
  "dashboard-queues-sort-ready",
  "dashboard-queues-sort-latency",
  "dashboard-queues-sort-workers",
  "dashboard-processes-sort-status",
  "dashboard-recurring-sort-next-run",
  "dashboard-semaphores-sort-blocked-waiters",
  "queue-info-all",
  "queue-page-ready",
  "queue-page-ready-deep",
  "queue-page-scheduled",
  "queue-page-claimed",
  "queue-page-blocked",
  "queue-page-failed",
  "queue-page-finished",
  "queue-page-finished-deep",
  "queue-page-invalid",
  "worker-empty-claim",
  "dispatcher-no-due-scheduled",
  "dispatcher-no-expired-blocked",
  "dispatcher-no-expired-semaphores",
  "scheduler-no-due-recurring",
  "scheduler-cleanup-not-due",
  "deep-health",
)
EXTRA_SCENARIOS = ("ordered-selector-claim",)
SCENARIO_CHOICES = (*SCENARIO_ORDER, *EXTRA_SCENARIOS)


@dataclass(frozen=True, slots=True)
class ProfileShape:
  jobs: int
  queues: int
  processes: int
  recurring: int
  semaphores: int


class Timer:
  def __enter__(self):
    self.started_at = perf_counter()
    return self

  def __exit__(self, exc_type, exc, tb):
    self.duration = perf_counter() - self.started_at
    return False


def parse_args(argv):
  parser = argparse.ArgumentParser(
    description="Profile dj_queue read surfaces against deterministic benchmark-scale rows.",
  )
  parser.add_argument(
    "--backend",
    choices=("sqlite", "postgres", "mysql", "mariadb"),
    default=os.environ.get("BENCHMARK_BACKEND", "postgres"),
  )
  parser.add_argument(
    "--profile",
    choices=tuple(PROFILE_SHAPES),
    default="small",
    help="Named row-count shape.",
  )
  parser.add_argument("--jobs", type=int, help="Override job count.")
  parser.add_argument("--queues", type=int, help="Override queue count.")
  parser.add_argument("--processes", type=int, help="Override process count.")
  parser.add_argument("--recurring", type=int, help="Override recurring task count.")
  parser.add_argument("--semaphores", type=int, help="Override semaphore count.")
  parser.add_argument(
    "--scenario",
    action="append",
    choices=SCENARIO_CHOICES,
    help="Scenario to run. Repeat to run several. Defaults to all.",
  )
  parser.add_argument("--output", help="JSONL output path.")
  parser.add_argument(
    "--create-db",
    action="store_true",
    help="Create missing benchmark database for PostgreSQL/MySQL/MariaDB.",
  )
  parser.add_argument(
    "--no-migrate",
    action="store_true",
    help="Skip migrations before destructive reset.",
  )
  parser.add_argument(
    "--explain",
    action="store_true",
    help="For PostgreSQL, explain the slowest captured SELECT per scenario.",
  )
  parser.add_argument(
    "--database-name",
    help="Override BENCHMARK_DB_NAME. Names must contain 'benchmark'.",
  )
  return parser.parse_args(argv)


def configure_environment(args):
  os.environ.setdefault("DJANGO_SETTINGS_MODULE", "benchmarks.settings")
  os.environ["BENCHMARK_BACKEND"] = args.backend
  if args.database_name:
    os.environ["BENCHMARK_DB_NAME"] = args.database_name


def profile_shape(args):
  shape = ProfileShape(**PROFILE_SHAPES[args.profile])
  for field_name in ("jobs", "queues", "processes", "recurring", "semaphores"):
    override = getattr(args, field_name)
    if override is not None:
      if override < 0:
        raise ValueError(f"--{field_name} must be non-negative")
      shape = replace(shape, **{field_name: override})
  if shape.jobs <= 0:
    raise ValueError("profile must include at least one job")
  if shape.queues <= 0:
    raise ValueError("profile must include at least one queue")
  return shape


def default_output_path(backend):
  timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
  return PROJECT_ROOT / "benchmark-results" / f"profile-{backend}-{timestamp}.jsonl"


def setup_django(args):
  import django

  django.setup()

  from benchmarks.harness import ensure_database_exists, prepare_database, reset_database

  if args.create_db:
    ensure_database_exists(args.backend)
  prepare_database(migrate=not args.no_migrate)
  reset_database()


def seed_profile_data(shape):
  from django.utils import timezone

  from dj_queue.models import Semaphore

  now = timezone.now()
  with Timer() as timer:
    semaphores = [
      Semaphore(
        key=semaphore_key(index),
        value=index % 5,
        limit=5,
        expires_at=now + timedelta(days=1, minutes=index % 60),
        created_at=now,
        updated_at=now,
      )
      for index in range(shape.semaphores)
    ]
    Semaphore.objects.bulk_create(semaphores, batch_size=5_000)

    processes = seed_processes(shape, now)
    seed_pauses(shape, now)
    seed_recurring(shape, now)
    state_counts = seed_jobs(shape, now, processes)

  return {
    "seed_seconds": timer.duration,
    "shape": asdict(shape),
    "state_counts": state_counts,
  }


def seed_processes(shape, now):
  from dj_queue.models import Process

  kinds = ("Worker", "Dispatcher", "Scheduler", "Supervisor")
  processes = []
  for index in range(shape.processes):
    kind = kinds[index % len(kinds)]
    processes.append(
      Process.objects.create(
        backend_alias="default",
        kind=kind,
        pid=10_000 + index,
        hostname="profile-host",
        name=f"profile-{kind.lower()}-{index:05d}",
        metadata=process_metadata(kind, index),
        last_heartbeat_at=now - timedelta(seconds=index % 30),
      )
    )
  if not processes:
    processes.append(
      Process.objects.create(
        backend_alias="default",
        kind="Worker",
        pid=10_000,
        hostname="profile-host",
        name="profile-worker-00000",
        metadata={"queues": ["*"]},
        last_heartbeat_at=now,
      )
    )
  return processes


def process_metadata(kind, index):
  if kind == "Worker":
    if index % 3 == 0:
      return {"queues": ["*"]}
    return {"queues": [queue_name(index), queue_name(index + 1)]}
  if kind == "Dispatcher":
    return {"batch_size": 500, "polling_interval": 1}
  if kind == "Scheduler":
    return {"polling_interval": 5}
  return {"mode": "async"}


def seed_pauses(shape, now):
  from dj_queue.models import Pause

  paused_count = min(max(shape.queues // 10, 1), shape.queues)
  pauses = [
    Pause(
      backend_alias="default",
      queue_name=queue_name(index * 10),
      created_at=now - timedelta(minutes=5),
    )
    for index in range(paused_count)
  ]
  Pause.objects.bulk_create(pauses, batch_size=1_000)


def seed_recurring(shape, now):
  from dj_queue.models import RecurringTask

  tasks = []
  for index in range(shape.recurring):
    key = f"profile-recurring-{index:06d}"
    tasks.append(
      RecurringTask(
        backend_alias="default",
        key=key,
        task_path="benchmarks.tasks.noop",
        payload={"args": [index], "kwargs": {}},
        schedule="*/5 * * * *",
        queue_name=queue_name(index),
        priority=index % 5,
        static=False,
        next_run_at=now + timedelta(days=1, minutes=index % 30),
        created_at=now,
        updated_at=now,
      )
    )
  RecurringTask.objects.bulk_create(tasks, batch_size=5_000)


def seed_jobs(shape, now, processes):
  from dj_queue.models import (
    BlockedExecution,
    ClaimedExecution,
    FailedExecution,
    Job,
    ReadyExecution,
    ScheduledExecution,
  )

  state_counts = {
    "ready": 0,
    "scheduled": 0,
    "blocked": 0,
    "failed": 0,
    "claimed": 0,
    "finished": 0,
  }
  for start in range(0, shape.jobs, 5_000):
    jobs = []
    ready_rows = []
    scheduled_rows = []
    blocked_rows = []
    failed_rows = []
    claimed_rows = []
    for index in range(start, min(start + 5_000, shape.jobs)):
      state = state_for_index(index)
      state_counts[state] += 1
      created_at = now - timedelta(seconds=index % 86_400)
      job = Job(
        id=uuid.uuid4(),
        task_path="benchmarks.tasks.noop",
        queue_name=queue_name(index),
        priority=(index % 21) - 10,
        payload={"args": [index], "kwargs": {}},
        backend_alias="default",
        scheduled_at=scheduled_at_for_state(state, now, index),
        concurrency_key=semaphore_key(index) if state == "blocked" else None,
        finished_at=created_at + timedelta(seconds=1) if state == "finished" else None,
        return_value=index if state == "finished" else None,
        created_at=created_at,
        updated_at=created_at,
      )
      jobs.append(job)
      if state == "ready":
        ready_rows.append(
          ReadyExecution(
            job_id=job.id,
            backend_alias="default",
            queue_name=job.queue_name,
            priority=job.priority,
            created_at=created_at,
            latency_started_at=created_at,
          )
        )
      elif state == "scheduled":
        scheduled_rows.append(
          ScheduledExecution(
            job_id=job.id,
            backend_alias="default",
            queue_name=job.queue_name,
            priority=job.priority,
            scheduled_at=job.scheduled_at,
            created_at=created_at,
          )
        )
      elif state == "blocked":
        blocked_rows.append(
          BlockedExecution(
            job_id=job.id,
            backend_alias="default",
            queue_name=job.queue_name,
            priority=job.priority,
            concurrency_key=job.concurrency_key,
            expires_at=now + timedelta(days=1, minutes=index % 60),
            created_at=created_at,
          )
        )
      elif state == "failed":
        failed_rows.append(
          FailedExecution(
            job_id=job.id,
            exception_class="builtins.ValueError",
            message="profile failure",
            traceback="profile traceback",
            created_at=created_at,
          )
        )
      elif state == "claimed":
        process = processes[index % len(processes)]
        claimed_rows.append(
          ClaimedExecution(job_id=job.id, process_id=process.id, created_at=created_at)
        )

    Job.objects.bulk_create(jobs, batch_size=5_000)
    ReadyExecution.objects.bulk_create(ready_rows, batch_size=5_000)
    ScheduledExecution.objects.bulk_create(scheduled_rows, batch_size=5_000)
    BlockedExecution.objects.bulk_create(blocked_rows, batch_size=5_000)
    FailedExecution.objects.bulk_create(failed_rows, batch_size=5_000)
    ClaimedExecution.objects.bulk_create(claimed_rows, batch_size=5_000)
  return state_counts


def state_for_index(index):
  bucket = index % 20
  if bucket < 3:
    return "ready"
  if bucket < 5:
    return "scheduled"
  if bucket < 7:
    return "blocked"
  if bucket < 9:
    return "failed"
  if bucket == 9:
    return "claimed"
  return "finished"


def scheduled_at_for_state(state, now, index):
  if state != "scheduled":
    return None
  return now + timedelta(days=1, minutes=index % 60)


def queue_name(index):
  return f"queue-{index % max(active_shape.queues, 1):05d}"


def semaphore_key(index):
  return f"profile-key-{index % max(active_shape.semaphores, 1):06d}"


def run_scenario(name, *, explain):
  from django.db import connection
  from django.test.utils import CaptureQueriesContext

  scenario = SCENARIOS[name]
  with CaptureQueriesContext(connection) as captured:
    with Timer() as timer:
      metrics = scenario()

  captured_queries = list(captured.captured_queries)
  metrics.update(
    {
      "duration_seconds": timer.duration,
      "query_count": len(captured_queries),
      "query_time_seconds": sum(query_time(query) for query in captured_queries),
      "slow_queries": slow_queries(captured_queries),
    }
  )
  if explain and connection.vendor == "postgresql":
    plan = explain_slowest_select(captured_queries)
    if plan is not None:
      metrics["explain"] = plan
  return metrics


@contextmanager
def _query_counter():
  from django.db import connection

  state = {"count": 0}

  def wrapper(execute, sql, params, many, context):
    state["count"] += 1
    return execute(sql, params, many, context)

  with connection.execute_wrapper(wrapper):
    yield state


def scenario_backend_snapshot():
  from dj_queue import observability

  snapshot = observability.backend_snapshot(backend_alias="default")
  return snapshot_metrics(snapshot)


def scenario_all_backend_snapshots():
  from dj_queue import observability

  snapshots = observability.all_backend_snapshots()
  return {
    "backend_count": len(snapshots),
    "queue_rows": sum(len(snapshot.queue_rows) for snapshot in snapshots),
    "process_rows": sum(len(snapshot.process_rows) for snapshot in snapshots),
    "recurring_rows": sum(len(snapshot.recurring_rows) for snapshot in snapshots),
    "semaphore_rows": sum(len(snapshot.semaphore_rows) for snapshot in snapshots),
  }


def scenario_stats_payload():
  from dj_queue import observability

  payload = observability.stats_payload()
  return {
    "backend_count": len(payload["backends"]),
    "json_bytes": len(json.dumps(payload, default=str)),
  }


def scenario_metric_families():
  from dj_queue.metrics import metric_families

  families = metric_families()
  return {
    "family_count": len(families),
    "sample_count": sum(len(family.samples) for family in families),
  }


def scenario_dashboard_overview():
  from dj_queue import dashboard

  stub_dashboard_urls(dashboard)
  context = dashboard.dashboard_context(backend_alias="default")
  return dashboard_metrics(context)


def scenario_dashboard_queues_sort_ready():
  return scenario_dashboard_sort({"queues_sort": "-ready"})


def scenario_dashboard_queues_sort_latency():
  return scenario_dashboard_sort({"queues_sort": "-latency"})


def scenario_dashboard_queues_sort_workers():
  return scenario_dashboard_sort({"queues_sort": "-workers"})


def scenario_dashboard_processes_sort_status():
  return scenario_dashboard_sort({"processes_sort": "-status"})


def scenario_dashboard_recurring_sort_next_run():
  return scenario_dashboard_sort({"recurring_sort": "next_run"})


def scenario_dashboard_semaphores_sort_blocked_waiters():
  return scenario_dashboard_sort({"semaphores_sort": "-blocked_waiters"})


def scenario_dashboard_sort(query_params):
  from dj_queue import dashboard

  stub_dashboard_urls(dashboard)
  context = dashboard.dashboard_context(backend_alias="default", query_params=query_params)
  return dashboard_metrics(context)


def dashboard_metrics(context):
  return {
    "summary_cards": len(context["summary_cards"]),
    "queue_rows": len(context["queue_section"]["rows"]),
    "queue_total": context["queue_section"]["total_count"],
    "process_rows": len(context["process_section"]["rows"]),
    "recurring_rows": len(context["recurring_section"]["rows"]),
    "semaphore_rows": len(context["semaphore_section"]["rows"]),
  }


def scenario_queue_info_all():
  from dj_queue.api import QueueInfo

  queues = QueueInfo.all()
  return {
    "queue_count": len(queues),
    "ready_total": sum(queue.size for queue in queues),
    "paused_count": sum(1 for queue in queues if queue.paused),
    "latency_count": sum(1 for queue in queues if queue.latency is not None),
  }


def scenario_queue_page_ready():
  return scenario_queue_page("ready")


def scenario_queue_page_ready_deep():
  return scenario_queue_page("ready", page_number=5)


def scenario_queue_page_scheduled():
  return scenario_queue_page("scheduled")


def scenario_queue_page_claimed():
  return scenario_queue_page("claimed")


def scenario_queue_page_blocked():
  return scenario_queue_page("blocked")


def scenario_queue_page_failed():
  return scenario_queue_page("failed")


def scenario_queue_page_finished():
  return scenario_queue_page("finished")


def scenario_queue_page_finished_deep():
  return scenario_queue_page("finished", page_number=5)


def scenario_queue_page_invalid():
  return scenario_queue_page("invalid")


def scenario_queue_page(state, *, page_number=1):
  from dj_queue import dashboard

  stub_dashboard_urls(dashboard)
  context = dashboard.queue_page_context(
    backend_alias="default",
    queue_name=queue_name(queue_index_for_state(state)),
    state=state,
    page_number=page_number,
  )
  return {
    "job_rows": len(context["jobs"]),
    "page_number": context["page_obj"].number,
    "result_count_text": context["result_count_text"],
    "state_tabs": len(context["state_tabs"]),
  }


def queue_index_for_state(state):
  return {
    "ready": 1,
    "scheduled": 3,
    "blocked": 5,
    "failed": 7,
    "claimed": 9,
    "finished": 10,
    "invalid": 1,
  }[state]


def scenario_worker_empty_claim():
  from dj_queue.operations.jobs import claim_ready_jobs

  claimed_jobs = claim_ready_jobs(
    limit=100, queues=("__profile_empty__",), backend_alias="default"
  )
  return {"claimed_count": len(claimed_jobs)}


def scenario_ordered_selector_claim():
  from django.utils import timezone

  from benchmarks.tasks import noop
  from dj_queue.models import ClaimedExecution, Job, ReadyExecution
  from dj_queue.operations.jobs import claim_ready_jobs, execute_claimed_job

  selectors = (
    "profile-selector-alpha",
    "profile-selector-beta",
    "profile-selector-gamma",
  )
  size = min(active_shape.jobs, 10_000)
  now = timezone.now()

  with _query_counter() as setup_queries:
    with Timer() as setup_timer:
      jobs = [
        Job(
          id=uuid.uuid4(),
          task_path=noop.module_path,
          queue_name=selectors[index % len(selectors)],
          priority=noop.priority,
          payload={"args": [f"selector-profile-{index}"], "kwargs": {}},
          backend_alias="default",
          created_at=now,
          updated_at=now,
        )
        for index in range(size)
      ]
      Job.objects.bulk_create(jobs, batch_size=5_000)
      ReadyExecution.objects.bulk_create(
        [
          ReadyExecution(
            job_id=job.id,
            backend_alias=job.backend_alias,
            queue_name=job.queue_name,
            priority=job.priority,
            created_at=now,
            latency_started_at=now,
          )
          for job in jobs
        ],
        batch_size=5_000,
      )

  phase_metrics = {selector: {"claims": 0, "duration": 0, "queries": 0} for selector in selectors}
  claim_duration = 0
  execute_duration = 0
  claim_query_count = 0
  execute_query_count = 0
  completed = 0

  with Timer() as drain_timer:
    while completed < size:
      with _query_counter() as claim_queries:
        with Timer() as claim_timer:
          claimed_jobs = claim_ready_jobs(limit=3, queues=selectors, backend_alias="default")
      claim_duration += claim_timer.duration
      claim_query_count += claim_queries["count"]
      if not claimed_jobs:
        break

      phase = phase_metrics[claimed_jobs[0].job.queue_name]
      phase["claims"] += 1
      phase["duration"] += claim_timer.duration
      phase["queries"] += claim_queries["count"]

      with _query_counter() as execute_queries:
        with Timer() as execute_timer:
          for claimed_job in claimed_jobs:
            execute_claimed_job(claimed_job, backend_alias="default")
      execute_duration += execute_timer.duration
      execute_query_count += execute_queries["count"]
      completed += len(claimed_jobs)

  finished_count = Job.objects.filter(
    queue_name__in=selectors,
    finished_at__isnull=False,
  ).count()
  if (
    completed != size
    or finished_count != size
    or ReadyExecution.objects.filter(queue_name__in=selectors).exists()
    or ClaimedExecution.objects.filter(job__queue_name__in=selectors).exists()
  ):
    raise AssertionError("ordered selector profile did not drain all ready jobs")

  metrics = {
    "profile_job_count": size,
    "completed_count": completed,
    "finished_count": finished_count,
    "setup_duration_seconds": setup_timer.duration,
    "setup_query_count": setup_queries["count"],
    "drain_duration_seconds": drain_timer.duration,
    "jobs_per_second": size / drain_timer.duration if drain_timer.duration > 0 else None,
    "claim_duration_seconds": claim_duration,
    "claim_query_count": claim_query_count,
    "execute_duration_seconds": execute_duration,
    "execute_query_count": execute_query_count,
  }
  for index, selector in enumerate(selectors):
    label = selector.rsplit("-", 1)[-1]
    phase = phase_metrics[selector]
    metrics[f"claim_{label}_attempts"] = phase["claims"]
    metrics[f"claim_{label}_duration_seconds"] = phase["duration"]
    metrics[f"claim_{label}_query_count"] = phase["queries"]
    metrics[f"claim_{label}_empty_prefix_selector_count"] = phase["claims"] * index
  return metrics


def scenario_dispatcher_no_due_scheduled():
  from dj_queue.operations.jobs import promote_scheduled_jobs

  promoted_jobs = promote_scheduled_jobs(batch_size=500, backend_alias="default")
  return {"promoted_count": len(promoted_jobs)}


def scenario_dispatcher_no_expired_blocked():
  from dj_queue.operations.concurrency import promote_expired_blocked_jobs

  promoted_jobs = promote_expired_blocked_jobs(batch_size=500, backend_alias="default")
  return {"promoted_count": len(promoted_jobs)}


def scenario_dispatcher_no_expired_semaphores():
  from dj_queue.operations.concurrency import cleanup_expired_semaphores

  return {"deleted_count": cleanup_expired_semaphores(backend_alias="default")}


def scenario_scheduler_no_due_recurring():
  from django.utils import timezone

  from dj_queue.operations.recurring import fire_due_recurring_tasks

  fired_jobs = fire_due_recurring_tasks(
    timezone.now(),
    include_dynamic_tasks=True,
    backend_alias="default",
    batch_size=500,
  )
  return {"fired_count": len(fired_jobs)}


def scenario_scheduler_cleanup_not_due():
  from django.utils import timezone

  from dj_queue.operations.cleanup import (
    clear_failed_jobs,
    clear_finished_jobs,
    clear_recurring_executions,
  )

  now = timezone.now()
  older_than = 10 * 365 * 24 * 60 * 60
  return {
    "finished_deleted": clear_finished_jobs(
      older_than=older_than,
      backend_alias="default",
      now=now,
    ),
    "failed_deleted": clear_failed_jobs(
      older_than=older_than,
      backend_alias="default",
      now=now,
    ),
    "recurring_deleted": clear_recurring_executions(
      older_than=older_than,
      backend_alias="default",
      now=now,
    ),
  }


def scenario_deep_health():
  from dj_queue import observability

  problems = observability.deep_health_problems(backend_alias="default")
  return {"problem_count": len(problems)}


def stub_dashboard_urls(dashboard):
  dashboard._job_changelist_url = lambda backend_alias, **filters: "#jobs"
  dashboard._failed_execution_changelist_url = lambda backend_alias, **filters: "#failed"


def snapshot_metrics(snapshot):
  return {
    "queue_rows": len(snapshot.queue_rows),
    "process_rows": len(snapshot.process_rows),
    "recurring_rows": len(snapshot.recurring_rows),
    "semaphore_rows": len(snapshot.semaphore_rows),
  }


def query_time(query):
  try:
    return float(query.get("time") or 0)
  except (TypeError, ValueError):
    return 0.0


def slow_queries(captured_queries, limit=5):
  queries = sorted(captured_queries, key=query_time, reverse=True)[:limit]
  return [
    {
      "seconds": query_time(query),
      "sql": compact_sql(query["sql"]),
    }
    for query in queries
  ]


def explain_slowest_select(captured_queries):
  from django.db import connection

  candidates = [query for query in captured_queries if is_select(query["sql"])]
  if not candidates:
    return None
  query = max(candidates, key=query_time)
  with connection.cursor() as cursor:
    cursor.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query['sql']}")
    plan = cursor.fetchone()[0]
  if isinstance(plan, str):
    plan = json.loads(plan)
  root = plan[0]["Plan"]
  return {
    "source_sql": compact_sql(query["sql"]),
    "node_type": root.get("Node Type"),
    "actual_rows": root.get("Actual Rows"),
    "actual_total_time_ms": root.get("Actual Total Time"),
    "shared_hit_blocks": root.get("Shared Hit Blocks"),
    "shared_read_blocks": root.get("Shared Read Blocks"),
    "temp_read_blocks": root.get("Temp Read Blocks"),
    "temp_written_blocks": root.get("Temp Written Blocks"),
  }


def is_select(sql):
  return sql.lstrip().upper().startswith("SELECT")


def compact_sql(sql, limit=500):
  compact = " ".join(sql.split())
  if len(compact) <= limit:
    return compact
  return f"{compact[:limit]}…"


def write_record(path, record):
  path.parent.mkdir(parents=True, exist_ok=True)
  line = json.dumps(record, sort_keys=True, default=str)
  with path.open("a", encoding="utf-8") as handle:
    handle.write(f"{line}\n")
  print(line)


def result_record(*, args, shape, seed_metrics, scenario, metrics, metadata):
  return {
    "schema_version": 1,
    "recorded_at": datetime.now(UTC).isoformat(),
    "kind": "profile",
    "profile": args.profile,
    "scenario": scenario,
    "shape": asdict(shape),
    "seed": seed_metrics,
    "metrics": metrics,
    "metadata": metadata,
  }


SCENARIOS = {
  "backend-snapshot": scenario_backend_snapshot,
  "all-backend-snapshots": scenario_all_backend_snapshots,
  "stats-payload": scenario_stats_payload,
  "metric-families": scenario_metric_families,
  "dashboard-overview": scenario_dashboard_overview,
  "dashboard-queues-sort-ready": scenario_dashboard_queues_sort_ready,
  "dashboard-queues-sort-latency": scenario_dashboard_queues_sort_latency,
  "dashboard-queues-sort-workers": scenario_dashboard_queues_sort_workers,
  "dashboard-processes-sort-status": scenario_dashboard_processes_sort_status,
  "dashboard-recurring-sort-next-run": scenario_dashboard_recurring_sort_next_run,
  "dashboard-semaphores-sort-blocked-waiters": (
    scenario_dashboard_semaphores_sort_blocked_waiters
  ),
  "queue-info-all": scenario_queue_info_all,
  "queue-page-ready": scenario_queue_page_ready,
  "queue-page-ready-deep": scenario_queue_page_ready_deep,
  "queue-page-scheduled": scenario_queue_page_scheduled,
  "queue-page-claimed": scenario_queue_page_claimed,
  "queue-page-blocked": scenario_queue_page_blocked,
  "queue-page-failed": scenario_queue_page_failed,
  "queue-page-finished": scenario_queue_page_finished,
  "queue-page-finished-deep": scenario_queue_page_finished_deep,
  "queue-page-invalid": scenario_queue_page_invalid,
  "worker-empty-claim": scenario_worker_empty_claim,
  "ordered-selector-claim": scenario_ordered_selector_claim,
  "dispatcher-no-due-scheduled": scenario_dispatcher_no_due_scheduled,
  "dispatcher-no-expired-blocked": scenario_dispatcher_no_expired_blocked,
  "dispatcher-no-expired-semaphores": scenario_dispatcher_no_expired_semaphores,
  "scheduler-no-due-recurring": scenario_scheduler_no_due_recurring,
  "scheduler-cleanup-not-due": scenario_scheduler_cleanup_not_due,
  "deep-health": scenario_deep_health,
}
active_shape = ProfileShape(**PROFILE_SHAPES["small"])


def main(argv):
  global active_shape

  args = parse_args(argv)
  configure_environment(args)
  active_shape = profile_shape(args)
  setup_django(args)

  from benchmarks.harness import environment_metadata

  output = Path(args.output) if args.output else default_output_path(args.backend)
  seed_metrics = seed_profile_data(active_shape)
  metadata = environment_metadata(backend=args.backend)
  scenarios = tuple(args.scenario or SCENARIO_ORDER)

  for scenario in scenarios:
    metrics = run_scenario(scenario, explain=args.explain)
    write_record(
      output,
      result_record(
        args=args,
        shape=active_shape,
        seed_metrics=seed_metrics,
        scenario=scenario,
        metrics=metrics,
        metadata=metadata,
      ),
    )
  return 0


if __name__ == "__main__":
  raise SystemExit(main(sys.argv[1:]))
