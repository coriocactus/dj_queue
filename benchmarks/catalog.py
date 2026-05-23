SCENARIO_CONTEXT = {
  "single-enqueue": {
    "description": "one-by-one immediate enqueue latency and throughput",
    "key_metric": "latency_p95_ms",
    "key_metric_note": "enqueue tail latency for individual task submissions; lower is better",
    "healthy_local_baseline": "`<= 15 ms` p95 for request-path enqueue on the 10k local benchmark",
    "use_case": (
      "web requests, admin actions, and small fan-out paths that submit tasks one at a time"
    ),
    "mechanics": (
      "calls the public `Task.enqueue()` path once per job, including validation, "
      "job insert, ready-row insert, result mapping, and ready wakeup registration"
    ),
  },
  "bulk-enqueue": {
    "description": "bulk immediate enqueue throughput and SQL statement count",
    "key_metric": "jobs_per_second",
    "key_metric_note": (
      "bulk enqueue throughput; `query_count` should stay nearly fixed as size grows"
    ),
    "healthy_local_baseline": (
      "`>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds"
    ),
    "use_case": "imports, backfills, and fan-out jobs that enqueue many independent tasks",
    "mechanics": (
      "calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including "
      "bulk job and ready-row inserts plus batched result creation"
    ),
  },
  "scheduled-promotion": {
    "description": "due scheduled-row promotion from a mixed due/future backlog",
    "key_metric": "rows_per_second",
    "key_metric_note": "due scheduled-row promotion throughput; higher is better",
    "healthy_local_baseline": (
      "`>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds"
    ),
    "use_case": "delayed-job bursts where the dispatcher must move due work into ready state",
    "mechanics": (
      "seeds equal due and future scheduled backlogs, then calls "
      "`promote_scheduled_jobs()` in batches until no due rows remain"
    ),
  },
  "recurring-scale": {
    "description": "scheduler poll cost for persisted not-due recurring rows",
    "key_metric": "duration_seconds",
    "key_metric_note": (
      "scheduler no-op poll duration over persisted not-due recurring rows; lower is better"
    ),
    "healthy_local_baseline": "`<= 0.025 seconds` for a no-op poll over 10k not-due schedules",
    "use_case": "large recurring-task catalogs where most scheduler ticks should be cheap no-ops",
    "mechanics": (
      "seeds dynamic recurring definitions with future `next_run_at` values, then runs one "
      "`Scheduler.poll_once()` without firing jobs"
    ),
  },
  "worker-drain": {
    "description": "async supervisor drain throughput for no-op ready jobs",
    "key_metric": "jobs_per_second",
    "key_metric_note": (
      "end-to-end ready-job drain throughput through the async runtime; higher is better"
    ),
    "healthy_local_baseline": "`>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds",
    "use_case": "steady ready-queue processing by embedded or standalone async workers",
    "mechanics": (
      "seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through "
      "worker claim, execution, completion, and finished-job retention"
    ),
  },
  "concurrency-contention": {
    "description": "one hot concurrency key through enqueue, block, release, and unblock",
    "key_metric": "drain_jobs_per_second",
    "key_metric_note": "serialized hot-key drain throughput after enqueue; higher is better",
    "healthy_local_baseline": "`>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes",
    "use_case": "per-tenant, per-account, or external API limits where one hot key must serialize work",
    "mechanics": (
      "enqueues jobs sharing one concurrency key so all but one block, then drains with "
      "`claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock"
    ),
  },
  "ordered-selector-claim": {
    "description": "ordered exact-queue claiming and drain throughput",
    "key_metric": "jobs_per_second",
    "key_metric_note": "selector-heavy claim and drain throughput; higher is better",
    "healthy_local_baseline": "`>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes",
    "use_case": "workers with ordered queue preferences, priority lanes, or queue-isolated tenants",
    "mechanics": (
      "seeds three queues and drains with exact ordered selectors to cover selector ordering, "
      "claim locking, query shape, and completion"
    ),
  },
}

SCENARIO_DESCRIPTIONS = {
  scenario: context["description"] for scenario, context in SCENARIO_CONTEXT.items()
}

EXCLUDED_SCENARIOS_BY_BACKEND = {
  "sqlite": frozenset({"concurrency-contention", "ordered-selector-claim"}),
}


def scenarios_for_backend(backend, scenario_names):
  excluded = EXCLUDED_SCENARIOS_BY_BACKEND.get(backend, frozenset())
  return tuple(scenario for scenario in scenario_names if scenario not in excluded)


def scenario_supported_by_backend(backend, scenario):
  return scenario in scenarios_for_backend(backend, (scenario,))
