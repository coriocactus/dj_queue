from dataclasses import dataclass

from dj_queue import observability


@dataclass(frozen=True, slots=True)
class MetricSample:
  labels: tuple[str, ...]
  value: float


@dataclass(frozen=True, slots=True)
class MetricFamily:
  name: str
  help_text: str
  labels: tuple[str, ...]
  samples: tuple[MetricSample, ...]


def metric_families(*, snapshots=None):
  if snapshots is None:
    snapshots = observability.all_backend_snapshots()

  queue_jobs = []
  queue_paused = []
  queue_latency = []
  queue_workers = []
  runner_processes = []
  runner_processes_by_kind = []
  recurring_tasks = []
  semaphores = []
  process_rows = []
  seen_queue_databases = set()

  for snapshot in snapshots:
    backend_alias = snapshot["backend_alias"]
    queue_database_alias = snapshot["queue_database_alias"]
    runner_metrics = snapshot["runner_metrics"]

    for queue in snapshot["queue_rows"]:
      for state in ("ready", "claimed", "scheduled", "blocked", "failed", "finished"):
        queue_jobs.append(
          MetricSample(
            labels=(backend_alias, queue["name"], state),
            value=queue[f"{state}_count"],
          )
        )
      queue_paused.append(
        MetricSample(
          labels=(backend_alias, queue["name"]),
          value=1 if queue["paused"] else 0,
        )
      )
      if queue["latency_seconds"] is not None:
        queue_latency.append(
          MetricSample(
            labels=(backend_alias, queue["name"]),
            value=queue["latency_seconds"],
          )
        )
      queue_workers.append(
        MetricSample(
          labels=(backend_alias, queue["name"]),
          value=queue["live_worker_count"],
        )
      )

    for status in ("live", "stale"):
      runner_processes.append(
        MetricSample(
          labels=(backend_alias, status),
          value=runner_metrics[status],
        )
      )
    for kind, counts in sorted(runner_metrics["by_kind"].items()):
      for status in ("live", "stale"):
        runner_processes_by_kind.append(
          MetricSample(
            labels=(backend_alias, kind, status),
            value=counts.get(status, 0),
          )
        )

    recurring_tasks.append(
      MetricSample(
        labels=(backend_alias,),
        value=len(snapshot["recurring_rows"]),
      )
    )
    process_rows.append(
      MetricSample(
        labels=(backend_alias,),
        value=len(snapshot["process_rows"]),
      )
    )

    if queue_database_alias in seen_queue_databases:
      continue
    seen_queue_databases.add(queue_database_alias)
    semaphores.append(
      MetricSample(
        labels=(queue_database_alias,),
        value=len(snapshot["semaphore_rows"]),
      )
    )

  return (
    MetricFamily(
      name="dj_queue_queue_jobs",
      help_text="Current job count by backend, queue, and state",
      labels=("backend", "queue", "state"),
      samples=tuple(queue_jobs),
    ),
    MetricFamily(
      name="dj_queue_queue_paused",
      help_text="Whether a queue is paused for a backend",
      labels=("backend", "queue"),
      samples=tuple(queue_paused),
    ),
    MetricFamily(
      name="dj_queue_queue_latency_seconds",
      help_text="Latency of the oldest ready job in a backend queue",
      labels=("backend", "queue"),
      samples=tuple(queue_latency),
    ),
    MetricFamily(
      name="dj_queue_queue_live_workers",
      help_text="Live workers that can service a backend queue",
      labels=("backend", "queue"),
      samples=tuple(queue_workers),
    ),
    MetricFamily(
      name="dj_queue_runner_processes",
      help_text="Current runner process count by backend and liveness",
      labels=("backend", "status"),
      samples=tuple(runner_processes),
    ),
    MetricFamily(
      name="dj_queue_runner_processes_by_kind",
      help_text="Current runner process count by backend, kind, and liveness",
      labels=("backend", "kind", "status"),
      samples=tuple(runner_processes_by_kind),
    ),
    MetricFamily(
      name="dj_queue_recurring_tasks",
      help_text="Current recurring task count by backend",
      labels=("backend",),
      samples=tuple(recurring_tasks),
    ),
    MetricFamily(
      name="dj_queue_semaphores",
      help_text="Current semaphore count by queue database",
      labels=("queue_database",),
      samples=tuple(semaphores),
    ),
    MetricFamily(
      name="dj_queue_process_rows",
      help_text="Current process row count by backend",
      labels=("backend",),
      samples=tuple(process_rows),
    ),
  )
