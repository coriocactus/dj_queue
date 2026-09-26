from dataclasses import dataclass

from dj_queue import observability
from dj_queue.queue_state import QUEUE_STATE_DEFINITIONS


@dataclass(frozen=True, slots=True)
class MetricSample:
  labels: tuple[str, ...]
  value: float


@dataclass(frozen=True, slots=True)
class MetricFamily:
  name: str
  help_text: str
  metric_type: str
  labels: tuple[str, ...]
  samples: tuple[MetricSample, ...]


def metric_families(*, snapshots=None):
  if snapshots is None:
    snapshots = observability.all_backend_snapshots()
  snapshots = tuple(snapshots)

  return (
    *_queue_metric_families(snapshots),
    *_runner_metric_families(snapshots),
    MetricFamily(
      name="dj_queue_recurring_tasks",
      help_text="Current recurring task count by backend",
      metric_type="gauge",
      labels=("backend",),
      samples=tuple(
        MetricSample(labels=(snapshot.backend_alias,), value=len(snapshot.recurring_rows))
        for snapshot in snapshots
      ),
    ),
    _semaphore_metric_family(snapshots),
    MetricFamily(
      name="dj_queue_process_rows",
      help_text="Current process row count by backend",
      metric_type="gauge",
      labels=("backend",),
      samples=tuple(
        MetricSample(labels=(snapshot.backend_alias,), value=len(snapshot.process_rows))
        for snapshot in snapshots
      ),
    ),
    *_failed_job_metric_families(snapshots),
  )


def _queue_metric_families(snapshots):
  jobs = []
  paused = []
  latency = []
  workers = []
  for snapshot in snapshots:
    for queue in snapshot.queue_rows:
      labels = (snapshot.backend_alias, queue["name"])
      for definition in QUEUE_STATE_DEFINITIONS:
        jobs.append(
          MetricSample(labels=(*labels, definition.name), value=queue[definition.count_key])
        )
      paused.append(MetricSample(labels=labels, value=1 if queue["paused"] else 0))
      if queue["latency_seconds"] is not None:
        latency.append(MetricSample(labels=labels, value=queue["latency_seconds"]))
      workers.append(MetricSample(labels=labels, value=queue["live_worker_count"]))

  return (
    MetricFamily(
      name="dj_queue_queue_jobs",
      help_text="Current job count by backend, queue, and state",
      metric_type="gauge",
      labels=("backend", "queue", "state"),
      samples=tuple(jobs),
    ),
    MetricFamily(
      name="dj_queue_queue_paused",
      help_text="Whether a queue is paused for a backend",
      metric_type="gauge",
      labels=("backend", "queue"),
      samples=tuple(paused),
    ),
    MetricFamily(
      name="dj_queue_queue_latency_seconds",
      help_text="Latency of the oldest ready job in a backend queue",
      metric_type="gauge",
      labels=("backend", "queue"),
      samples=tuple(latency),
    ),
    MetricFamily(
      name="dj_queue_queue_live_workers",
      help_text="Live workers that can service a backend queue",
      metric_type="gauge",
      labels=("backend", "queue"),
      samples=tuple(workers),
    ),
  )


def _runner_metric_families(snapshots):
  processes = []
  processes_by_kind = []
  for snapshot in snapshots:
    backend_alias = snapshot.backend_alias
    runner_metrics = snapshot.runner_metrics
    for status in ("live", "stale"):
      processes.append(MetricSample(labels=(backend_alias, status), value=runner_metrics[status]))
    for kind, counts in sorted(runner_metrics["by_kind"].items()):
      for status in ("live", "stale"):
        processes_by_kind.append(
          MetricSample(labels=(backend_alias, kind, status), value=counts.get(status, 0))
        )

  return (
    MetricFamily(
      name="dj_queue_runner_processes",
      help_text="Current runner process count by backend and liveness",
      metric_type="gauge",
      labels=("backend", "status"),
      samples=tuple(processes),
    ),
    MetricFamily(
      name="dj_queue_runner_processes_by_kind",
      help_text="Current runner process count by backend, kind, and liveness",
      metric_type="gauge",
      labels=("backend", "kind", "status"),
      samples=tuple(processes_by_kind),
    ),
  )


def _semaphore_metric_family(snapshots):
  samples = []
  seen_queue_databases = set()
  for snapshot in snapshots:
    alias = snapshot.queue_database_alias
    if alias in seen_queue_databases:
      continue
    seen_queue_databases.add(alias)
    samples.append(MetricSample(labels=(alias,), value=len(snapshot.semaphore_rows)))

  return MetricFamily(
    name="dj_queue_semaphores",
    help_text="Current semaphore count by queue database",
    metric_type="gauge",
    labels=("queue_database",),
    samples=tuple(samples),
  )


def _failed_job_metric_families(snapshots):
  jobs = []
  oldest_age = []
  retention = []
  over_retention = []
  for snapshot in snapshots:
    metrics = snapshot.failed_metrics
    if metrics is None:
      continue
    labels = (snapshot.backend_alias,)
    jobs.append(MetricSample(labels=labels, value=metrics["count"]))
    if metrics["oldest_age_seconds"] is not None:
      oldest_age.append(MetricSample(labels=labels, value=metrics["oldest_age_seconds"]))
    if metrics["retention_seconds"] is not None:
      retention.append(MetricSample(labels=labels, value=metrics["retention_seconds"]))
      over_retention.append(MetricSample(labels=labels, value=metrics["over_retention_count"]))

  return (
    MetricFamily(
      name="dj_queue_failed_jobs",
      help_text="Current failed job count by backend",
      metric_type="gauge",
      labels=("backend",),
      samples=tuple(jobs),
    ),
    MetricFamily(
      name="dj_queue_failed_job_oldest_age_seconds",
      help_text="Age of the oldest failed job by backend",
      metric_type="gauge",
      labels=("backend",),
      samples=tuple(oldest_age),
    ),
    MetricFamily(
      name="dj_queue_failed_job_retention_seconds",
      help_text="Configured failed-job retention window by backend",
      metric_type="gauge",
      labels=("backend",),
      samples=tuple(retention),
    ),
    MetricFamily(
      name="dj_queue_failed_jobs_over_retention",
      help_text="Current failed job count older than configured retention by backend",
      metric_type="gauge",
      labels=("backend",),
      samples=tuple(over_retention),
    ),
  )
