try:
  from prometheus_client import CollectorRegistry, generate_latest
  from prometheus_client.core import GaugeMetricFamily
except ImportError:
  DjQueueCollector = None
  registry = None
  generate_latest = None
else:
  from dj_queue import observability

  class DjQueueCollector:
    """Prometheus collector that exposes dj_queue metrics from the shared observability snapshot."""

    def collect(self):
      queue_jobs = GaugeMetricFamily(
        "dj_queue_queue_jobs",
        "Current job count by backend, queue, and state",
        labels=["backend", "queue", "state"],
      )
      queue_paused = GaugeMetricFamily(
        "dj_queue_queue_paused",
        "Whether a queue is paused for a backend",
        labels=["backend", "queue"],
      )
      queue_latency = GaugeMetricFamily(
        "dj_queue_queue_latency_seconds",
        "Latency of the oldest ready job in a backend queue",
        labels=["backend", "queue"],
      )
      queue_workers = GaugeMetricFamily(
        "dj_queue_queue_live_workers",
        "Live workers that can service a backend queue",
        labels=["backend", "queue"],
      )
      runner_processes = GaugeMetricFamily(
        "dj_queue_runner_processes",
        "Current runner process count by backend and liveness",
        labels=["backend", "status"],
      )
      runner_processes_by_kind = GaugeMetricFamily(
        "dj_queue_runner_processes_by_kind",
        "Current runner process count by backend, kind, and liveness",
        labels=["backend", "kind", "status"],
      )
      recurring_tasks = GaugeMetricFamily(
        "dj_queue_recurring_tasks",
        "Current recurring task count by backend",
        labels=["backend"],
      )
      semaphores = GaugeMetricFamily(
        "dj_queue_semaphores",
        "Current semaphore count by queue database",
        labels=["queue_database"],
      )
      process_rows = GaugeMetricFamily(
        "dj_queue_process_rows",
        "Current process row count by backend",
        labels=["backend"],
      )

      seen_queue_databases = set()
      for snapshot in observability.all_backend_snapshots():
        backend_alias = snapshot["backend_alias"]
        queue_database_alias = snapshot["queue_database_alias"]
        runner_metrics = snapshot["runner_metrics"]

        for queue in snapshot["queue_rows"]:
          for state in ("ready", "claimed", "scheduled", "blocked", "failed", "finished"):
            queue_jobs.add_metric(
              [backend_alias, queue["name"], state],
              queue[f"{state}_count"],
            )
          queue_paused.add_metric(
            [backend_alias, queue["name"]],
            1 if queue["paused"] else 0,
          )
          if queue["latency_seconds"] is not None:
            queue_latency.add_metric(
              [backend_alias, queue["name"]],
              queue["latency_seconds"],
            )
          queue_workers.add_metric(
            [backend_alias, queue["name"]],
            queue["live_worker_count"],
          )

        for status in ("live", "stale"):
          runner_processes.add_metric(
            [backend_alias, status],
            runner_metrics[status],
          )
        for kind, counts in sorted(runner_metrics["by_kind"].items()):
          for status in ("live", "stale"):
            runner_processes_by_kind.add_metric(
              [backend_alias, kind, status],
              counts.get(status, 0),
            )

        recurring_tasks.add_metric(
          [backend_alias],
          len(snapshot["recurring_rows"]),
        )
        process_rows.add_metric(
          [backend_alias],
          len(snapshot["process_rows"]),
        )

        if queue_database_alias in seen_queue_databases:
          continue
        seen_queue_databases.add(queue_database_alias)

        semaphores.add_metric(
          [queue_database_alias],
          len(snapshot["semaphore_rows"]),
        )

      yield queue_jobs
      yield queue_paused
      yield queue_latency
      yield queue_workers
      yield runner_processes
      yield runner_processes_by_kind
      yield recurring_tasks
      yield semaphores
      yield process_rows

  registry = CollectorRegistry(auto_describe=False)
  registry.register(DjQueueCollector())
