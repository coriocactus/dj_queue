import os
from datetime import datetime

from django.utils import timezone

DEFAULT_SEEDS = [1, 7, 19]
DEFAULT_STEPS = 90


def simulation_seeds():
  configured = os.environ.get("SIM_SEEDS")
  if configured:
    return [int(value.strip()) for value in configured.split(",") if value.strip()]
  return DEFAULT_SEEDS


def simulation_steps():
  configured = os.environ.get("SIM_STEPS")
  if configured:
    return int(configured)
  return DEFAULT_STEPS


def fixed_now():
  return datetime(2026, 4, 8, 12, 0, 1, tzinfo=timezone.get_current_timezone())


def simulation_tasks_settings():
  return {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {
        "mode": "async",
        "workers": [{"queues": "*", "threads": 1, "processes": 1, "polling_interval": 0.01}],
        "dispatchers": [
          {
            "batch_size": 10,
            "polling_interval": 0.01,
            "concurrency_maintenance": True,
            "concurrency_maintenance_interval": 0,
          }
        ],
        "scheduler": {
          "dynamic_tasks_enabled": True,
          "polling_interval": 1,
        },
        "recurring": {},
        "process_heartbeat_interval": 0,
        "process_alive_threshold": 10_000_000,
        "preserve_finished_jobs": True,
        "clear_finished_jobs_after": None,
      },
    }
  }
