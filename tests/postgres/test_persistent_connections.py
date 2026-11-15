import gc
import threading

import pytest
from django.db import connection, connections

from dj_queue.models import Job, Process
from dj_queue.runtime.connection_budget import estimate_persistent_connections
from dj_queue.runtime.supervisor import AsyncSupervisor
from tests.factories import wait_until
from tests.tasks import sleep_for

pytestmark = [pytest.mark.postgres, pytest.mark.django_db(transaction=True)]


def other_database_connections():
  with connection.cursor() as cursor:
    cursor.execute(
      "SELECT count(*) FROM pg_stat_activity "
      "WHERE datname = current_database() AND pid <> pg_backend_pid()"
    )
    return cursor.fetchone()[0]


@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_persistent_connection_estimate_matches_a_running_async_supervisor(monkeypatch):
  monkeypatch.setitem(connections["default"].settings_dict, "CONN_MAX_AGE", 60)
  tasks_settings = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {
        "mode": "async",
        "workers": [{"queues": "*", "threads": 3, "polling_interval": 0.05}] * 2,
        "dispatchers": [{"polling_interval": 0.05}],
        "scheduler": {"dynamic_tasks_enabled": True, "polling_interval": 0.05},
        "process_heartbeat_interval": 0.05,
        "shutdown_timeout": 5,
      },
    }
  }
  supervisor = AsyncSupervisor.from_backend_config(tasks_settings=tasks_settings, standalone=False)
  baseline = other_database_connections()
  thread = threading.Thread(target=supervisor.run)
  thread.start()

  try:
    wait_until(lambda: Process.objects.count() == 5)
    # two rounds so every worker thread has run a job and kept its connection
    for _round in range(2):
      for _job in range(6):
        sleep_for.enqueue(0.2)
      wait_until(lambda: Job.objects.filter(finished_at__isnull=True).exists() is False)

    estimate = estimate_persistent_connections(supervisor.config)
    assert estimate == 18
    wait_until(lambda: other_database_connections() - baseline == estimate)
  finally:
    supervisor.request_stop()
    thread.join()

  # dead runtime threads leave their persistent connections to the garbage collector
  wait_until(lambda: gc.collect() is not None and other_database_connections() == baseline)
