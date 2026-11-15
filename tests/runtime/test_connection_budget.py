import logging

import pytest

from dj_queue.config import BackendConfig, DispatcherConfig, SchedulerConfig, WorkerConfig
from dj_queue.runtime.connection_budget import (
  PostgresConnectionCapacity,
  estimate_persistent_connections,
  warn_if_persistent_connection_budget_is_tight,
)


class FakeConnection:
  def __init__(self, *, conn_max_age):
    self.settings_dict = {"CONN_MAX_AGE": conn_max_age}


@pytest.mark.parametrize(("listen_notify", "expected"), [(True, 57), (False, 52)])
def test_estimate_persistent_connections_counts_every_runtime_thread(
  monkeypatch, listen_notify, expected
):
  monkeypatch.setattr(
    "dj_queue.runtime.connection_budget.supports_listen_notify", lambda alias: True
  )
  config = BackendConfig(
    workers=(
      WorkerConfig(threads=8, processes=4),
      WorkerConfig(threads=2, processes=1),
    ),
    dispatchers=(DispatcherConfig(), DispatcherConfig()),
    scheduler=SchedulerConfig(),
    listen_notify=listen_notify,
  )

  # 8 runners with a poll and a heartbeat connection each, 34 worker threads,
  # one listener per worker, and the supervisor's poll and heartbeat connections
  assert estimate_persistent_connections(config) == expected


def test_estimate_persistent_connections_skips_listeners_without_notify_support(monkeypatch):
  monkeypatch.setattr(
    "dj_queue.runtime.connection_budget.supports_listen_notify", lambda alias: False
  )
  config = BackendConfig(workers=(WorkerConfig(threads=3, processes=2),))

  assert estimate_persistent_connections(config) == 16


def test_warn_if_persistent_connection_budget_is_tight_logs_warning(monkeypatch, caplog):
  config = BackendConfig(
    database_alias="queue",
    workers=(WorkerConfig(threads=8, processes=8),),
  )
  monkeypatch.setattr(
    "dj_queue.runtime.connection_budget.connections",
    {"queue": FakeConnection(conn_max_age=60)},
  )
  monkeypatch.setattr(
    "dj_queue.runtime.connection_budget.postgres_connection_capacity",
    lambda alias: PostgresConnectionCapacity(max_connections=100, reserved_connections=3),
  )
  monkeypatch.setattr(
    "dj_queue.runtime.connection_budget.supports_listen_notify", lambda alias: True
  )

  with caplog.at_level(logging.WARNING, logger="dj_queue"):
    ratio = warn_if_persistent_connection_budget_is_tight(config)

  assert ratio == pytest.approx(94 / 97)
  assert caplog.records[-1].event == "connection_budget.warning"
  assert caplog.records[-1].dj_queue == {
    "backend_alias": "default",
    "database_alias": "queue",
    "estimated_worker_connections": 94,
    "available_database_connections": 97,
    "max_connections": 100,
    "reserved_connections": 3,
    "threshold": 0.75,
  }


def test_warn_if_persistent_connection_budget_is_tight_skips_non_persistent_connections(
  monkeypatch,
):
  config = BackendConfig(database_alias="queue")
  monkeypatch.setattr(
    "dj_queue.runtime.connection_budget.connections",
    {"queue": FakeConnection(conn_max_age=0)},
  )

  def fail_capacity_lookup(alias):
    pytest.fail("capacity should not be loaded when persistent connections are disabled")

  monkeypatch.setattr(
    "dj_queue.runtime.connection_budget.postgres_connection_capacity",
    fail_capacity_lookup,
  )

  assert warn_if_persistent_connection_budget_is_tight(config) is None
