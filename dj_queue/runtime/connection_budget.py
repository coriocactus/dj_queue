import logging
from dataclasses import dataclass

from django.db import connections

from dj_queue.db import database_capabilities, supports_listen_notify
from dj_queue.log import log_event
from dj_queue.sql import backend_sql


@dataclass(frozen=True, slots=True)
class PostgresConnectionCapacity:
  max_connections: int
  reserved_connections: int

  @property
  def available_connections(self):
    return max(0, self.max_connections - self.reserved_connections)


def persistent_connections_enabled(value):
  if value is None:
    return True
  return int(value) > 0


def estimate_persistent_connections(config):
  workers = sum(worker.processes for worker in config.workers)
  runners = workers + len(config.dispatchers) + (config.scheduler is not None)
  worker_threads = sum(worker.processes * worker.threads for worker in config.workers)
  listeners = 0
  if config.listen_notify and supports_listen_notify(config.database_alias):
    listeners = workers
  # each runner and the supervisor hold one connection per poll and heartbeat thread
  return 2 * runners + worker_threads + listeners + 2


def postgres_connection_capacity(alias):
  if database_capabilities(alias).backend_family != "postgresql":
    return None

  max_connections, reserved_connections = backend_sql(alias).connection_capacity(alias)
  return PostgresConnectionCapacity(
    max_connections=max_connections,
    reserved_connections=reserved_connections,
  )


def warn_if_persistent_connection_budget_is_tight(
  config,
  *,
  threshold=0.75,
):
  connection = connections[config.database_alias]
  if not persistent_connections_enabled(connection.settings_dict.get("CONN_MAX_AGE", 0)):
    return None

  try:
    capacity = postgres_connection_capacity(config.database_alias)
  except Exception:
    return None

  if capacity is None:
    return None

  estimated_connections = estimate_persistent_connections(config)
  usage_ratio = estimated_connections / max(capacity.available_connections, 1)
  if usage_ratio < threshold:
    return None

  log_event(
    "connection_budget.warning",
    level=logging.WARNING,
    backend_alias=config.backend_alias,
    database_alias=config.database_alias,
    estimated_worker_connections=estimated_connections,
    available_database_connections=capacity.available_connections,
    max_connections=capacity.max_connections,
    reserved_connections=capacity.reserved_connections,
    threshold=threshold,
  )
  return usage_ratio
