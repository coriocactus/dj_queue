import logging
from dataclasses import dataclass

from django.db import connections

from dj_queue.db import database_capabilities
from dj_queue.log import log_event


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


def estimate_persistent_worker_connections(*, worker_processes, worker_threads):
  return worker_processes + worker_threads + 2


def estimate_config_worker_connections(config):
  worker_processes = sum(worker.processes for worker in config.workers)
  worker_threads = sum(worker.processes * worker.threads for worker in config.workers)
  return estimate_persistent_worker_connections(
    worker_processes=worker_processes,
    worker_threads=worker_threads,
  )


def postgres_connection_capacity(alias):
  if database_capabilities(alias).backend_family != "postgresql":
    return None

  connection = connections[alias]
  with connection.cursor() as cursor:
    cursor.execute(
      "select current_setting('max_connections')::int, "
      "current_setting('superuser_reserved_connections')::int"
    )
    max_connections, reserved_connections = cursor.fetchone()
  return PostgresConnectionCapacity(
    max_connections=max_connections,
    reserved_connections=reserved_connections,
  )


def connection_budget_ratio(*, estimated_connections, capacity):
  if capacity.available_connections <= 0:
    return 1
  return estimated_connections / capacity.available_connections


def warn_if_persistent_connection_budget_is_tight(
  config,
  *,
  backend_alias="default",
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

  estimated_connections = estimate_config_worker_connections(config)
  usage_ratio = connection_budget_ratio(
    estimated_connections=estimated_connections,
    capacity=capacity,
  )
  if usage_ratio < threshold:
    return None

  log_event(
    "connection_budget.warning",
    level=logging.WARNING,
    backend_alias=backend_alias,
    database_alias=config.database_alias,
    estimated_worker_connections=estimated_connections,
    available_database_connections=capacity.available_connections,
    max_connections=capacity.max_connections,
    reserved_connections=capacity.reserved_connections,
    threshold=threshold,
  )
  return usage_ratio
