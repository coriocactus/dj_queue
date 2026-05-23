from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Literal

from django.core.exceptions import ImproperlyConfigured
from django.db import DEFAULT_DB_ALIAS, connections

from dj_queue.config import load_backend_config


@dataclass(frozen=True, slots=True)
class DatabaseCapabilities:
  backend_family: Literal["postgresql", "mysql", "mariadb", "sqlite"]
  supports_skip_locked: bool
  supports_listen_notify: bool
  uses_serialized_writes: bool


def get_database_alias(backend_alias: str = "default") -> str:
  return load_backend_config(backend_alias).database_alias


def locked_queryset(qs, use_skip_locked: bool = True):
  alias = getattr(qs, "db", DEFAULT_DB_ALIAS)
  connection = connections[alias]
  select_for_update_kwargs = {}
  if use_skip_locked and supports_skip_locked(alias):
    select_for_update_kwargs["skip_locked"] = True
  if getattr(connection.features, "has_select_for_update_of", False):
    select_for_update_kwargs["of"] = ("self",)
  return qs.select_for_update(**select_for_update_kwargs)


def database_capabilities(alias: str) -> DatabaseCapabilities:
  connection = connections[alias]
  backend_family = _backend_family(connection)
  supports_skip_locked_flag = bool(connection.features.has_select_for_update_skip_locked)

  return DatabaseCapabilities(
    backend_family=backend_family,
    supports_skip_locked=supports_skip_locked_flag,
    supports_listen_notify=backend_family == "postgresql",
    uses_serialized_writes=backend_family == "sqlite",
  )


def supports_skip_locked(alias: str) -> bool:
  return database_capabilities(alias).supports_skip_locked


def supports_listen_notify(alias: str) -> bool:
  return database_capabilities(alias).supports_listen_notify


def get_queue_connection(backend_alias: str = "default"):
  return connections[get_database_alias(backend_alias)]


@contextmanager
def queue_cursor(backend_alias: str = "default") -> Iterator:
  with get_queue_connection(backend_alias).cursor() as cursor:
    yield cursor


def _backend_family(connection) -> Literal["postgresql", "mysql", "mariadb", "sqlite"]:
  if connection.vendor == "postgresql":
    return "postgresql"
  if connection.vendor == "sqlite":
    return "sqlite"
  if connection.vendor == "mysql" and getattr(connection, "mysql_is_mariadb", False):
    return "mariadb"
  if connection.vendor == "mysql":
    return "mysql"
  raise ImproperlyConfigured(
    f"dj_queue unsupported database vendor {connection.vendor!r}; "
    "supported vendors are 'postgresql', 'mysql', and 'sqlite'"
  )
