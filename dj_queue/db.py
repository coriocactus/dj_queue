import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Literal

from django.core.exceptions import ImproperlyConfigured
from django.db import DEFAULT_DB_ALIAS, connections
from django.db.utils import OperationalError

from dj_queue.config import load_backend_config

TRANSIENT_DATABASE_RETRY_ATTEMPTS = 3
TRANSIENT_DATABASE_RETRY_SLEEP_BASE = 0.01
TRANSIENT_DATABASE_ERROR_SQLSTATES = {
  "40001",  # serialization failure
  "40P01",  # deadlock detected
  "55P03",  # lock not available
}
TRANSIENT_DATABASE_ERROR_ERRNOS = {
  1205,  # mysql lock wait timeout
  1213,  # mysql deadlock
}
TRANSIENT_DATABASE_SQLITE_CODES = {
  5,  # SQLITE_BUSY
  6,  # SQLITE_LOCKED
  261,  # SQLITE_BUSY_RECOVERY
  262,  # SQLITE_LOCKED_SHAREDCACHE
  517,  # SQLITE_BUSY_SNAPSHOT
}
TRANSIENT_DATABASE_ERROR_MESSAGES = (
  "deadlock",
  "lock wait timeout",
  "try restarting transaction",
  "could not serialize access",
  "database is locked",
)


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


_CAPABILITIES_CACHE: dict[str, DatabaseCapabilities] = {}


def database_capabilities(alias: str) -> DatabaseCapabilities:
  cached = _CAPABILITIES_CACHE.get(alias)
  if cached is not None:
    return cached

  connection = connections[alias]
  backend_family = _backend_family(connection)
  supports_skip_locked_flag = bool(connection.features.has_select_for_update_skip_locked)

  capabilities = DatabaseCapabilities(
    backend_family=backend_family,
    supports_skip_locked=supports_skip_locked_flag,
    supports_listen_notify=backend_family == "postgresql",
    uses_serialized_writes=backend_family == "sqlite",
  )
  _CAPABILITIES_CACHE[alias] = capabilities
  return capabilities


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


def retry_transient_database_errors(operation):
  for attempt in range(TRANSIENT_DATABASE_RETRY_ATTEMPTS):
    try:
      return operation()
    except OperationalError as error:
      if attempt == TRANSIENT_DATABASE_RETRY_ATTEMPTS - 1 or not is_transient_database_error(
        error
      ):
        raise
      time.sleep(TRANSIENT_DATABASE_RETRY_SLEEP_BASE * (attempt + 1))


def is_transient_database_error(error):
  for candidate in _exception_chain(error):
    if _transient_sqlstate(candidate) or _transient_errno(candidate):
      return True
  message = str(error).lower()
  return any(marker in message for marker in TRANSIENT_DATABASE_ERROR_MESSAGES)


def _exception_chain(error):
  seen = set()
  while error is not None and id(error) not in seen:
    seen.add(id(error))
    yield error
    error = error.__cause__ or error.__context__


def _transient_sqlstate(error):
  for name in ("pgcode", "sqlstate"):
    value = getattr(error, name, None)
    if value in TRANSIENT_DATABASE_ERROR_SQLSTATES:
      return True
  return False


def _transient_errno(error):
  sqlite_code = getattr(error, "sqlite_errorcode", None)
  if sqlite_code in TRANSIENT_DATABASE_SQLITE_CODES:
    return True
  errno = getattr(error, "errno", None)
  if errno in TRANSIENT_DATABASE_ERROR_ERRNOS:
    return True
  return bool(error.args and error.args[0] in TRANSIENT_DATABASE_ERROR_ERRNOS)


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
