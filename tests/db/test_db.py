import os
from datetime import timedelta
from types import SimpleNamespace

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.db import connections
from django.utils import timezone

from dj_queue.db import (
  database_capabilities,
  get_database_alias,
  get_queue_connection,
  supports_listen_notify,
  supports_skip_locked,
)
from dj_queue.models import RecurringExecution
from dj_queue.operations._insert import create_ignore_conflicts


@pytest.mark.postgres
def test_postgres_capabilities_enable_skip_locked_and_notify(django_db_blocker):
  with django_db_blocker.unblock():
    capabilities = database_capabilities("default")

  assert capabilities.backend_family == "postgresql"
  assert capabilities.supports_skip_locked is True
  assert capabilities.supports_listen_notify is True
  assert capabilities.uses_serialized_writes is False
  assert supports_skip_locked("default") is True
  assert supports_listen_notify("default") is True


@pytest.mark.mysql
def test_mysql_8_capabilities_enable_skip_locked_without_notify(django_db_blocker):
  with django_db_blocker.unblock():
    capabilities = database_capabilities("default")

  assert capabilities.backend_family == "mysql"
  assert capabilities.supports_skip_locked is True
  assert capabilities.supports_listen_notify is False
  assert capabilities.uses_serialized_writes is False


@pytest.mark.mariadb
def test_mariadb_10_6_capabilities_enable_skip_locked_without_notify(django_db_blocker):
  with django_db_blocker.unblock():
    capabilities = database_capabilities("default")

  assert capabilities.backend_family == "mariadb"
  assert capabilities.supports_skip_locked is True
  assert capabilities.supports_listen_notify is False
  assert capabilities.uses_serialized_writes is False


@pytest.mark.skipif(
  os.environ.get("DB_BACKEND", "sqlite") != "sqlite",
  reason="requires DB_BACKEND=sqlite",
)
def test_sqlite_capabilities_disable_skip_locked_and_notify():
  capabilities = database_capabilities("default")

  assert capabilities.backend_family == "sqlite"
  assert capabilities.supports_skip_locked is False
  assert capabilities.supports_listen_notify is False
  assert capabilities.uses_serialized_writes is True
  assert supports_skip_locked("default") is False
  assert supports_listen_notify("default") is False


def test_unsupported_database_vendor_is_rejected(monkeypatch):
  class FakeConnection:
    vendor = "oracle"

  monkeypatch.setattr("dj_queue.db.connections", {"default": FakeConnection()})

  with pytest.raises(ImproperlyConfigured, match="unsupported database vendor 'oracle'"):
    database_capabilities("default")


def test_queue_helpers_use_configured_database_alias(settings, monkeypatch):
  class FakeConnection:
    def __init__(self, alias):
      self.alias = alias

  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {"database_alias": "queue"},
    }
  }
  monkeypatch.setattr("dj_queue.db.connections", {"queue": FakeConnection("queue")})

  assert get_database_alias() == "queue"
  assert get_queue_connection().alias == "queue"


def test_mysql_create_ignore_conflicts_uses_duplicate_key_noop(monkeypatch):
  statements = []

  class FakeCursor:
    lastrowid = 0

    def __enter__(self):
      return self

    def __exit__(self, *_args):
      return None

    def execute(self, sql, params):
      statements.append((sql, params))

  class FakeConnection:
    ops = connections["default"].ops

    def cursor(self):
      return FakeCursor()

  monkeypatch.setattr("dj_queue.operations._insert.connections", {"default": FakeConnection()})
  monkeypatch.setattr(
    "dj_queue.operations._insert.database_capabilities",
    lambda using: SimpleNamespace(backend_family="mysql"),
  )

  created = create_ignore_conflicts(
    RecurringExecution,
    using="default",
    backend_alias="default",
    task_key="nightly",
    run_at=timezone.now() - timedelta(minutes=1),
  )

  sql, _params = statements[0]
  assert created is False
  assert "INSERT IGNORE" not in sql
  assert "ON DUPLICATE KEY UPDATE" in sql
  assert "LAST_INSERT_ID(0)" in sql
