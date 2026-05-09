import pytest
import sys
from types import SimpleNamespace

from benchmarks.harness import (
  assert_persistent_connection_budget,
  ensure_database_exists,
  parse_sizes,
)


def test_parse_sizes_uses_default_for_empty_value():
  assert parse_sizes(None, default=[100]) == [100]
  assert parse_sizes("", default=[100]) == [100]


def test_parse_sizes_rejects_non_positive_values():
  with pytest.raises(ValueError, match="positive"):
    parse_sizes("100,0", default=[100])


def test_assert_persistent_connection_budget_allows_estimate_below_capacity():
  assert (
    assert_persistent_connection_budget(
      estimated_connections=74,
      available_connections=97,
    )
    is None
  )


def test_assert_persistent_connection_budget_rejects_estimate_at_capacity():
  with pytest.raises(RuntimeError, match="estimated 97 worker connections"):
    assert_persistent_connection_budget(
      estimated_connections=97,
      available_connections=97,
    )


@pytest.mark.parametrize(
  ("backend", "expected_port"),
  (("mysql", 17312), ("mariadb", 17306)),
)
def test_ensure_database_exists_creates_mysql_family_database(monkeypatch, backend, expected_port):
  calls = []

  class FakeCursor:
    def __enter__(self):
      return self

    def __exit__(self, exc_type, exc, tb):
      return False

    def execute(self, sql):
      calls.append(sql)

  class FakeConnection:
    def __enter__(self):
      return self

    def __exit__(self, exc_type, exc, tb):
      return False

    def cursor(self):
      return FakeCursor()

  def connect(**kwargs):
    calls.append(kwargs)
    return FakeConnection()

  monkeypatch.setitem(sys.modules, "pymysql", SimpleNamespace(connect=connect))
  monkeypatch.setenv("BENCHMARK_DB_NAME", "dj_queue_benchmark`quoted")
  monkeypatch.delenv("BENCHMARK_DB_USER", raising=False)
  monkeypatch.delenv("BENCHMARK_DB_PASSWORD", raising=False)
  monkeypatch.delenv("BENCHMARK_DB_HOST", raising=False)
  monkeypatch.delenv("BENCHMARK_DB_PORT", raising=False)

  ensure_database_exists(backend)

  assert calls[0] == {
    "user": "root",
    "password": "root",
    "host": "127.0.0.1",
    "port": expected_port,
    "database": "mysql",
    "autocommit": True,
  }
  assert calls[1] == (
    "CREATE DATABASE IF NOT EXISTS `dj_queue_benchmark``quoted` "
    "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
  )


def test_ensure_database_exists_rejects_unsafe_mysql_family_name(monkeypatch):
  monkeypatch.setenv("BENCHMARK_DB_NAME", "production")

  with pytest.raises(RuntimeError, match="without 'benchmark'"):
    ensure_database_exists("mysql")
