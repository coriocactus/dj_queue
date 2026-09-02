import runpy

import pytest
from django.db import connection
from django.db.utils import OperationalError

from benchmarks import prerelease_runtime, prerelease_tasks


def test_prerelease_runtime_parses_bounded_process_duration():
  args = prerelease_runtime.parse_args(
    [
      "produce",
      "--run-id",
      "release",
      "--rate",
      "25",
      "--run-started-at",
      "100.5",
      "--duration",
      "10",
    ]
  )

  assert args.run_id == "release"
  assert args.rate == 25
  assert args.run_started_at == 100.5
  assert args.duration == 10


def test_prerelease_runtime_rejects_unsafe_database_name(monkeypatch):
  monkeypatch.setenv("PRERELEASE_DB_NAME", "production")

  with pytest.raises(RuntimeError, match="must contain 'prerelease'"):
    prerelease_runtime.assert_prerelease_database_name()


def test_prerelease_runtime_retries_migration_lock_conflict(monkeypatch):
  calls = 0

  def migrate_once_lock_is_available(*_args, **_kwargs):
    nonlocal calls
    calls += 1
    if calls == 1:
      raise OperationalError(1205, "Lock wait timeout exceeded")

  monkeypatch.setattr("django.core.management.call_command", migrate_once_lock_is_available)
  monkeypatch.setattr(prerelease_runtime.time, "sleep", lambda _seconds: None)

  prerelease_runtime.migrate()

  assert calls == 2


def test_prerelease_runtime_recognizes_postgres_lock_conflict():
  driver_error = Exception("canceling statement due to lock timeout")
  driver_error.sqlstate = "55P03"
  error = OperationalError("migration failed")
  error.__cause__ = driver_error

  assert prerelease_runtime._is_transient_migration_error(error) is True


def test_prerelease_runtime_sets_postgres_migration_lock_timeout():
  statements = []

  class Cursor:
    def __enter__(self):
      return self

    def __exit__(self, *_args):
      return None

    def execute(self, statement):
      statements.append(statement)

  database = type("Database", (), {"vendor": "postgresql", "cursor": lambda _self: Cursor()})()

  prerelease_runtime._set_migration_lock_timeout(database)

  assert statements == ["SET SESSION lock_timeout = '2s'"]


def test_prerelease_runtime_refuses_to_replace_existing_sqlite_database(monkeypatch, tmp_path):
  database = tmp_path / "prerelease-existing.sqlite3"
  database.write_text("keep", encoding="utf-8")
  monkeypatch.setenv("PRERELEASE_BACKEND", "sqlite")
  monkeypatch.setenv("PRERELEASE_DB_NAME", str(database))

  with pytest.raises(RuntimeError, match="already exists"):
    prerelease_runtime.create_database()

  assert database.read_text(encoding="utf-8") == "keep"


def test_prerelease_sqlite_serializes_write_transactions(monkeypatch, tmp_path):
  monkeypatch.setenv("PRERELEASE_BACKEND", "sqlite")
  monkeypatch.setenv("PRERELEASE_DB_NAME", str(tmp_path / "prerelease.sqlite3"))

  settings = runpy.run_path(prerelease_runtime.__file__.replace("runtime.py", "settings.py"))

  assert settings["DATABASES"]["default"]["OPTIONS"] == {
    "timeout": 30,
    "transaction_mode": "IMMEDIATE",
  }


@pytest.mark.django_db(transaction=True)
def test_prerelease_tasks_record_attempts_and_duplicate_completions(monkeypatch):
  monkeypatch.setenv("PRERELEASE_RUNTIME_LABEL", "X")
  prerelease_runtime.create_control_tables()

  assert prerelease_tasks.record.func("record-token") == "record-token"
  assert prerelease_tasks.record.func("record-token") == "record-token"
  with pytest.raises(RuntimeError, match="expected prerelease failure"):
    prerelease_tasks.fail_once.func("retry-token")
  assert prerelease_tasks.fail_once.func("retry-token") == "retry-token"

  table = connection.ops.quote_name("dj_queue_prerelease_effects")
  with connection.cursor() as cursor:
    cursor.execute(f"SELECT token, attempts, completions FROM {table} ORDER BY token")
    rows = cursor.fetchall()

  assert rows == [("record-token", 2, 2), ("retry-token", 2, 1)]
