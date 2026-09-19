import runpy
from unittest.mock import Mock

import pytest
from django.db import connection
from django.db.utils import OperationalError

from benchmarks import prerelease_runtime, prerelease_tasks


def test_prerelease_supervision_uses_normal_management_command(monkeypatch):
  command = Mock()
  monkeypatch.setattr("django.core.management.call_command", command)

  prerelease_runtime.supervise()

  command.assert_called_once_with("dj_queue", mode="async")


def test_prerelease_runtime_rejects_unsafe_database_name(monkeypatch):
  monkeypatch.setenv("PRERELEASE_DB_NAME", "production")

  with pytest.raises(RuntimeError, match="must contain 'prerelease'"):
    prerelease_runtime.assert_prerelease_database_name()


def test_prerelease_runtime_retries_migration_lock_conflict(monkeypatch):
  command = Mock(side_effect=[OperationalError(1205, "Lock wait timeout exceeded"), None])
  monkeypatch.setattr("django.core.management.call_command", command)
  monkeypatch.setattr(prerelease_runtime, "_set_migration_lock_timeout", lambda _connection: None)
  monkeypatch.setattr(prerelease_runtime.time, "sleep", lambda _seconds: None)

  prerelease_runtime.migrate()

  assert command.call_count == 2


def test_prerelease_runtime_does_not_retry_other_migration_errors(monkeypatch):
  command = Mock(side_effect=OperationalError("invalid DDL"))
  monkeypatch.setattr("django.core.management.call_command", command)
  monkeypatch.setattr(prerelease_runtime, "_set_migration_lock_timeout", lambda _connection: None)

  with pytest.raises(OperationalError, match="invalid DDL"):
    prerelease_runtime.migrate()

  assert command.call_count == 1


def test_prerelease_runtime_refuses_to_replace_existing_sqlite_database(monkeypatch, tmp_path):
  database = tmp_path / "prerelease-existing.sqlite3"
  database.write_text("keep", encoding="utf-8")
  monkeypatch.setenv("PRERELEASE_BACKEND", "sqlite")
  monkeypatch.setenv("PRERELEASE_DB_NAME", str(database))

  with pytest.raises(FileExistsError):
    prerelease_runtime.create_database()

  assert database.read_text(encoding="utf-8") == "keep"


def test_prerelease_settings_route_version_witnesses(monkeypatch, tmp_path):
  monkeypatch.setenv("PRERELEASE_BACKEND", "sqlite")
  monkeypatch.setenv("PRERELEASE_DB_NAME", str(tmp_path / "prerelease.sqlite3"))
  monkeypatch.setenv("PRERELEASE_RUNTIME_LABEL", "Y")

  settings = runpy.run_path(prerelease_runtime.__file__.replace("runtime.py", "settings.py"))

  assert settings["DATABASES"]["default"]["OPTIONS"]["transaction_mode"] == "IMMEDIATE"
  assert settings["TASKS"]["default"]["OPTIONS"]["workers"][0]["queues"] == ["y", "shared"]


@pytest.fixture
def ledger(transactional_db):
  prerelease_runtime.create_control_tables()
  yield
  with connection.cursor() as cursor:
    cursor.execute("DROP TABLE dj_queue_prerelease_effects")


def test_prerelease_tasks_count_retries_and_duplicates(ledger, monkeypatch):
  monkeypatch.setenv("PRERELEASE_RUNTIME_LABEL", "X")
  prerelease_runtime.expect_tokens(["batch:immediate", "batch:retry"])

  prerelease_tasks.record.func("batch:immediate")
  prerelease_tasks.record.func("batch:immediate")
  with pytest.raises(RuntimeError, match="expected prerelease failure"):
    prerelease_tasks.fail_once.func("batch:retry")
  prerelease_tasks.fail_once.func("batch:retry")

  assert prerelease_runtime.status()["effects"] == {
    "batch:immediate": [2, 2, "X"],
    "batch:retry": [2, 1, "X"],
  }


def test_prerelease_requires_every_planned_effect_and_reservation(ledger, monkeypatch):
  monkeypatch.setenv("PRERELEASE_RUNTIME_LABEL", "X")
  prerelease_runtime.expect_tokens(["old:immediate"])
  prerelease_tasks.record.func("old:immediate")

  problems = prerelease_runtime.batch_problems(prerelease_runtime.status(), "old", "X")

  assert any("old:recurring" in problem for problem in problems)
  assert any("old:scheduled" in problem for problem in problems)
  assert any("reservation" in problem for problem in problems)


@pytest.mark.parametrize(
  "fault", [None, "wrong-worker", "duplicate", "bad-retry", "no-reservation"]
)
def test_prerelease_batch_checks_outcomes(fault):
  snapshot = {
    "effects": {
      f"old:{name}": [2 if name == "retry" else 1, 1, "X"]
      for name in (
        "immediate",
        "scheduled",
        "limited-0",
        "limited-1",
        "bulk-0",
        "bulk-1",
        "retry",
        "recurring",
      )
    },
    "recurring": {"old:recurring": "a-job-id"},
  }
  if fault == "wrong-worker":
    snapshot["effects"]["old:immediate"][2] = "Y"
  elif fault == "duplicate":
    snapshot["effects"]["old:bulk-1"] = [2, 2, "X"]
  elif fault == "bad-retry":
    snapshot["effects"]["old:retry"][0] = 1
  elif fault == "no-reservation":
    snapshot["recurring"] = {}

  assert bool(prerelease_runtime.batch_problems(snapshot, "old", "X")) is (fault is not None)


def test_prerelease_plans_work_before_enqueue(ledger, monkeypatch):
  import sys

  monkeypatch.setitem(sys.modules, "prerelease_tasks", prerelease_tasks)
  record = Mock()
  record.using.side_effect = RuntimeError("enqueue")
  monkeypatch.setattr(prerelease_tasks, "record", record)

  with pytest.raises(RuntimeError, match="enqueue"):
    prerelease_runtime.enqueue_batch("old", "x")

  effects = prerelease_runtime.status()["effects"]
  assert len(effects) == 8
  assert effects["old:recurring"] == [0, 0, ""]
  assert effects["old:scheduled"] == [0, 0, ""]
