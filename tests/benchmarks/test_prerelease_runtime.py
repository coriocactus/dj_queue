import pytest
from django.db import connection

from benchmarks import prerelease_runtime, prerelease_tasks


def test_prerelease_runtime_parses_bounded_process_duration():
  args = prerelease_runtime.parse_args(
    ["produce", "--run-id", "release", "--rate", "25", "--duration", "10"]
  )

  assert args.run_id == "release"
  assert args.rate == 25
  assert args.duration == 10


def test_prerelease_runtime_rejects_unsafe_database_name(monkeypatch):
  monkeypatch.setenv("PRERELEASE_DB_NAME", "production")

  with pytest.raises(RuntimeError, match="must contain 'prerelease'"):
    prerelease_runtime.assert_prerelease_database_name()


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
