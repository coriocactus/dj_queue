import os

from django.db import connection
from django.tasks import task


def _table(name):
  return connection.ops.quote_name(name)


def _record(token, category, *, completion):
  table = _table("dj_queue_prerelease_effects")
  runtime_label = os.environ.get("PRERELEASE_RUNTIME_LABEL", "unknown")
  params = [token, category, int(completion), runtime_label, runtime_label]
  if connection.vendor == "postgresql":
    conflict_sql = (
      "ON CONFLICT (token) DO UPDATE SET "
      f"attempts = {table}.attempts + 1, "
      f"completions = {table}.completions + EXCLUDED.completions, "
      "last_version = EXCLUDED.last_version, "
      "completed_at = CASE WHEN EXCLUDED.completions = 1 "
      f"THEN EXCLUDED.completed_at ELSE {table}.completed_at END"
    )
  elif connection.vendor == "mysql":
    conflict_sql = (
      "ON DUPLICATE KEY UPDATE "
      "attempts = attempts + 1, "
      "completions = completions + %s, "
      "last_version = %s, "
      "completed_at = IF(%s = 1, CURRENT_TIMESTAMP, completed_at)"
    )
    params.extend((int(completion), runtime_label, int(completion)))
  else:
    conflict_sql = (
      "ON CONFLICT(token) DO UPDATE SET "
      "attempts = attempts + 1, "
      "completions = completions + excluded.completions, "
      "last_version = excluded.last_version, "
      "completed_at = CASE WHEN excluded.completions = 1 "
      "THEN excluded.completed_at ELSE completed_at END"
    )

  with connection.cursor() as cursor:
    cursor.execute(
      f"""
      INSERT INTO {table} (
        token, category, attempts, completions, first_version, last_version, completed_at
      ) VALUES (%s, %s, 1, %s, %s, %s, CURRENT_TIMESTAMP)
      {conflict_sql}
      """,
      params,
    )
    cursor.execute(f"SELECT attempts FROM {table} WHERE token = %s", [token])
    return cursor.fetchone()[0]


@task
def record(token, category="immediate"):
  _record(token, category, completion=True)
  return token


@task
def record_limited(account_id, token):
  _record(token, "concurrency", completion=True)
  return token


record_limited.func.concurrency_key = "prerelease:{account_id}"
record_limited.func.concurrency_limit = 2
record_limited.func.concurrency_duration = 60


@task
def fail_once(token):
  attempt = _record(token, "failure", completion=False)
  if attempt == 1:
    raise RuntimeError("expected prerelease failure")
  _complete_failed_attempt(token)
  return token


def _complete_failed_attempt(token):
  table = _table("dj_queue_prerelease_effects")
  runtime_label = os.environ.get("PRERELEASE_RUNTIME_LABEL", "unknown")
  with connection.cursor() as cursor:
    cursor.execute(
      f"""
      UPDATE {table}
      SET completions = completions + 1,
          last_version = %s,
          completed_at = CURRENT_TIMESTAMP
      WHERE token = %s
      """,
      [runtime_label, token],
    )


@task(takes_context=True)
def record_recurring(context, run_id):
  token = f"{run_id}:recurring:{context.task_result.id}"
  _record(token, "recurring", completion=True)
  return token
