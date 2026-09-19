import os
import time

from django.db import connection, transaction
from django.tasks import task


def _record(token, *, completion):
  with transaction.atomic(), connection.cursor() as cursor:
    cursor.execute(
      "UPDATE dj_queue_prerelease_effects "
      "SET attempts = attempts + 1, completions = completions + %s, worker = %s "
      "WHERE token = %s",
      [int(completion), os.environ["PRERELEASE_RUNTIME_LABEL"], token],
    )
    if cursor.rowcount != 1:
      raise RuntimeError(f"unplanned side effect: {token}")
    cursor.execute("SELECT attempts FROM dj_queue_prerelease_effects WHERE token = %s", [token])
    return cursor.fetchone()[0]


@task
def record(token):
  _record(token, completion=True)
  return token


@task
def record_limited(queue, token):
  time.sleep(0.05)
  _record(token, completion=True)
  return token


record_limited.func.concurrency_key = "prerelease:{queue}"
record_limited.func.concurrency_limit = 1
record_limited.func.concurrency_duration = 60


@task
def fail_once(token):
  if _record(token, completion=False) == 1:
    raise RuntimeError("expected prerelease failure")
  with connection.cursor() as cursor:
    cursor.execute(
      "UPDATE dj_queue_prerelease_effects SET completions = completions + 1 WHERE token = %s",
      [token],
    )
  return token
