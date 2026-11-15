from dataclasses import replace

import pytest
from django.db import connection
from django.test.utils import CaptureQueriesContext

from dj_queue.config import load_backend_config
from dj_queue.operations.claiming import claim_ready_jobs
from dj_queue.operations.enqueue import enqueue_job
from dj_queue.operations.execution import complete_claimed_job, fail_claimed_job
from tests.settings import DB_BACKEND
from tests.tasks import echo, limited

pytestmark = pytest.mark.django_db(transaction=True)

# statements each hot operation sends, excluding transaction control and
# notifications; keyed rows use tests.tasks.limited, whose claims and
# transitions run with a blocked waiter queued behind the claimed job
EXPECTED_STATEMENTS = {
  "enqueue": {"postgres": 2, "mysql": 2, "mariadb": 2, "sqlite": 2},
  "enqueue keyed": {"postgres": 3, "mysql": 3, "mariadb": 3, "sqlite": 3},
  "enqueue blocked": {"postgres": 3, "mysql": 3, "mariadb": 3, "sqlite": 5},
  "claim": {"postgres": 5, "mysql": 6, "mariadb": 6, "sqlite": 5},
  "claim keyed": {"postgres": 5, "mysql": 6, "mariadb": 6, "sqlite": 5},
  # postgres is pinned by tests/postgres/test_completion_statement.py
  "complete": {"mysql": 2, "mariadb": 2, "sqlite": 2},
  "complete keyed": {"postgres": 3, "mysql": 6, "mariadb": 6, "sqlite": 6},
  "complete unretained": {"postgres": 9, "mysql": 9, "mariadb": 9, "sqlite": 9},
  "complete unretained keyed": {"postgres": 11, "mysql": 13, "mariadb": 13, "sqlite": 13},
  "fail": {"postgres": 3, "mysql": 3, "mariadb": 3, "sqlite": 3},
  "fail keyed": {"postgres": 5, "mysql": 7, "mariadb": 7, "sqlite": 7},
}

TRANSACTION_CONTROL = ("BEGIN", "COMMIT", "SAVEPOINT", "RELEASE")


def enqueue(task, config):
  return enqueue_job(task, (1,) if task is limited else (), {}, config=config)


def prepare(operation, task, config):
  if operation == "enqueue":
    return lambda: enqueue(task, config)
  enqueue(task, config)
  if task is limited:
    enqueue(task, config)
  if operation == "claim":
    return lambda: claim_ready_jobs(limit=1, config=config)
  job = claim_ready_jobs(limit=1, config=config)[0].job
  if operation == "complete":
    return lambda: complete_claimed_job(job, "done", config=config, task=task)
  return lambda: fail_claimed_job(job, RuntimeError("boom"), config=config, task=task)


@pytest.mark.parametrize("case", EXPECTED_STATEMENTS)
def test_hot_operation_statement_budget(case):
  expected = EXPECTED_STATEMENTS[case].get(DB_BACKEND)
  if expected is None:
    pytest.skip(f"{case} is pinned elsewhere on {DB_BACKEND}")
  config = load_backend_config()
  if "unretained" in case:
    config = replace(config, preserve_finished_jobs=False)
  if "blocked" in case:
    enqueue(limited, config)
  task = limited if "keyed" in case or "blocked" in case else echo
  operation = prepare(case.split()[0], task, config)

  with CaptureQueriesContext(connection) as ctx:
    operation()

  statements = [
    query["sql"]
    for query in ctx.captured_queries
    if not query["sql"].startswith(TRANSACTION_CONTROL) and "pg_notify" not in query["sql"]
  ]
  assert len(statements) == expected, "\n".join(statements)
