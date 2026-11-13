from contextlib import contextmanager

import pytest
from django.db import connection, transaction
from django.test.utils import CaptureQueriesContext

from dj_queue.exceptions import EnqueueError
from dj_queue.models import ClaimedExecution, ReadyExecution
from dj_queue.operations.claiming import claim_ready_jobs
from tests.tasks import echo

pytestmark = [pytest.mark.postgres, pytest.mark.django_db(transaction=True)]

READY_ONLY_SELECTORS = ("*", ("alpha", "beta"), ("alpha", "*"))


@contextmanager
def autocommit_off():
  transaction.set_autocommit(False)
  try:
    yield
  finally:
    transaction.rollback()
    transaction.set_autocommit(True)


def statements(ctx):
  return [query["sql"] for query in ctx.captured_queries]


def locking_ready_selects(ctx):
  return [
    sql for sql in statements(ctx) if "FOR UPDATE" in sql and "dj_queue_ready_executions" in sql
  ]


def enable_sort():
  with connection.cursor() as cursor:
    cursor.execute("SHOW enable_sort")
    return cursor.fetchone()[0]


@pytest.mark.parametrize("queues", READY_ONLY_SELECTORS)
def test_claim_owning_its_transaction_disables_sort_for_that_transaction_only(queues):
  echo.using(queue_name="alpha").enqueue("job")

  with CaptureQueriesContext(connection) as ctx:
    claimed_jobs = claim_ready_jobs(limit=1, queues=queues)

  assert len(claimed_jobs) == 1
  assert statements(ctx)[:2] == ["BEGIN", "SET LOCAL enable_sort = off"]
  assert enable_sort() == "on"


@pytest.mark.parametrize("caller_transaction", [transaction.atomic, autocommit_off])
@pytest.mark.parametrize("queues", READY_ONLY_SELECTORS)
def test_claim_inside_caller_transaction_leaves_enable_sort_untouched(caller_transaction, queues):
  echo.using(queue_name="alpha").enqueue("job")

  with caller_transaction(), CaptureQueriesContext(connection) as ctx:
    claimed_jobs = claim_ready_jobs(limit=1, queues=queues)
    setting_after_claim = enable_sort()

  assert len(claimed_jobs) == 1
  assert setting_after_claim == "on"
  assert not any("SET LOCAL" in sql for sql in statements(ctx))


@pytest.mark.parametrize(
  ("queues", "claimed_queue_names"),
  [
    (("al*",), ["alpha"]),
    (("be*", "*"), ["beta", "alpha"]),
    (("beta", "al*"), ["beta", "alpha"]),
    (("beta", "*", "al*"), ["beta", "alpha"]),
  ],
)
def test_prefix_wildcard_claim_keeps_joined_statements_without_the_setting(
  queues, claimed_queue_names
):
  for queue_name in ("alpha", "beta"):
    echo.using(queue_name=queue_name).enqueue(queue_name)

  with CaptureQueriesContext(connection) as ctx:
    claimed_jobs = claim_ready_jobs(limit=2, queues=queues)

  assert [claimed_job.job.queue_name for claimed_job in claimed_jobs] == claimed_queue_names
  assert not any("SET LOCAL" in sql for sql in statements(ctx))
  job_reads = [sql for sql in statements(ctx) if "dj_queue_jobs" in sql]
  assert job_reads == locking_ready_selects(ctx)


@pytest.mark.parametrize(
  ("queues", "claimed_args"),
  [
    ("*", ["beta-9", "alpha-5", "alpha-0"]),
    (("alpha", "beta"), ["alpha-5", "alpha-0", "beta-9"]),
    (("alpha", "*"), ["alpha-5", "alpha-0", "beta-9"]),
  ],
)
def test_claim_locks_ready_rows_without_jobs_then_reads_jobs_by_primary_key(queues, claimed_args):
  for queue_name, priority in (("alpha", 0), ("beta", 9), ("alpha", 5)):
    echo.using(queue_name=queue_name, priority=priority).enqueue(f"{queue_name}-{priority}")

  with CaptureQueriesContext(connection) as ctx:
    claimed_jobs = claim_ready_jobs(limit=3, queues=queues)

  assert [claimed_job.job.payload["args"][0] for claimed_job in claimed_jobs] == claimed_args
  locking_selects = locking_ready_selects(ctx)
  assert locking_selects
  assert not any("dj_queue_jobs" in sql for sql in locking_selects)
  job_reads = [sql for sql in statements(ctx) if "dj_queue_jobs" in sql]
  assert len(job_reads) == 1
  assert 'WHERE "dj_queue_jobs"."id" IN (' in job_reads[0]
  assert statements(ctx).index(job_reads[0]) > statements(ctx).index(locking_selects[-1])


@pytest.mark.parametrize("queues", ["*", ("default",), ("default", "*")])
def test_claim_rejects_selected_ready_row_without_job(queues):
  result = echo.enqueue("orphan")

  with transaction.atomic():
    # the foreign key is deferred, so the ready row outlives its job until the block ends
    with connection.cursor() as cursor:
      cursor.execute("DELETE FROM dj_queue_jobs WHERE id = %s", [result.id])
    with pytest.raises(EnqueueError, match=f"job {result.id} does not exist"):
      claim_ready_jobs(limit=1, queues=queues)
    ready_row_exists = ReadyExecution.objects.filter(job_id=result.id).exists()
    claimed_row_exists = ClaimedExecution.objects.exists()
    transaction.set_rollback(True)

  assert ready_row_exists is True
  assert claimed_row_exists is False
