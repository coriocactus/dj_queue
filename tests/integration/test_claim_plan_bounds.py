import json
import logging
import os
import re

import pytest
from django.db import connection, transaction
from django.utils import timezone

from dj_queue.config import load_backend_config
from dj_queue.models import Job, ReadyExecution
from dj_queue.operations.claiming import claim_ready_jobs
from dj_queue.operations.execution import complete_claimed_job
from tests.tasks import echo

logger = logging.getLogger("dj_queue.stress")

pytestmark = [
  pytest.mark.django_db(transaction=True),
  pytest.mark.postgres,
  pytest.mark.skipif(
    os.environ.get("STRESS") != "1",
    reason="run separately with STRESS=1",
  ),
]

LIMIT = 16
BACKLOG = 5_000
FINISHED_JOBS = 50_000
# a claim whose cost follows its limit measures 44 to 105 buffers here on PostgreSQL 14 to 18
# (50 to 180 in ADR 0023) and the cheapest one that scans this backlog measures 3,816, so the
# bound sits between them with room for drift across majors
BUFFER_BOUND = 1_000
QUEUES = ("default", "alpha", "beta", "gamma")
SPARSE_QUEUE = "sparse"
SPARSE_EVERY = BACKLOG // (LIMIT // 2)
SELECTOR_FORMS = {
  "all queues": "*",
  "exact list": ("alpha", "beta", "default"),
  # the sparse queue holds half a claim, so the exact select and the * tail both run
  "exact then all": (SPARSE_QUEUE, "*"),
}
TABLES = ("dj_queue_jobs", "dj_queue_ready_executions", "dj_queue_claimed_executions")
STATES = (
  "fresh statistics",
  "post-backlog",
  "uncommitted enqueue, finished jobs retained",
  "uncommitted enqueue, no finished jobs",
)


class Captured(Exception):
  pass


@pytest.fixture
def maintenance(transactional_db):
  # vacuum cannot run inside the enqueueing transaction, so it gets its own connection
  other = connection.copy()
  with other.cursor() as cursor:
    cursor.execute("TRUNCATE dj_queue_jobs CASCADE")
    for table in TABLES:
      cursor.execute(f"ALTER TABLE {table} SET (autovacuum_enabled = false)")
  try:
    yield other
  finally:
    with other.cursor() as cursor:
      for table in TABLES:
        cursor.execute(f"ALTER TABLE {table} RESET (autovacuum_enabled)")
    other.close()


@pytest.mark.parametrize("state", STATES)
def test_claim_cost_is_bounded_by_the_claim_limit(state, maintenance):
  _build_state(state, maintenance)

  buffers = {}
  sorts = {}
  jit = {}
  rows = {}
  for form, queues in SELECTOR_FORMS.items():
    plans = _explain(_claim_statements(queues))
    node_types = re.findall(r'"Node Type": "([^"]+)"', json.dumps(plans))
    buffers[form] = sum(
      plan["Plan"]["Shared Hit Blocks"] + plan["Plan"]["Shared Read Blocks"] for plan in plans
    )
    sorts[form] = [node_type for node_type in node_types if "Sort" in node_type]
    jit[form] = any("JIT" in plan for plan in plans)
    rows[form] = sum(plan["Plan"]["Actual Rows"] for plan in plans)
  logger.info(
    "claim plan bounds server=%s state=%r buffers=%s sorts=%s jit=%s",
    connection.pg_version,
    state,
    buffers,
    sorts,
    jit,
  )

  assert max(buffers.values()) < BUFFER_BOUND, buffers
  assert not any(sorts.values()), sorts
  assert not any(jit.values()), jit
  # every form selected a full claim of ready rows and read their jobs
  assert rows == dict.fromkeys(SELECTOR_FORMS, 2 * LIMIT)


@pytest.mark.parametrize("state", STATES)
def test_completion_cost_does_not_follow_the_jobs_table(state, maintenance):
  _build_state(state, maintenance)
  claimed = claim_ready_jobs(limit=LIMIT, queues="*")

  (plan,) = _explain([_completion_statement(claimed[0].job)])
  buffers = plan["Plan"]["Shared Hit Blocks"] + plan["Plan"]["Shared Read Blocks"]
  logger.info(
    "completion plan bounds server=%s state=%r buffers=%s", connection.pg_version, state, buffers
  )

  # the ready-state check may still scan the ready table after a drained backlog, which stays
  # under the bound at this backlog; a plan that walks the jobs table or its index does not
  assert buffers < BUFFER_BOUND, buffers
  assert "JIT" not in plan


def _build_state(state, maintenance):
  if state != "uncommitted enqueue, no finished jobs":
    now = timezone.now()
    Job.objects.bulk_create(
      [
        Job(
          task_path=echo.module_path,
          queue_name=QUEUES[index % len(QUEUES)],
          payload={"args": [], "kwargs": {}},
          backend_alias="default",
          finished_at=now,
        )
        for index in range(FINISHED_JOBS)
      ],
      batch_size=5_000,
    )
    _vacuum_analyze(maintenance, "dj_queue_jobs")

  if state == "fresh statistics":
    _enqueue_backlog()
    _vacuum_analyze(maintenance, "dj_queue_ready_executions", "dj_queue_jobs")
  elif state == "post-backlog":
    # the newest ready rows survive the drain, so vacuum cannot truncate the emptied pages
    _enqueue_backlog()
    kept = list(ReadyExecution.objects.order_by("-id").values_list("id", flat=True)[:3])
    ReadyExecution.objects.exclude(id__in=kept).delete()
    Job.objects.filter(finished_at=None, ready_execution=None).update(finished_at=timezone.now())
    _vacuum_analyze(maintenance, "dj_queue_ready_executions", "dj_queue_jobs")
    _enqueue_backlog()
  else:
    with transaction.atomic():
      _enqueue_backlog()
      _vacuum_analyze(maintenance, "dj_queue_ready_executions", "dj_queue_jobs")


def _enqueue_backlog():
  tasks = [echo.using(queue_name=queue_name) for queue_name in QUEUES]
  sparse_task = echo.using(queue_name=SPARSE_QUEUE)
  echo.get_backend().enqueue_all(
    [
      (sparse_task if index % SPARSE_EVERY == 0 else tasks[index % len(tasks)], (index,), {})
      for index in range(BACKLOG)
    ]
  )


def _vacuum_analyze(maintenance, *tables):
  with maintenance.cursor() as cursor:
    for table in tables:
      cursor.execute(f"VACUUM ANALYZE {table}")


def _claim_statements(queues):
  statements = []

  # aborting before the consume leaves every selector form the same state to claim from
  def capture(execute, sql, params, many, context):
    if "dj_queue_claimed_executions" in sql:
      raise Captured
    statements.append(connection.ops.compose_sql(sql, params))
    return execute(sql, params, many, context)

  with connection.execute_wrapper(capture), pytest.raises(Captured):
    claim_ready_jobs(limit=LIMIT, queues=queues)
  return statements


def _completion_statement(job):
  statements = []

  def capture(execute, sql, params, many, context):
    statements.append(connection.ops.compose_sql(sql, params))
    raise Captured

  with connection.execute_wrapper(capture), pytest.raises(Captured):
    complete_claimed_job(job, "done", config=load_backend_config())
  return statements[0]


def _explain(statements):
  settings = [sql for sql in statements if sql.startswith("SET LOCAL")]
  reads = [
    sql for sql in statements if "dj_queue_ready_executions" in sql or "dj_queue_jobs" in sql
  ]
  plans = []
  with transaction.atomic(), connection.cursor() as cursor:
    for sql in settings:
      cursor.execute(sql)
    for sql in reads:
      cursor.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}")
      plans.append(cursor.fetchone()[0][0])
    transaction.set_rollback(True)
  return plans
