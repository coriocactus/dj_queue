import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from django.db import connection, connections
from django.db.utils import OperationalError
from django.tasks import TaskResultStatus
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

import dj_queue.db as database
import dj_queue.operations.claiming as claiming_operations
import dj_queue.operations.concurrency as concurrency_operations
import dj_queue.operations.jobs as job_operations
from dj_queue.api import QueueInfo
from dj_queue.models import (
  BlockedExecution,
  ClaimedExecution,
  FailedExecution,
  Job,
  Pause,
  Process,
  ReadyExecution,
  RecurringExecution,
  RecurringTask,
  ScheduledExecution,
  Semaphore,
)
from dj_queue.operations.concurrency import (
  cleanup_expired_semaphores,
  promote_expired_blocked_jobs,
  semaphore_acquire,
  semaphore_acquire_many,
  semaphore_release,
)
from dj_queue.operations.jobs import (
  EnqueueError,
  claim_ready_jobs,
  complete_claimed_job,
  discard_ready_jobs,
  execute_claimed_job,
  fail_claimed_job,
  promote_failed_job_retries,
  promote_scheduled_jobs,
)
from dj_queue.operations.recurring import fire_recurring_task
from dj_queue.sql import mysql as mysql_sql
from dj_queue.sql import postgres as postgres_sql
from tests.tasks import echo, limited, limited_discard, other_queue


def make_job(task=echo, **overrides):
  payload = {
    "args": list(overrides.pop("args", [])),
    "kwargs": dict(overrides.pop("kwargs", {})),
  }
  payload.update(overrides.pop("payload", {}))

  return Job.objects.create(
    task_path=overrides.pop("task_path", task.module_path),
    queue_name=overrides.pop("queue_name", task.queue_name),
    priority=overrides.pop("priority", task.priority),
    payload=payload,
    backend_alias=overrides.pop("backend_alias", task.backend),
    scheduled_at=overrides.pop("scheduled_at", None),
    concurrency_key=overrides.pop("concurrency_key", None),
    finished_at=overrides.pop("finished_at", None),
    return_value=overrides.pop("return_value", None),
    **overrides,
  )


def queries_touching(ctx, table_name):
  return [query["sql"] for query in ctx.captured_queries if table_name in query["sql"]]


@pytest.mark.django_db
def test_semaphore_acquire_release_cycle():
  assert semaphore_acquire("account:1", limit=1, duration_seconds=60) is True
  assert semaphore_acquire("account:1", limit=1, duration_seconds=60) is False
  assert semaphore_release("account:1", duration_seconds=60) is True
  assert semaphore_acquire("account:1", limit=1, duration_seconds=60) is True


@pytest.mark.django_db
@pytest.mark.parametrize(
  ("value", "active_count", "acquired"),
  ((0, 0, False), (1, 1, True)),
)
def test_semaphore_acquire_uses_value_when_bridge_count_is_stale(
  value,
  active_count,
  acquired,
):
  Semaphore.objects.create(
    key="account:mixed-version",
    value=value,
    active_count=active_count,
    limit=1,
    expires_at=timezone.now() + timedelta(seconds=60),
  )

  assert semaphore_acquire("account:mixed-version", limit=1, duration_seconds=60) is acquired

  semaphore = Semaphore.objects.get(key="account:mixed-version")
  assert semaphore.active_count == semaphore.limit - semaphore.value


@pytest.mark.django_db
def test_semaphore_release_repairs_null_bridge_count_from_value():
  Semaphore.objects.create(
    key="account:mixed-version",
    value=0,
    active_count=None,
    limit=1,
    expires_at=timezone.now() + timedelta(seconds=60),
  )

  assert semaphore_release("account:mixed-version", duration_seconds=60) is True

  semaphore = Semaphore.objects.get(key="account:mixed-version")
  assert semaphore.value == 1
  assert semaphore.active_count == 0


@pytest.mark.django_db
def test_semaphore_acquire_many_uses_value_when_bridge_count_is_stale():
  Semaphore.objects.create(
    key="account:mixed-version",
    value=2,
    active_count=2,
    limit=2,
    expires_at=timezone.now() + timedelta(seconds=60),
  )

  acquired = semaphore_acquire_many(
    "account:mixed-version",
    count=2,
    limit=2,
    duration_seconds=60,
  )

  assert acquired == 2
  semaphore = Semaphore.objects.get(key="account:mixed-version")
  assert semaphore.value == 0
  assert semaphore.active_count == 2


@pytest.mark.django_db
def test_semaphore_acquire_reconciles_increased_limit():
  assert semaphore_acquire("account:resize", limit=1, duration_seconds=60) is True

  assert semaphore_acquire("account:resize", limit=2, duration_seconds=60) is True

  semaphore = Semaphore.objects.get(key="account:resize")
  assert semaphore.limit == 2
  assert semaphore.active_count == 2
  assert semaphore.value == 0


@pytest.mark.django_db
def test_semaphore_acquire_reconciles_reduced_limit_when_saturated():
  assert semaphore_acquire("account:resize", limit=2, duration_seconds=60) is True
  assert semaphore_acquire("account:resize", limit=2, duration_seconds=60) is True

  assert semaphore_acquire("account:resize", limit=1, duration_seconds=60) is False

  semaphore = Semaphore.objects.get(key="account:resize")
  assert semaphore.limit == 1
  assert semaphore.active_count == 1
  assert semaphore.value == 0


@pytest.mark.django_db
def test_reduced_semaphore_limit_uses_compatibility_available_count():
  assert semaphore_acquire("account:resize", limit=2, duration_seconds=60) is True
  assert semaphore_acquire("account:resize", limit=2, duration_seconds=60) is True

  assert semaphore_acquire("account:resize", limit=1, duration_seconds=60) is False
  assert semaphore_release("account:resize", duration_seconds=60) is True
  assert semaphore_acquire("account:resize", limit=1, duration_seconds=60) is True

  semaphore = Semaphore.objects.get(key="account:resize")
  assert semaphore.active_count == semaphore.limit == 1


@pytest.mark.django_db
def test_saturated_semaphore_acquire_without_reconcile_does_not_touch_row():
  assert semaphore_acquire("account:saturated", limit=1, duration_seconds=60) is True
  untouched_at = timezone.now() - timedelta(minutes=5)
  Semaphore.objects.filter(key="account:saturated").update(updated_at=untouched_at)

  assert semaphore_acquire("account:saturated", limit=1, duration_seconds=60) is False

  semaphore = Semaphore.objects.get(key="account:saturated")
  assert semaphore.updated_at == untouched_at


@pytest.mark.django_db
def test_saturated_bulk_semaphore_acquire_without_reconcile_does_not_touch_row():
  assert (
    semaphore_acquire_many("account:bulk-saturated", count=1, limit=1, duration_seconds=60) == 1
  )
  untouched_at = timezone.now() - timedelta(minutes=5)
  Semaphore.objects.filter(key="account:bulk-saturated").update(updated_at=untouched_at)

  assert (
    semaphore_acquire_many("account:bulk-saturated", count=1, limit=1, duration_seconds=60) == 0
  )

  semaphore = Semaphore.objects.get(key="account:bulk-saturated")
  assert semaphore.updated_at == untouched_at


@pytest.mark.django_db
def test_semaphore_release_reconciles_supplied_limit():
  assert semaphore_acquire("account:resize", limit=2, duration_seconds=60) is True
  assert semaphore_acquire("account:resize", limit=2, duration_seconds=60) is True

  assert semaphore_release("account:resize", limit=1, duration_seconds=60) is True

  semaphore = Semaphore.objects.get(key="account:resize")
  assert semaphore.limit == 1
  assert semaphore.active_count == 1
  assert semaphore.value == 0


@pytest.mark.django_db(transaction=True)
def test_claim_ready_jobs_retries_transient_database_deadlock(monkeypatch):
  job = make_job(args=["deadlock"])
  ReadyExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
  )
  calls = 0
  if connection.vendor == "postgresql":
    consume_owner = postgres_sql
    target = "consume_ready_and_create_claimed_executions"
  else:
    consume_owner = claiming_operations
    target = "_consume_selected_rows"
  original_consume = getattr(consume_owner, target)

  def consume_with_deadlock_once(*args, **kwargs):
    nonlocal calls
    calls += 1
    if calls == 1:
      raise OperationalError(
        "(1213, 'Deadlock found when trying to get lock; try restarting transaction')"
      )
    return original_consume(*args, **kwargs)

  monkeypatch.setattr(consume_owner, target, consume_with_deadlock_once)

  claimed_jobs = claim_ready_jobs(limit=1)

  assert [claimed_job.job.id for claimed_job in claimed_jobs] == [job.id]
  assert calls == 2


@pytest.mark.parametrize(
  "error",
  (
    type("PostgresError", (), {"pgcode": "40P01", "args": ()})(),
    type("SqlstateError", (), {"sqlstate": "40001", "args": ()})(),
    type("MysqlError", (), {"args": (1213, "deadlock")})(),
    type("SqliteError", (), {"sqlite_errorcode": 5, "args": ()})(),
    OperationalError("database is locked"),
  ),
)
def test_transient_claim_error_classification_uses_codes_before_message(error):
  assert database.is_transient_database_error(error) is True


def test_transient_claim_error_classification_checks_driver_cause():
  cause = type("PostgresError", (Exception,), {"pgcode": "55P03"})()
  error = OperationalError("driver wrapped error")
  error.__cause__ = cause

  assert database.is_transient_database_error(error) is True


def test_non_transient_claim_error_classification_rejects_unknown_errors():
  assert database.is_transient_database_error(OperationalError("table missing")) is False


@pytest.mark.django_db
def test_semaphore_signal_caps_at_limit():
  semaphore_acquire("account:1", limit=2, duration_seconds=60)
  semaphore_release("account:1", duration_seconds=60)
  semaphore_release("account:1", duration_seconds=60)

  semaphore = Semaphore.objects.get(key="account:1")

  assert semaphore.value == semaphore.limit == 2


@pytest.mark.django_db
def test_semaphore_release_uses_one_semaphore_table_query():
  semaphore_acquire("account:1", limit=1, duration_seconds=60)

  with CaptureQueriesContext(connection) as ctx:
    assert semaphore_release("account:1", duration_seconds=60) is True

  assert len(queries_touching(ctx, "dj_queue_semaphores")) == 1


@pytest.mark.django_db
def test_complete_claimed_job_uses_one_claimed_table_query():
  job = make_job(args=["done"])
  ClaimedExecution.objects.create(job=job)

  with CaptureQueriesContext(connection) as ctx:
    complete_claimed_job(job.id, "done")

  assert len(queries_touching(ctx, "dj_queue_claimed_executions")) == 1
  assert ClaimedExecution.objects.filter(job=job).exists() is False


@pytest.mark.django_db
def test_execute_claimed_job_uses_terminal_update_query_budget():
  job = make_job(args=["done"])
  ReadyExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
  )
  claimed_job = claim_ready_jobs(limit=1)[0]

  with CaptureQueriesContext(connection) as ctx:
    execute_claimed_job(claimed_job)

  expected_queries = 3 if connection.vendor == "postgresql" else 4
  assert len(ctx.captured_queries) == expected_queries


@pytest.mark.django_db
def test_execute_claimed_job_retries_terminal_deadlock_without_repeating_task(monkeypatch):
  job = make_job(args=["done"])
  ReadyExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
  )
  claimed_job = claim_ready_jobs(limit=1)[0]
  task_calls = 0
  transition_calls = 0
  original_call_task = job_operations._call_task
  original_release = job_operations._release_concurrency_slot

  def call_task(*args, **kwargs):
    nonlocal task_calls
    task_calls += 1
    return original_call_task(*args, **kwargs)

  def release_with_deadlock_once(*args, **kwargs):
    nonlocal transition_calls
    transition_calls += 1
    if transition_calls == 1:
      raise OperationalError("deadlock found when trying to get lock")
    return original_release(*args, **kwargs)

  monkeypatch.setattr(job_operations, "_call_task", call_task)
  monkeypatch.setattr(job_operations, "_release_concurrency_slot", release_with_deadlock_once)

  execute_claimed_job(claimed_job)

  assert task_calls == 1
  assert transition_calls == 2
  assert ClaimedExecution.objects.filter(job=job).exists() is False
  assert Job.objects.get(pk=job.id).return_value == "done"


@pytest.mark.django_db
def test_execute_failed_job_retries_terminal_deadlock_without_repeating_task(monkeypatch):
  job = make_job(task_path="tests.tasks.fail", args=["expected"])
  ReadyExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
  )
  claimed_job = claim_ready_jobs(limit=1)[0]
  task_calls = 0
  transition_calls = 0
  original_call_task = job_operations._call_task
  original_release = job_operations._release_concurrency_slot

  def call_task(*args, **kwargs):
    nonlocal task_calls
    task_calls += 1
    return original_call_task(*args, **kwargs)

  def release_with_deadlock_once(*args, **kwargs):
    nonlocal transition_calls
    transition_calls += 1
    if transition_calls == 1:
      raise OperationalError("deadlock found when trying to get lock")
    return original_release(*args, **kwargs)

  monkeypatch.setattr(job_operations, "_call_task", call_task)
  monkeypatch.setattr(job_operations, "_release_concurrency_slot", release_with_deadlock_once)

  execute_claimed_job(claimed_job)

  assert task_calls == 1
  assert transition_calls == 2
  assert ClaimedExecution.objects.filter(job=job).exists() is False
  assert FailedExecution.objects.filter(job=job, message="expected").exists() is True


@pytest.mark.django_db
def test_complete_claimed_job_with_waiter_uses_one_semaphore_query():
  first = limited.enqueue(1, value="first")
  limited.enqueue(1, value="second")
  claim_ready_jobs(limit=1)

  with CaptureQueriesContext(connection) as ctx:
    complete_claimed_job(first.id, "done")

  assert len(queries_touching(ctx, "dj_queue_semaphores")) == 1


@pytest.mark.django_db
def test_execute_claimed_job_with_waiter_avoids_nested_unblock_savepoint():
  limited.enqueue(1, value="first")
  limited.enqueue(1, value="second")
  claimed_job = claim_ready_jobs(limit=1)[0]

  with CaptureQueriesContext(connection) as ctx:
    execute_claimed_job(claimed_job)

  expected_queries = 6 if connection.vendor == "postgresql" else 9
  assert len(ctx.captured_queries) == expected_queries


@pytest.mark.django_db
def test_execute_claimed_job_direct_handoff_query_budget_stays_bounded():
  limited.enqueue(1, value="first")
  limited.enqueue(1, value="second")
  process = Process.objects.create(
    backend_alias="default",
    kind="Worker",
    pid=12345,
    hostname="localhost",
    name="worker-handoff-budget",
    metadata={},
    last_heartbeat_at=timezone.now(),
  )
  claimed_job = claim_ready_jobs(limit=1, process=process)[0]

  with CaptureQueriesContext(connection) as ctx:
    execute_claimed_job(claimed_job)

  assert len(ctx.captured_queries) <= 20


@pytest.mark.skipif(
  os.environ.get("DB_BACKEND", "sqlite") != "postgres",
  reason="requires DB_BACKEND=postgres",
)
@pytest.mark.django_db
def test_execute_claimed_job_with_waiter_consumes_blocked_row_without_select_on_postgres():
  limited.enqueue(1, value="first")
  limited.enqueue(1, value="second")
  claimed_job = claim_ready_jobs(limit=1)[0]

  with CaptureQueriesContext(connection) as ctx:
    execute_claimed_job(claimed_job)

  blocked_selects = [
    sql
    for sql in queries_touching(ctx, "dj_queue_blocked_executions")
    if sql.lstrip().upper().startswith("SELECT")
  ]
  assert blocked_selects == []


@pytest.mark.django_db
def test_fail_claimed_job_uses_one_claimed_table_query():
  job = make_job(args=["failed"])
  ClaimedExecution.objects.create(job=job)

  with CaptureQueriesContext(connection) as ctx:
    fail_claimed_job(job.id, ValueError("boom"), traceback_text="traceback")

  assert len(queries_touching(ctx, "dj_queue_claimed_executions")) == 1
  assert ClaimedExecution.objects.filter(job=job).exists() is False


@pytest.mark.skipif(
  os.environ.get("DB_BACKEND", "sqlite") == "sqlite",
  reason="requires a shared test database across threads",
)
@pytest.mark.django_db(transaction=True)
def test_concurrent_acquire_allows_exactly_limit_successes():
  limit = 2
  attempts = 5
  barrier = threading.Barrier(attempts)

  def acquire_once():
    try:
      barrier.wait()
      return semaphore_acquire("account:concurrent", limit=limit, duration_seconds=60)
    finally:
      connections.close_all()

  with ThreadPoolExecutor(max_workers=attempts) as executor:
    results = list(executor.map(lambda _: acquire_once(), range(attempts)))

  assert results.count(True) == limit
  assert results.count(False) == attempts - limit
  assert Semaphore.objects.get(key="account:concurrent").value == 0


@pytest.mark.skipif(
  os.environ.get("DB_BACKEND", "sqlite") == "sqlite",
  reason="requires a shared test database across threads",
)
@pytest.mark.django_db(transaction=True)
def test_concurrent_first_acquire_creates_one_semaphore_row():
  attempts = 2
  barrier = threading.Barrier(attempts)

  def acquire_once():
    try:
      barrier.wait()
      return semaphore_acquire("account:first", limit=1, duration_seconds=60)
    finally:
      connections.close_all()

  with ThreadPoolExecutor(max_workers=attempts) as executor:
    results = list(executor.map(lambda _: acquire_once(), range(attempts)))

  assert results.count(True) == 1
  assert results.count(False) == 1
  assert Semaphore.objects.filter(key="account:first").count() == 1
  assert Semaphore.objects.get(key="account:first").value == 0


@pytest.mark.skipif(
  os.environ.get("DB_BACKEND", "sqlite") == "sqlite",
  reason="requires a shared test database across threads",
)
@pytest.mark.django_db(transaction=True)
def test_concurrent_recurring_fire_creates_one_reservation_and_job():
  attempts = 2
  barrier = threading.Barrier(attempts)
  recurring_task = RecurringTask.objects.create(
    backend_alias="default",
    key="every-minute",
    task_path=echo.module_path,
    payload={"args": ["hello"], "kwargs": {}},
    schedule="* * * * *",
    queue_name=echo.queue_name,
    priority=echo.priority,
  )
  run_at = timezone.now().replace(second=0, microsecond=0) + timedelta(minutes=1)

  def fire_once():
    try:
      barrier.wait()
      execution = fire_recurring_task(recurring_task, run_at)
      return execution is not None
    finally:
      connections.close_all()

  with ThreadPoolExecutor(max_workers=attempts) as executor:
    results = list(executor.map(lambda _: fire_once(), range(attempts)))

  assert results.count(True) == 1
  assert results.count(False) == 1
  assert (
    RecurringExecution.objects.filter(
      backend_alias="default",
      task_key=recurring_task.key,
      run_at=run_at,
    ).count()
    == 1
  )
  assert Job.objects.count() == 1


@pytest.mark.skipif(
  os.environ.get("DB_BACKEND", "sqlite") == "sqlite",
  reason="requires a shared test database across threads",
)
@pytest.mark.django_db(transaction=True)
def test_concurrent_failed_retry_promotion_consumes_each_due_row_once():
  attempts = 2
  barrier = threading.Barrier(attempts)
  jobs = [make_job(args=[index]) for index in range(5)]
  retry_at = timezone.now() - timedelta(seconds=1)
  FailedExecution.objects.bulk_create(
    [
      FailedExecution(
        job=job,
        exception_class="builtins.ValueError",
        message="boom",
        traceback="traceback",
        retry_at=retry_at,
      )
      for job in jobs
    ]
  )

  def promote_once():
    try:
      barrier.wait()
      return [job.id for job in promote_failed_job_retries(batch_size=len(jobs))]
    finally:
      connections.close_all()

  with ThreadPoolExecutor(max_workers=attempts) as executor:
    promoted_groups = list(executor.map(lambda _: promote_once(), range(attempts)))

  promoted_ids = [job_id for group in promoted_groups for job_id in group]
  expected_ids = {job.id for job in jobs}
  assert len(promoted_ids) == len(expected_ids)
  assert set(promoted_ids) == expected_ids
  assert FailedExecution.objects.filter(job__in=jobs).exists() is False
  assert ReadyExecution.objects.filter(job__in=jobs).count() == len(jobs)


@pytest.mark.django_db
def test_retry_failed_jobs_retries_transient_database_deadlock(monkeypatch):
  job = make_job(args=["retry"])
  FailedExecution.objects.create(
    job=job,
    exception_class="builtins.ValueError",
    message="boom",
    traceback="traceback",
  )
  calls = 0
  original_dispatch = job_operations._dispatch_consumed_failed_rows

  def dispatch_with_deadlock_once(*args, **kwargs):
    nonlocal calls
    calls += 1
    if calls == 1:
      raise OperationalError("deadlock found when trying to get lock")
    return original_dispatch(*args, **kwargs)

  monkeypatch.setattr(
    job_operations,
    "_dispatch_consumed_failed_rows",
    dispatch_with_deadlock_once,
  )

  assert job_operations.retry_failed_jobs(job_ids=[job.id]) == 1
  assert calls == 2
  assert FailedExecution.objects.filter(job=job).exists() is False
  assert ReadyExecution.objects.filter(job=job).exists() is True


@pytest.mark.django_db
def test_enqueue_with_concurrency_slot_available_goes_ready():
  result = limited.enqueue(1, value="first")

  job = ReadyExecution.objects.get(job_id=result.id).job
  semaphore = Semaphore.objects.get(key="account:1")

  assert result.status.name == "READY"
  assert job.concurrency_key == "account:1"
  assert semaphore.value == 0
  assert semaphore.limit == 1


@pytest.mark.django_db
def test_semaphore_acquire_many_reserves_available_slots():
  acquired = semaphore_acquire_many(
    "account:bulk",
    count=3,
    limit=2,
    duration_seconds=60,
  )

  assert acquired == 2
  semaphore = Semaphore.objects.get(key="account:bulk")
  assert semaphore.value == 0

  assert (
    semaphore_acquire_many(
      "account:bulk",
      count=1,
      limit=2,
      duration_seconds=60,
    )
    == 0
  )


@pytest.mark.django_db
def test_enqueue_with_concurrency_limit_reached_goes_blocked():
  limited.enqueue(1, value="first")
  second = limited.enqueue(1, value="second")
  job = Job.objects.get(pk=second.id)

  assert ReadyExecution.objects.count() == 1
  assert job.blocked is True
  assert job.concurrency_key == "account:1"
  assert job.blocked_execution.concurrency_key == "account:1"
  assert limited.get_backend().get_result(second.id).status == TaskResultStatus.READY


@pytest.mark.django_db
def test_successful_completion_unblocks_next_waiter():
  first = limited.enqueue(1, value="first")
  second = limited.enqueue(1, value="second")

  claimed_jobs = claim_ready_jobs(limit=1)
  complete_claimed_job(first.id, "done")

  assert [str(claimed_job.job.id) for claimed_job in claimed_jobs] == [first.id]
  assert ReadyExecution.objects.filter(job_id=second.id).exists() is True
  assert Semaphore.objects.get(key="account:1").value == 0


@pytest.mark.django_db
def test_worker_completion_directly_claims_limit_one_waiter():
  process = Process.objects.create(
    backend_alias="default",
    kind="Worker",
    pid=12345,
    hostname="localhost",
    name="worker-handoff",
    metadata={},
    last_heartbeat_at=timezone.now(),
  )
  first = limited.enqueue(1, value="first")
  second = limited.enqueue(1, value="second")
  claimed_job = claim_ready_jobs(limit=1, process=process)[0]

  outcome = job_operations._complete_claimed_job(
    claimed_job,
    "done",
    backend_alias="default",
    task=limited,
  )

  assert str(outcome.job.id) == first.id
  assert str(outcome.next_claimed_job.job.id) == second.id
  assert outcome.next_claimed_job.process_id == process.id
  assert ClaimedExecution.objects.filter(job_id=second.id, process=process).exists() is True
  assert ReadyExecution.objects.filter(job_id=second.id).exists() is False
  assert BlockedExecution.objects.filter(job_id=second.id).exists() is False
  assert Semaphore.objects.get(key="account:1").value == 0


@pytest.mark.django_db
def test_direct_handoff_respects_paused_queue():
  process = Process.objects.create(
    backend_alias="default",
    kind="Worker",
    pid=12345,
    hostname="localhost",
    name="worker-paused-handoff",
    metadata={},
    last_heartbeat_at=timezone.now(),
  )
  first = limited.enqueue(1, value="first")
  second = limited.enqueue(1, value="second")
  claimed_job = claim_ready_jobs(limit=1, process=process)[0]
  QueueInfo("default").pause()

  outcome = job_operations._complete_claimed_job(
    claimed_job,
    "done",
    backend_alias="default",
    task=limited,
  )

  assert str(outcome.job.id) == first.id
  assert outcome.next_claimed_job is None
  assert ReadyExecution.objects.filter(job_id=second.id).exists() is True
  assert ClaimedExecution.objects.filter(job_id=second.id).exists() is False
  assert BlockedExecution.objects.filter(job_id=second.id).exists() is False


@pytest.mark.django_db
def test_execute_claimed_job_reuses_loaded_task_for_concurrency_release(monkeypatch):
  limited.func.concurrency_limit = 1
  limited.enqueue(1, value="first")
  second = limited.enqueue(1, value="second")
  claimed_job = claim_ready_jobs(limit=1)[0]
  seen = []
  original_import_string = job_operations.import_string

  def capture(path):
    seen.append(path)
    return original_import_string(path)

  monkeypatch.setattr(job_operations, "import_string", capture)

  execute_claimed_job(claimed_job)

  assert seen == [limited.module_path]
  assert ReadyExecution.objects.filter(job_id=second.id).exists() is True
  assert Semaphore.objects.get(key="account:1").value == 0


@pytest.mark.django_db
def test_failed_completion_still_unblocks_next_waiter():
  first = limited.enqueue(1, value="first")
  second = limited.enqueue(1, value="second")

  claim_ready_jobs(limit=1)
  fail_claimed_job(first.id, ValueError("boom"), traceback_text="traceback")

  assert FailedExecution.objects.filter(job_id=first.id).exists() is True
  assert ReadyExecution.objects.filter(job_id=second.id).exists() is True
  assert Semaphore.objects.get(key="account:1").value == 0


@pytest.mark.django_db
def test_complete_claimed_job_rolls_back_when_concurrency_release_fails(monkeypatch):
  first = limited.enqueue(1, value="first")
  claim_ready_jobs(limit=1)

  def fail_release(*args, **kwargs):
    raise RuntimeError("release failed")

  monkeypatch.setattr(job_operations, "semaphore_release", fail_release)

  with pytest.raises(RuntimeError, match="release failed"):
    complete_claimed_job(first.id, "done")

  assert ClaimedExecution.objects.filter(job_id=first.id).exists() is True
  assert Job.objects.get(pk=first.id).finished_at is None


@pytest.mark.django_db
def test_fail_claimed_job_rejects_conflicting_execution_state():
  process = Process.objects.create(
    backend_alias="default",
    kind="Worker",
    pid=12345,
    hostname="localhost",
    name="worker-fail-conflict",
    metadata={},
    last_heartbeat_at=timezone.now(),
  )
  job = make_job()
  ClaimedExecution.objects.create(job=job, process=process)
  ReadyExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
  )

  with pytest.raises(EnqueueError, match="already has an execution-state row"):
    fail_claimed_job(job.id, ValueError("boom"), traceback_text="traceback")

  assert FailedExecution.objects.filter(job=job).exists() is False


@pytest.mark.django_db
def test_complete_claimed_job_rejects_conflicting_execution_state():
  job = make_job()
  ClaimedExecution.objects.create(job=job)
  ReadyExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
  )

  with pytest.raises(EnqueueError, match="already has an execution-state row"):
    complete_claimed_job(job.id, "done")

  assert ClaimedExecution.objects.filter(job=job).exists() is True
  assert Job.objects.get(pk=job.pk).finished_at is None


@pytest.mark.django_db(transaction=True)
def test_missing_concurrency_task_path_releases_slot_and_unblocks_next_waiter():
  process = Process.objects.create(
    backend_alias="default",
    kind="Worker",
    pid=12345,
    hostname="localhost",
    name="worker-missing-concurrency-task",
    metadata={},
    last_heartbeat_at=timezone.now(),
  )
  first = make_job(
    task_path="tests.tasks.missing_concurrency_task",
    concurrency_key="account:missing-task",
  )
  second = make_job(concurrency_key="account:missing-task")
  Semaphore.objects.create(
    key="account:missing-task",
    value=0,
    active_count=1,
    limit=1,
    expires_at=timezone.now() + timedelta(seconds=60),
  )
  ClaimedExecution.objects.create(job=first, process=process)
  BlockedExecution.objects.create(
    job=second,
    backend_alias=second.backend_alias,
    queue_name=second.queue_name,
    priority=second.priority,
    concurrency_key="account:missing-task",
    expires_at=timezone.now() + timedelta(seconds=60),
  )

  execute_claimed_job(first)

  assert FailedExecution.objects.filter(job=first).exists() is True
  assert ReadyExecution.objects.filter(job=second).exists() is True
  assert BlockedExecution.objects.filter(job=second).exists() is False


@pytest.mark.django_db
def test_invalid_concurrency_settings_after_claim_do_not_leak_slot(monkeypatch):
  first = limited.enqueue(1, value="first")
  second = limited.enqueue(1, value="second")
  claim_ready_jobs(limit=1)
  monkeypatch.setattr(limited.func, "concurrency_limit", "many")

  complete_claimed_job(first.id, "done")

  assert ReadyExecution.objects.filter(job_id=second.id).exists() is True
  assert BlockedExecution.objects.filter(job_id=second.id).exists() is False
  assert Semaphore.objects.get(key="account:1").value == 0


@pytest.mark.django_db
def test_completion_uses_persisted_concurrency_policy(monkeypatch):
  first = limited.enqueue(1, value="first")
  second = limited.enqueue(1, value="second")
  claim_ready_jobs(limit=1)

  def fail_import(_task_path):
    raise AssertionError("completion should use persisted concurrency policy")

  monkeypatch.setattr(job_operations, "import_string", fail_import)

  complete_claimed_job(first.id, "done")

  assert ReadyExecution.objects.filter(job_id=second.id).exists() is True
  assert BlockedExecution.objects.filter(job_id=second.id).exists() is False


@pytest.mark.django_db
def test_reduced_concurrency_limit_after_claim_uses_compatibility_handoff(monkeypatch):
  monkeypatch.setattr(limited.func, "concurrency_limit", 2)
  first = limited.enqueue(1, value="first")
  second = limited.enqueue(1, value="second")
  third = limited.enqueue(1, value="third")

  claim_ready_jobs(limit=2)
  monkeypatch.setattr(limited.func, "concurrency_limit", 1)
  assert semaphore_acquire("account:1", limit=1, duration_seconds=60) is False

  complete_claimed_job(first.id, "done")

  assert ClaimedExecution.objects.filter(job_id=second.id).exists() is True
  assert BlockedExecution.objects.filter(job_id=third.id).exists() is False
  assert ReadyExecution.objects.filter(job_id=third.id).exists() is True
  assert Semaphore.objects.get(key="account:1").value == 1


@pytest.mark.django_db
def test_recovered_concurrency_release_falls_back_for_conflicting_settings(monkeypatch):
  monkeypatch.setattr(limited.func, "concurrency_limit", 1)
  monkeypatch.setattr(limited_discard.func, "concurrency_limit", 2)
  jobs = [
    make_job(
      task=limited,
      args=[1],
      kwargs={"value": "first"},
      concurrency_key="account:conflict",
    ),
    make_job(
      task=limited_discard,
      args=[1],
      kwargs={"value": "second"},
      concurrency_key="account:conflict",
    ),
  ]

  fallback_jobs = concurrency_operations.release_recovered_concurrency_slots(jobs)

  assert fallback_jobs == jobs


@pytest.mark.django_db
def test_dispatcher_promotes_expired_blocked_jobs():
  job = make_job(task=limited, args=[1], kwargs={"value": "later"}, concurrency_key="account:1")
  BlockedExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
    concurrency_key=job.concurrency_key,
    expires_at=timezone.now() - timedelta(seconds=1),
  )

  promoted = promote_expired_blocked_jobs(batch_size=10)

  assert [promoted_job.id for promoted_job in promoted] == [job.id]
  assert BlockedExecution.objects.filter(job=job).exists() is False
  assert ReadyExecution.objects.filter(job=job).exists() is True
  assert Semaphore.objects.get(key="account:1").value == 0


@pytest.mark.django_db
def test_promote_expired_blocked_jobs_rejects_conflicting_execution_state():
  job = make_job(task=limited, args=[1], kwargs={"value": "later"}, concurrency_key="account:1")
  BlockedExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
    concurrency_key=job.concurrency_key,
    expires_at=timezone.now() - timedelta(seconds=1),
  )
  FailedExecution.objects.create(
    job=job,
    exception_class="builtins.ValueError",
    message="boom",
    traceback="traceback",
  )

  with pytest.raises(EnqueueError, match="already has an execution-state row"):
    promote_expired_blocked_jobs(batch_size=10)

  assert BlockedExecution.objects.filter(job=job).exists() is True
  assert ReadyExecution.objects.filter(job=job).exists() is False
  assert FailedExecution.objects.filter(job=job).exists() is True


@pytest.mark.django_db
def test_expired_semaphore_cleanup_preserves_active_claimed_key():
  first = limited.enqueue(1, value="first")
  second = limited.enqueue(1, value="second")
  claim_ready_jobs(limit=1)
  Semaphore.objects.filter(key="account:1").update(
    expires_at=timezone.now() - timedelta(seconds=1)
  )
  BlockedExecution.objects.filter(job_id=second.id).update(
    expires_at=timezone.now() - timedelta(seconds=1)
  )

  assert cleanup_expired_semaphores() == 0
  assert promote_expired_blocked_jobs(batch_size=10) == []
  assert ClaimedExecution.objects.filter(job_id=first.id).exists() is True
  assert BlockedExecution.objects.filter(job_id=second.id).exists() is True
  assert ReadyExecution.objects.filter(job_id=second.id).exists() is False


@pytest.mark.django_db
def test_expired_semaphore_cleanup_preserves_ready_key_with_reserved_slot():
  first = limited.enqueue(1, value="first")
  second = limited.enqueue(1, value="second")
  Semaphore.objects.filter(key="account:1").update(
    expires_at=timezone.now() - timedelta(seconds=1)
  )

  assert cleanup_expired_semaphores() == 0

  third = limited.enqueue(1, value="third")

  assert ReadyExecution.objects.filter(job_id=first.id).exists() is True
  assert BlockedExecution.objects.filter(job_id=second.id).exists() is True
  assert BlockedExecution.objects.filter(job_id=third.id).exists() is True
  assert Semaphore.objects.get(key="account:1").value == 0


@pytest.mark.django_db
def test_promote_expired_blocked_jobs_reuses_task_import_for_shared_task_path(monkeypatch):
  imported = []

  def fake_import_string(path):
    imported.append(path)
    from django.utils.module_loading import import_string

    return import_string(path)

  monkeypatch.setattr("dj_queue.operations.concurrency.import_string", fake_import_string)

  for account_id in (1, 2):
    job = make_job(
      task=limited,
      args=[account_id],
      kwargs={"value": f"later-{account_id}"},
      concurrency_key=f"account:{account_id}",
    )
    BlockedExecution.objects.create(
      job=job,
      backend_alias=job.backend_alias,
      queue_name=job.queue_name,
      priority=job.priority,
      concurrency_key=job.concurrency_key,
      expires_at=timezone.now() - timedelta(seconds=1),
    )

  promote_expired_blocked_jobs(batch_size=10)

  assert imported == [limited.module_path]


@pytest.mark.django_db
def test_promote_expired_blocked_jobs_uses_backend_default_concurrency_duration(
  monkeypatch, settings
):
  settings.TASKS = {
    **settings.TASKS,
    "default": {
      **settings.TASKS["default"],
      "OPTIONS": {
        **settings.TASKS["default"].get("OPTIONS", {}),
        "default_concurrency_duration": 240,
      },
    },
  }
  monkeypatch.delattr(limited.func, "concurrency_duration", raising=False)
  job = make_job(task=limited, args=[1], kwargs={"value": "later"}, concurrency_key="account:1")
  BlockedExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
    concurrency_key=job.concurrency_key,
    expires_at=timezone.now() - timedelta(seconds=1),
  )
  Semaphore.objects.create(
    key="account:1",
    value=0,
    active_count=1,
    limit=1,
    expires_at=timezone.now() + timedelta(seconds=240),
  )
  before_promote = timezone.now()

  assert promote_expired_blocked_jobs(batch_size=10) == []

  blocked = BlockedExecution.objects.get(job=job)
  assert blocked.expires_at >= before_promote + timedelta(seconds=200)


@pytest.mark.django_db
def test_blocked_promotion_uses_persisted_concurrency_policy(monkeypatch):
  limited.enqueue(1, value="first")
  second = limited.enqueue(1, value="second")
  BlockedExecution.objects.filter(job_id=second.id).update(
    expires_at=timezone.now() - timedelta(seconds=1)
  )

  def fail_import(_task_path):
    raise AssertionError("blocked promotion should use persisted concurrency policy")

  monkeypatch.setattr(concurrency_operations, "import_string", fail_import)
  before_promote = timezone.now()

  assert promote_expired_blocked_jobs(batch_size=10) == []

  blocked = BlockedExecution.objects.get(job_id=second.id)
  assert blocked.expires_at >= before_promote + timedelta(seconds=50)


@pytest.mark.django_db
def test_blocked_promotion_isolates_unresolvable_historical_policy():
  expired_at = timezone.now() - timedelta(seconds=2)
  missing = make_job(
    task_path="tests.missing.removed_task",
    concurrency_key="account:missing",
  )
  following = make_job(
    task=limited,
    args=[2],
    concurrency_key="account:following",
    concurrency_limit=1,
    concurrency_duration=60,
    concurrency_on_conflict="block",
  )
  for index, job in enumerate((missing, following)):
    BlockedExecution.objects.create(
      job=job,
      backend_alias=job.backend_alias,
      queue_name=job.queue_name,
      priority=job.priority,
      concurrency_key=job.concurrency_key,
      expires_at=expired_at + timedelta(seconds=index),
    )

  promoted = promote_expired_blocked_jobs(batch_size=10)

  assert [job.id for job in promoted] == [following.id]
  assert FailedExecution.objects.filter(job=missing).exists() is True
  assert BlockedExecution.objects.filter(job=missing).exists() is False
  assert ReadyExecution.objects.filter(job=following).exists() is True


@pytest.mark.django_db
def test_discarding_ready_job_releases_waiter():
  first = limited.enqueue(1, value="first")
  second = limited.enqueue(1, value="second")

  deleted = discard_ready_jobs(job_ids=[first.id], batch_size=1)

  assert deleted == 1
  assert ReadyExecution.objects.filter(job_id=second.id).exists() is True
  assert Semaphore.objects.get(key="account:1").value == 0


@pytest.mark.django_db(transaction=True)
def test_unblock_next_blocked_job_restores_waiter_when_slot_is_not_acquired(monkeypatch):
  job = make_job(task=limited, args=[1], kwargs={"value": "blocked"}, concurrency_key="account:1")
  expires_at = timezone.now() + timedelta(minutes=1)
  BlockedExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
    concurrency_key=job.concurrency_key,
    expires_at=expires_at,
  )
  monkeypatch.setattr(concurrency_operations, "semaphore_acquire", lambda *args, **kwargs: False)

  unblocked = concurrency_operations.unblock_next_blocked_job(
    "account:1",
    limit=1,
    duration_seconds=60,
  )

  blocked = BlockedExecution.objects.get(job=job)
  assert unblocked is None
  assert blocked.queue_name == job.queue_name
  assert blocked.priority == job.priority
  assert blocked.concurrency_key == "account:1"
  assert blocked.expires_at == expires_at
  assert ReadyExecution.objects.filter(job=job).exists() is False


@pytest.mark.django_db(transaction=True)
def test_unblock_next_blocked_job_returns_blocked_job_reference():
  job = make_job(task=limited, args=[1], kwargs={"value": "blocked"}, concurrency_key="account:1")
  BlockedExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
    concurrency_key=job.concurrency_key,
    expires_at=timezone.now() + timedelta(minutes=1),
  )

  unblocked = concurrency_operations.unblock_next_blocked_job(
    "account:1",
    limit=1,
    duration_seconds=60,
  )

  assert isinstance(unblocked, concurrency_operations.BlockedJobRef)
  assert unblocked.pk == job.pk
  assert unblocked.backend_alias == job.backend_alias
  assert not hasattr(unblocked, "task_path")
  assert ReadyExecution.objects.filter(job=job).exists() is True


@pytest.mark.django_db
def test_unblock_next_blocked_job_rejects_conflicting_execution_state():
  job = make_job(task=limited, args=[1], kwargs={"value": "blocked"}, concurrency_key="account:1")
  BlockedExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
    concurrency_key=job.concurrency_key,
    expires_at=timezone.now() + timedelta(minutes=1),
  )
  FailedExecution.objects.create(
    job=job,
    exception_class="builtins.ValueError",
    message="boom",
    traceback="traceback",
  )

  with pytest.raises(EnqueueError, match="already has an execution-state row"):
    concurrency_operations.unblock_next_blocked_job(
      "account:1",
      limit=1,
      duration_seconds=60,
    )


@pytest.mark.django_db
def test_queue_pause_blocks_claiming_not_enqueue():
  Pause.objects.create(backend_alias="default", queue_name="other")
  other_queue.enqueue("paused")

  claimed_jobs = claim_ready_jobs(limit=1, queues=("other",))

  assert ReadyExecution.objects.filter(queue_name="other").count() == 1
  assert claimed_jobs == []


@pytest.mark.django_db(transaction=True)
def test_claim_rechecks_pause_created_during_claim(monkeypatch):
  job = make_job(queue_name="critical")
  ReadyExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name="critical",
    priority=0,
  )
  original_select_ready_rows = claiming_operations._select_ready_rows

  def pause_during_selection(*args, **kwargs):
    rows = original_select_ready_rows(*args, **kwargs)
    QueueInfo("critical").pause()
    return rows

  monkeypatch.setattr(claiming_operations, "_select_ready_rows", pause_during_selection)

  claimed_jobs = claim_ready_jobs(limit=1, queues=("critical",))

  assert claimed_jobs == []
  assert ReadyExecution.objects.filter(job=job).exists() is True
  assert ClaimedExecution.objects.filter(job=job).exists() is False


@pytest.mark.django_db
def test_claim_ready_jobs_uses_fixed_query_budget_for_successful_claim():
  job = make_job()
  ReadyExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
  )

  with CaptureQueriesContext(connection) as ctx:
    claim_ready_jobs(limit=1)

  if connection.vendor == "postgresql":
    expected_queries = 5
  elif connection.vendor == "mysql":
    expected_queries = 8
  else:
    expected_queries = 7
  assert len(ctx.captured_queries) == expected_queries


@pytest.mark.skipif(
  os.environ.get("DB_BACKEND", "sqlite") not in {"mysql", "mariadb"},
  reason="requires DB_BACKEND=mysql or DB_BACKEND=mariadb",
)
@pytest.mark.django_db(transaction=True)
def test_mysql_family_claim_uses_indexed_ready_lookups():
  job = make_job()
  ReadyExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
  )

  with CaptureQueriesContext(connection) as ctx:
    claim_ready_jobs(limit=1)

  selects = [
    query["sql"]
    for query in ctx.captured_queries
    if query["sql"].lstrip().startswith("SELECT") and "dj_queue_ready_executions" in query["sql"]
  ]
  deletes = [
    query["sql"]
    for query in ctx.captured_queries
    if query["sql"].lstrip().startswith("DELETE") and "dj_queue_ready_executions" in query["sql"]
  ]
  assert any("FORCE INDEX (`djq_re_b_prio_d_idx`)" in query for query in selects)
  assert any("STRAIGHT_JOIN `dj_queue_ready_executions` ready" in query for query in deletes)


@pytest.mark.skipif(
  os.environ.get("DB_BACKEND", "sqlite") != "sqlite",
  reason="simulates SQLite's lack of row-level locks",
)
@pytest.mark.django_db(transaction=True)
def test_sqlite_claim_skips_ready_row_consumed_after_selection(monkeypatch):
  job = make_job()
  ReadyExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
  )
  original_select_ready_rows = claiming_operations._select_ready_rows

  def consume_during_selection(*args, **kwargs):
    rows = original_select_ready_rows(*args, **kwargs)
    ReadyExecution.objects.filter(pk__in=[row.pk for row in rows]).delete()
    return rows

  monkeypatch.setattr(claiming_operations, "_select_ready_rows", consume_during_selection)

  assert claim_ready_jobs(limit=1) == []
  assert ClaimedExecution.objects.filter(job=job).exists() is False


@pytest.mark.skipif(
  os.environ.get("DB_BACKEND", "sqlite") != "sqlite",
  reason="simulates SQLite's lack of row-level locks",
)
@pytest.mark.django_db(transaction=True)
def test_sqlite_promote_skips_scheduled_row_consumed_after_selection(monkeypatch):
  scheduled_at = timezone.now() - timedelta(seconds=1)
  job = make_job(scheduled_at=scheduled_at)
  ScheduledExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
    scheduled_at=scheduled_at,
  )
  original_locked_queryset = __import__(
    "dj_queue.operations.jobs", fromlist=["locked_queryset"]
  ).locked_queryset

  class ConsumingQuerySet:
    def __init__(self, queryset):
      self.queryset = queryset

    def __getitem__(self, index):
      rows = list(self.queryset[index])
      ScheduledExecution.objects.filter(pk__in=[row.pk for row in rows]).delete()
      return rows

  def consume_during_selection(queryset, *, use_skip_locked=True):
    return ConsumingQuerySet(original_locked_queryset(queryset, use_skip_locked=use_skip_locked))

  monkeypatch.setattr("dj_queue.operations.jobs.locked_queryset", consume_during_selection)

  assert promote_scheduled_jobs(batch_size=10) == []
  assert ReadyExecution.objects.filter(job=job).exists() is False


@pytest.mark.skipif(
  os.environ.get("DB_BACKEND", "sqlite") != "sqlite",
  reason="simulates SQLite's lack of row-level locks",
)
@pytest.mark.django_db(transaction=True)
def test_sqlite_promote_skips_blocked_row_consumed_after_selection(monkeypatch):
  job = make_job(task=limited, args=[1], kwargs={"value": "later"}, concurrency_key="account:1")
  BlockedExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
    concurrency_key=job.concurrency_key,
    expires_at=timezone.now() - timedelta(seconds=1),
  )
  original_locked_queryset = __import__(
    "dj_queue.operations.concurrency", fromlist=["locked_queryset"]
  ).locked_queryset

  class ConsumingQuerySet:
    def __init__(self, queryset):
      self.queryset = queryset

    def __getitem__(self, index):
      rows = list(self.queryset[index])
      BlockedExecution.objects.filter(pk__in=[row.pk for row in rows]).delete()
      return rows

  def consume_during_selection(queryset, *, use_skip_locked=True):
    return ConsumingQuerySet(original_locked_queryset(queryset, use_skip_locked=use_skip_locked))

  monkeypatch.setattr("dj_queue.operations.concurrency.locked_queryset", consume_during_selection)

  assert promote_expired_blocked_jobs(batch_size=10) == []
  assert ReadyExecution.objects.filter(job=job).exists() is False


@pytest.mark.django_db
def test_queue_resume_restores_claiming():
  pause = Pause.objects.create(backend_alias="default", queue_name="other")
  result = other_queue.enqueue("paused")

  assert claim_ready_jobs(limit=1, queues=("other",)) == []

  pause.delete()
  claimed_jobs = claim_ready_jobs(limit=1, queues=("other",))

  assert [str(claimed_job.job.id) for claimed_job in claimed_jobs] == [result.id]


@pytest.mark.django_db
def test_queue_selector_exact_prefix_and_star_ordering():
  alpha = echo.using(queue_name="alpha").enqueue("alpha")
  mail = echo.using(queue_name="mailers").enqueue("mail")
  default = echo.enqueue("default")

  claimed_jobs = claim_ready_jobs(limit=3, queues=("alpha", "mail*", "*"))

  assert [str(claimed_job.job.id) for claimed_job in claimed_jobs] == [
    alpha.id,
    mail.id,
    default.id,
  ]


@pytest.mark.django_db
def test_queue_selector_exact_group_drains_first_selector_before_next():
  alpha_1 = echo.using(queue_name="alpha").enqueue("alpha-1")
  beta = echo.using(queue_name="beta").enqueue("beta")
  alpha_2 = echo.using(queue_name="alpha").enqueue("alpha-2")

  claimed_jobs = claim_ready_jobs(limit=2, queues=("alpha", "beta"))

  assert [str(claimed_job.job.id) for claimed_job in claimed_jobs] == [
    alpha_1.id,
    alpha_2.id,
  ]
  assert ReadyExecution.objects.filter(job_id=beta.id).exists() is True


@pytest.mark.django_db
def test_queue_selector_exact_group_uses_physical_queue_plan(monkeypatch):
  alpha_1 = echo.using(queue_name="alpha").enqueue("alpha-1")
  alpha_2 = echo.using(queue_name="alpha").enqueue("alpha-2")
  echo.using(queue_name="beta").enqueue("beta")

  def fail_generic_selector_plan(*args, **kwargs):
    raise AssertionError("exact selectors should not use the generic ranked plan")

  monkeypatch.setattr(
    claiming_operations,
    "_ordered_selector_rows_queryset",
    fail_generic_selector_plan,
  )

  claimed_jobs = claim_ready_jobs(limit=2, queues=("alpha", "beta"))

  assert [str(claimed_job.job.id) for claimed_job in claimed_jobs] == [
    alpha_1.id,
    alpha_2.id,
  ]


@pytest.mark.django_db
def test_queue_selector_exact_group_query_budget_stays_claim_sized():
  for queue_name in ("alpha", "alpha", "alpha", "alpha", "beta", "beta"):
    echo.using(queue_name=queue_name).enqueue(queue_name)

  with CaptureQueriesContext(connection) as ctx:
    claimed_jobs = claim_ready_jobs(limit=4, queues=("alpha", "beta"))

  assert len(ctx.captured_queries) <= 10
  assert [claimed_job.job.queue_name for claimed_job in claimed_jobs] == [
    "alpha",
    "alpha",
    "alpha",
    "alpha",
  ]


@pytest.mark.django_db
def test_queue_selector_duplicate_exact_entries_do_not_duplicate_claims():
  result = echo.using(queue_name="alpha").enqueue("alpha")

  claimed_jobs = claim_ready_jobs(limit=2, queues=("alpha", "alpha"))

  assert [str(claimed_job.job.id) for claimed_job in claimed_jobs] == [result.id]


@pytest.mark.skipif(
  os.environ.get("DB_BACKEND", "sqlite") != "postgres",
  reason="requires DB_BACKEND=postgres",
)
@pytest.mark.django_db(transaction=True)
def test_postgres_exact_selector_claim_uses_one_selection_after_earlier_empty():
  for queue_name in ("alpha", "alpha", "alpha", "beta", "beta", "beta"):
    echo.using(queue_name=queue_name).enqueue(queue_name)

  assert [
    claimed_job.job.queue_name
    for claimed_job in claim_ready_jobs(limit=3, queues=("alpha", "beta"))
  ] == ["alpha", "alpha", "alpha"]

  with CaptureQueriesContext(connection) as ctx:
    claimed_jobs = claim_ready_jobs(limit=3, queues=("alpha", "beta"))

  assert [claimed_job.job.queue_name for claimed_job in claimed_jobs] == [
    "beta",
    "beta",
    "beta",
  ]
  assert len(ctx.captured_queries) <= 5


@pytest.mark.django_db
def test_claim_ready_jobs_bulk_inserts_claimed_rows_for_full_batch():
  jobs = [make_job(args=[index]) for index in range(3)]
  for job in jobs:
    ReadyExecution.objects.create(
      job=job,
      backend_alias=job.backend_alias,
      queue_name=job.queue_name,
      priority=job.priority,
    )

  claimed_jobs = claim_ready_jobs(limit=3)

  assert [claimed_job.job.id for claimed_job in claimed_jobs] == [
    jobs[0].id,
    jobs[1].id,
    jobs[2].id,
  ]
  assert ClaimedExecution.objects.count() == 3
  assert ReadyExecution.objects.count() == 0


@pytest.mark.django_db
def test_claim_ready_jobs_rejects_job_with_conflicting_execution_state():
  job = make_job()
  ReadyExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
  )
  FailedExecution.objects.create(
    job=job,
    exception_class="builtins.ValueError",
    message="boom",
    traceback="traceback",
  )

  with pytest.raises(EnqueueError, match="already has an execution-state row"):
    claim_ready_jobs(limit=1)

  assert ClaimedExecution.objects.filter(job=job).exists() is False


@pytest.mark.django_db
def test_claim_ready_jobs_rejects_conflicting_state_with_fixed_query_budget():
  job = make_job()
  ReadyExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
  )
  FailedExecution.objects.create(
    job=job,
    exception_class="builtins.ValueError",
    message="boom",
    traceback="traceback",
  )

  with (
    CaptureQueriesContext(connection) as ctx,
    pytest.raises(EnqueueError, match="already has an execution-state row"),
  ):
    claim_ready_jobs(limit=1)

  if connection.vendor == "postgresql":
    expected_queries = 7
  elif connection.vendor == "mysql":
    expected_queries = 6
  else:
    expected_queries = 5
  assert len(ctx.captured_queries) == expected_queries


@pytest.mark.django_db
def test_claim_ready_jobs_rejects_process_from_another_backend():
  job = make_job()
  ReadyExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
  )
  process = Process.objects.create(
    backend_alias="secondary",
    kind="Worker",
    pid=12345,
    hostname="localhost",
    name="secondary-worker",
    metadata={},
    last_heartbeat_at=timezone.now(),
  )

  with pytest.raises(EnqueueError, match="belongs to backend 'secondary'"):
    claim_ready_jobs(limit=1, process=process)

  assert ReadyExecution.objects.filter(job=job).exists() is True
  assert ClaimedExecution.objects.filter(job=job).exists() is False


@pytest.mark.django_db
def test_claim_ready_jobs_rejects_mismatched_ready_row_backend_alias():
  job = make_job(backend_alias="secondary")
  ReadyExecution.objects.create(
    job=job,
    backend_alias="default",
    queue_name=job.queue_name,
    priority=job.priority,
  )

  with pytest.raises(EnqueueError, match="belongs to backend 'secondary'"):
    claim_ready_jobs(limit=1, backend_alias="default")

  assert ReadyExecution.objects.filter(job=job).exists() is True
  assert ClaimedExecution.objects.filter(job=job).exists() is False


@pytest.mark.django_db
def test_promote_scheduled_jobs_rejects_job_with_conflicting_execution_state():
  job = make_job(scheduled_at=timezone.now() - timedelta(seconds=1))
  ScheduledExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
    scheduled_at=job.scheduled_at,
  )
  ReadyExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
  )

  with pytest.raises(EnqueueError, match="already has an execution-state row"):
    promote_scheduled_jobs(batch_size=10)


@pytest.mark.django_db
def test_promote_scheduled_concurrency_job_rejects_conflicting_execution_state():
  job = make_job(
    task=limited,
    args=[1],
    kwargs={"value": "later"},
    scheduled_at=timezone.now() - timedelta(seconds=1),
    concurrency_key="account:1",
  )
  ScheduledExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
    scheduled_at=job.scheduled_at,
  )
  FailedExecution.objects.create(
    job=job,
    exception_class="builtins.ValueError",
    message="boom",
    traceback="traceback",
  )

  with pytest.raises(EnqueueError, match="already has an execution-state row"):
    promote_scheduled_jobs(batch_size=10)

  assert ScheduledExecution.objects.filter(job=job).exists() is True
  assert ReadyExecution.objects.filter(job=job).exists() is False
  assert FailedExecution.objects.filter(job=job).exists() is True


@pytest.mark.django_db
def test_promote_scheduled_discard_job_rejects_conflicting_execution_state():
  job = make_job(
    task=limited_discard,
    args=[1],
    kwargs={"value": "later"},
    scheduled_at=timezone.now() - timedelta(seconds=1),
    concurrency_key="account:1",
  )
  ScheduledExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
    scheduled_at=job.scheduled_at,
  )
  FailedExecution.objects.create(
    job=job,
    exception_class="builtins.ValueError",
    message="boom",
    traceback="traceback",
  )
  Semaphore.objects.create(
    key="account:1",
    value=0,
    active_count=1,
    limit=1,
    expires_at=timezone.now() + timedelta(seconds=60),
  )

  with pytest.raises(EnqueueError, match="already has an execution-state row"):
    promote_scheduled_jobs(batch_size=10)

  job.refresh_from_db()
  assert job.finished_at is None
  assert ScheduledExecution.objects.filter(job=job).exists() is True
  assert FailedExecution.objects.filter(job=job).exists() is True


@pytest.mark.django_db
def test_cleanup_expired_semaphores():
  Semaphore.objects.create(
    key="expired",
    value=0,
    active_count=1,
    limit=1,
    expires_at=timezone.now() - timedelta(seconds=1),
  )
  Semaphore.objects.create(
    key="fresh",
    value=0,
    active_count=1,
    limit=1,
    expires_at=timezone.now() + timedelta(minutes=1),
  )

  deleted = cleanup_expired_semaphores()

  assert deleted == 1
  assert Semaphore.objects.filter(key="expired").exists() is False
  assert Semaphore.objects.filter(key="fresh").exists() is True


@pytest.mark.django_db
def test_cleanup_expired_semaphores_respects_batch_size():
  now = timezone.now()
  for index in range(3):
    Semaphore.objects.create(
      key=f"expired:{index}",
      value=0,
      active_count=1,
      limit=1,
      expires_at=now - timedelta(seconds=3 - index),
    )

  deleted = cleanup_expired_semaphores(batch_size=2)

  assert deleted == 2
  assert Semaphore.objects.filter(expires_at__lte=now).count() == 1


@pytest.mark.skipif(
  os.environ.get("DB_BACKEND", "sqlite") != "postgres",
  reason="requires DB_BACKEND=postgres",
)
@pytest.mark.django_db(transaction=True)
def test_postgres_semaphore_acquire_uses_one_semaphore_query_per_attempt():
  with CaptureQueriesContext(connection) as ctx:
    assert semaphore_acquire("account:postgres-upsert", limit=1, duration_seconds=60) is True
    assert semaphore_acquire("account:postgres-upsert", limit=1, duration_seconds=60) is False

  assert len(queries_touching(ctx, "dj_queue_semaphores")) == 2


def test_postgres_semaphore_acquire_uses_value_and_valid_placeholders(monkeypatch):
  captured = {}

  class FakeCursor:
    def __enter__(self):
      return self

    def __exit__(self, *args):
      return None

    def execute(self, sql, params):
      captured["sql"] = sql
      captured["params"] = params

    def fetchone(self):
      return (False,)

  monkeypatch.setattr(connections["default"], "cursor", lambda: FakeCursor())
  now = timezone.now()

  assert (
    postgres_sql.semaphore_acquire(
      "default",
      "account:postgres-syntax",
      limit=3,
      expires_at=now + timedelta(seconds=60),
      now=now,
    )
    is False
  )

  table = connection.ops.quote_name(Semaphore._meta.db_table)
  value_column = connection.ops.quote_name("value")
  assert f"WHERE {table}.{value_column} >" in captured["sql"]
  assert captured["sql"].count("%s") == len(captured["params"])


def test_postgres_released_slot_handoff_uses_value_and_valid_placeholders(monkeypatch):
  captured = {}

  class FakeCursor:
    def __enter__(self):
      return self

    def __exit__(self, *args):
      return None

    def execute(self, sql, params):
      captured["sql"] = sql
      captured["params"] = params

    def fetchone(self):
      return None

  monkeypatch.setattr(connections["default"], "cursor", lambda: FakeCursor())
  now = timezone.now()

  assert (
    postgres_sql.consume_next_blocked_job_with_released_slot(
      "default",
      backend_alias="default",
      key="account:postgres-syntax",
      limit=3,
      duration_seconds=60,
      now=now,
      use_skip_locked=True,
    )
    is None
  )

  table = connection.ops.quote_name(Semaphore._meta.db_table)
  value_column = connection.ops.quote_name("value")
  assert f"AND {table}.{value_column} >" in captured["sql"]
  assert captured["sql"].count("%s") == len(captured["params"])


def test_mysql_family_semaphore_acquire_avoids_deprecated_values_function(monkeypatch):
  captured = {}

  class FakeCursor:
    lastrowid = 1

    def __enter__(self):
      return self

    def __exit__(self, *args):
      return None

    def execute(self, sql, params):
      captured["sql"] = sql
      captured["params"] = params

  monkeypatch.setattr(connections["default"], "cursor", lambda: FakeCursor())
  now = timezone.now()

  assert (
    mysql_sql.semaphore_acquire(
      "default",
      "account:mysql-syntax",
      limit=3,
      expires_at=now + timedelta(seconds=60),
      now=now,
    )
    is True
  )

  assert "VALUES(" not in captured["sql"]
  assert captured["sql"].count("%s") == len(captured["params"])


@pytest.mark.skipif(
  os.environ.get("DB_BACKEND", "sqlite") not in {"mysql", "mariadb"},
  reason="requires DB_BACKEND=mysql or DB_BACKEND=mariadb",
)
@pytest.mark.django_db(transaction=True)
def test_mysql_family_semaphore_acquire_uses_one_semaphore_query_per_attempt():
  with CaptureQueriesContext(connection) as ctx:
    assert semaphore_acquire("account:mysql-upsert", limit=1, duration_seconds=60) is True
    assert semaphore_acquire("account:mysql-upsert", limit=1, duration_seconds=60) is False

  assert len(queries_touching(ctx, "dj_queue_semaphores")) == 2
