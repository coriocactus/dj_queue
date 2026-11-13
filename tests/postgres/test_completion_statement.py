import time
from contextlib import contextmanager, nullcontext
from threading import Thread

import pytest
from django.db import OperationalError, connection, connections, transaction
from django.test.utils import CaptureQueriesContext

from dj_queue.config import load_backend_config
from dj_queue.exceptions import EnqueueError
from dj_queue.models import ClaimedExecution, FailedExecution, Job, ReadyExecution, Semaphore
from dj_queue.operations.claiming import claim_ready_jobs
from dj_queue.operations.execution import complete_claimed_job
from dj_queue.operations.recovery import fail_claimed_jobs_for_process
from dj_queue.sql.state import EXECUTION_STATE_MODELS
from tests.factories import make_job, make_process
from tests.tasks import limited

pytestmark = [pytest.mark.postgres, pytest.mark.django_db(transaction=True)]

STATE_CONFLICT = "job {job.id} already has an execution-state row"


def make_claimed_job(**claim):
  job = make_job()
  ClaimedExecution.objects.create(job=job, **claim)
  return job


def complete(job):
  return complete_claimed_job(job, "done", config=load_backend_config())


def first_words(ctx):
  return [query["sql"].split()[0] for query in ctx.captured_queries]


def claimed_row(job):
  # a delete that was rolled back leaves its transaction id in xmax
  with connection.cursor() as cursor:
    cursor.execute(
      "SELECT id, xmax::text FROM dj_queue_claimed_executions WHERE job_id = %s", [job.id]
    )
    return cursor.fetchone()


def add_ready_state(job):
  ReadyExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
  )


def add_failed_state(job):
  FailedExecution.objects.create(job=job, exception_class="builtins.ValueError", message="boom")


def move_job_row_to_another_backend(job):
  Job.objects.filter(pk=job.pk).update(backend_alias="secondary")


def remove_claim(job):
  ClaimedExecution.objects.filter(job=job).delete()


def recover(job):
  remove_claim(job)
  add_failed_state(job)


@contextmanager
def lock_timeout():
  with connection.cursor() as cursor:
    cursor.execute("SET lock_timeout = '100ms'")
  try:
    yield
  finally:
    with connection.cursor() as cursor:
      cursor.execute("RESET lock_timeout")


@contextmanager
def held_claim_lock(job):
  locker = connection.copy()
  try:
    locker.set_autocommit(False)
    with locker.cursor() as cursor:
      cursor.execute(
        "SELECT id FROM dj_queue_claimed_executions WHERE job_id = %s FOR UPDATE", [job.id]
      )
      assert cursor.fetchone() is not None
    yield locker
  finally:
    locker.close()


def wait_for_lock_waiters(count):
  deadline = time.monotonic() + 10
  while time.monotonic() < deadline:
    with connection.cursor() as cursor:
      cursor.execute(
        "SELECT COUNT(*) FROM pg_stat_activity "
        "WHERE datname = current_database() AND wait_event_type = 'Lock'"
      )
      if cursor.fetchone()[0] == count:
        return
  raise AssertionError(f"expected {count} sessions waiting on a lock")


def test_owned_completion_is_one_statement_without_a_transaction_block():
  job = make_claimed_job()
  in_atomic_block = []

  def record_atomic_block(execute, sql, params, many, context):
    in_atomic_block.append(connection.in_atomic_block)
    return execute(sql, params, many, context)

  with connection.execute_wrapper(record_atomic_block), CaptureQueriesContext(connection) as ctx:
    completed = complete(job)

  assert first_words(ctx) == ["WITH"]
  assert in_atomic_block == [False]
  assert claimed_row(job) is None
  stored = Job.objects.get(pk=job.pk)
  assert stored.return_value == completed.return_value == "done"
  assert stored.finished_at == stored.updated_at == completed.finished_at == completed.updated_at
  assert completed.finished_at is not None


@pytest.mark.parametrize(
  ("caller_transaction", "statements"),
  [(nullcontext, ["WITH"]), (transaction.atomic, ["SAVEPOINT", "WITH", "ROLLBACK", "RELEASE"])],
)
@pytest.mark.parametrize(
  ("break_precondition", "error", "message"),
  [
    (add_ready_state, EnqueueError, STATE_CONFLICT),
    (add_failed_state, EnqueueError, STATE_CONFLICT),
    (move_job_row_to_another_backend, EnqueueError, STATE_CONFLICT),
    (remove_claim, ClaimedExecution.DoesNotExist, ""),
    (recover, ClaimedExecution.DoesNotExist, ""),
  ],
)
def test_failed_precondition_raises_the_same_error_and_leaves_the_claimed_row_untouched(
  caller_transaction, statements, break_precondition, error, message
):
  job = make_claimed_job()
  break_precondition(job)
  claimed_row_before = claimed_row(job)

  with caller_transaction():
    with CaptureQueriesContext(connection) as ctx, pytest.raises(error) as raised:
      complete(job)
    caller_write = make_job()

  assert str(raised.value) == message.format(job=job)
  assert first_words(ctx) == statements
  assert claimed_row(job) == claimed_row_before
  assert Job.objects.get(pk=job.pk).finished_at is None
  assert Job.objects.filter(pk=caller_write.pk).exists()


@pytest.mark.parametrize("by_id", [False, True])
def test_job_of_another_backend_is_rejected_before_the_completion_statement(by_id):
  job = make_job(backend_alias="secondary")
  ClaimedExecution.objects.create(job=job)
  claimed_row_before = claimed_row(job)

  with CaptureQueriesContext(connection) as ctx, pytest.raises(ClaimedExecution.DoesNotExist):
    complete(job.id if by_id else job)

  assert first_words(ctx) == ["SELECT"] * by_id
  assert claimed_row(job) == claimed_row_before
  assert Job.objects.get(pk=job.pk).finished_at is None


def test_owned_completion_retries_a_lock_timeout_one_statement_per_attempt():
  job = make_claimed_job()

  def release_after_conflict(execute, sql, params, many, context):
    try:
      return execute(sql, params, many, context)
    except OperationalError:
      locker.rollback()
      raise

  with (
    lock_timeout(),
    held_claim_lock(job) as locker,
    connection.execute_wrapper(release_after_conflict),
    CaptureQueriesContext(connection) as ctx,
  ):
    complete(job)

  assert first_words(ctx) == ["WITH", "WITH"]
  assert claimed_row(job) is None
  assert Job.objects.get(pk=job.pk).return_value == "done"


def test_caller_owned_completion_propagates_a_lock_timeout_into_a_usable_transaction():
  job = make_claimed_job()

  with lock_timeout(), held_claim_lock(job), transaction.atomic():
    with CaptureQueriesContext(connection) as ctx, pytest.raises(OperationalError) as raised:
      complete(job)
    caller_write = make_job()

  assert raised.value.__cause__.sqlstate == "55P03"
  assert first_words(ctx) == ["SAVEPOINT", "WITH", "ROLLBACK", "RELEASE"]
  assert claimed_row(job) is not None
  assert Job.objects.get(pk=job.pk).finished_at is None
  assert Job.objects.filter(pk=caller_write.pk).exists()


@pytest.mark.parametrize("caller_transaction", [nullcontext, transaction.atomic])
@pytest.mark.parametrize("winner", ["completion", "recovery"])
def test_completion_racing_recovery_of_the_same_claim_has_one_winner(caller_transaction, winner):
  process = make_process()
  job = make_claimed_job(process=process)
  errors = {}

  def completion():
    with caller_transaction():
      complete(job)

  def recovery():
    fail_claimed_jobs_for_process(process, RuntimeError("died"), config=load_backend_config())

  def run(actor):
    try:
      actor()
    except Exception as error:
      errors[actor.__name__] = error
    finally:
      connections.close_all()

  # sessions queue on the claimed row in arrival order, so the first one started wins
  actors = [completion, recovery] if winner == "completion" else [recovery, completion]
  threads = [Thread(target=run, args=(actor,)) for actor in actors]
  with held_claim_lock(job) as locker:
    for waiters, thread in enumerate(threads, start=1):
      thread.start()
      wait_for_lock_waiters(waiters)
    locker.rollback()
    for thread in threads:
      thread.join(timeout=10)

  assert not any(thread.is_alive() for thread in threads)
  loser = "recovery" if winner == "completion" else "completion"
  assert list(errors) == [loser]
  assert type(errors[loser]) is ClaimedExecution.DoesNotExist
  assert str(errors[loser]) == ""
  job.refresh_from_db()
  state_rows = [
    model for model in EXECUTION_STATE_MODELS if model.objects.filter(job=job).exists()
  ]
  if winner == "completion":
    assert (job.finished_at is not None, job.return_value, state_rows) == (True, "done", [])
  else:
    assert (job.finished_at, job.return_value, state_rows) == (None, None, [FailedExecution])


def claim_keyed_job():
  limited.enqueue(1, value="first")
  job = claim_ready_jobs(limit=1)[0].job
  assert Semaphore.objects.get(key=job.concurrency_key).value == 0
  return job


def test_keyed_completion_keeps_the_transaction_around_the_statement_and_slot_release():
  job = claim_keyed_job()

  with CaptureQueriesContext(connection) as ctx:
    complete(job)

  assert first_words(ctx) == ["BEGIN", "WITH", "WITH", "UPDATE", "WITH", "COMMIT"]
  assert claimed_row(job) is None
  assert Job.objects.get(pk=job.pk).return_value == "done"
  assert Semaphore.objects.get(key=job.concurrency_key).value == 1


def test_keyed_completion_with_another_execution_state_leaves_the_claim_and_slot_untouched():
  job = claim_keyed_job()
  add_failed_state(job)
  claimed_row_before = claimed_row(job)

  with CaptureQueriesContext(connection) as ctx, pytest.raises(EnqueueError) as raised:
    complete(job)

  assert str(raised.value) == STATE_CONFLICT.format(job=job)
  assert first_words(ctx) == ["BEGIN", "WITH", "ROLLBACK"]
  assert claimed_row(job) == claimed_row_before
  assert Job.objects.get(pk=job.pk).finished_at is None
  assert Semaphore.objects.get(key=job.concurrency_key).value == 0
