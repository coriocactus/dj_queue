from datetime import timedelta
from threading import Event, Thread
from unittest.mock import Mock

import pytest
from django.db import OperationalError, connection, connections, transaction
from django.utils import timezone

from dj_queue.models import ClaimedExecution, FailedExecution, Job, Semaphore
from dj_queue.operations import enqueue, execution
from dj_queue.operations.claiming import claim_ready_jobs
from tests.tasks import echo, fail, limited

pytestmark = pytest.mark.django_db(transaction=True)


def test_nested_enqueue_propagates_conflict_and_rolls_back_caller(monkeypatch):
  notifications = Mock()
  dispatch = enqueue.dispatch_job
  attempts = 0

  def conflict_once(*args, **kwargs):
    nonlocal attempts
    attempts += 1
    outcome = dispatch(*args, **kwargs)
    if attempts == 1:
      raise OperationalError("database is locked")
    return outcome

  monkeypatch.setattr("dj_queue.wakeup.supports_listen_notify", lambda alias: True)
  monkeypatch.setattr("dj_queue.runtime.notify.notify_ready_queues", notifications)
  with pytest.raises(OperationalError), transaction.atomic():
    echo.enqueue("caller write")
    monkeypatch.setattr(enqueue, "dispatch_job", conflict_once)
    echo.enqueue("nested write")

  assert attempts == 1
  assert not Job.objects.exists()
  notifications.assert_not_called()


@pytest.mark.parametrize("fails", [False, True])
def test_nested_terminal_lock_timeout_rolls_back_caller_without_repeating_task(monkeypatch, fails):
  if connection.vendor == "sqlite":
    pytest.skip("requires row locks")
  (fail if fails else echo).enqueue("result")
  claimed = claim_ready_jobs(limit=1)[0]
  notifications = Mock()
  call_task = Mock(wraps=execution._call_task)
  monkeypatch.setattr(execution, "_call_task", call_task)
  monkeypatch.setattr("dj_queue.wakeup.supports_listen_notify", lambda alias: True)
  monkeypatch.setattr("dj_queue.runtime.notify.notify_ready_queues", notifications)
  postgres = connection.vendor == "postgresql"
  timeout_name = "lock_timeout" if postgres else "innodb_lock_wait_timeout"
  with connection.cursor() as cursor:
    cursor.execute("SHOW lock_timeout" if postgres else f"SELECT @@SESSION.{timeout_name}")
    original_timeout = cursor.fetchone()[0]
    cursor.execute(f"SET SESSION {timeout_name} = %s", ["100ms" if postgres else 1])

  locker = connection.copy()
  conflicts = []

  def release_after_conflict(execute, sql, params, many, context):
    try:
      return execute(sql, params, many, context)
    except OperationalError as error:
      conflicts.append(error)
      locker.rollback()
      raise

  try:
    locker.set_autocommit(False)
    with locker.cursor() as cursor:
      cursor.execute(
        "SELECT id FROM dj_queue_claimed_executions WHERE job_id = %s FOR UPDATE",
        [Job._meta.pk.get_db_prep_value(claimed.job.id, locker)],
      )
      assert cursor.fetchone() is not None
    with (
      connection.execute_wrapper(release_after_conflict),
      pytest.raises(OperationalError),
      transaction.atomic(),
    ):
      echo.enqueue("caller write")
      execution.execute_claimed_job(claimed)
  finally:
    locker.close()
    with connection.cursor() as cursor:
      cursor.execute(f"SET SESSION {timeout_name} = %s", [original_timeout])

  assert len(conflicts) == 1
  call_task.assert_called_once()
  notifications.assert_not_called()
  assert Job.objects.count() == 1
  assert ClaimedExecution.objects.filter(job=claimed.job).exists()
  assert not FailedExecution.objects.exists()
  claimed.job.refresh_from_db()
  assert claimed.job.finished_at is None


def test_mysql_deadlock_propagates_original_error_from_nested_enqueue(monkeypatch):
  if connection.vendor != "mysql":
    pytest.skip("requires MySQL-family transaction invalidation")
  Semaphore.objects.bulk_create(
    [
      Semaphore(
        key=f"account:{index}", value=1, limit=1, expires_at=timezone.now() + timedelta(minutes=1)
      )
      for index in range(32)
    ]
  )
  other_locked = Event()
  caller_locked = Event()
  errors = []
  notifications = Mock()
  dispatch = Mock(wraps=enqueue.dispatch_job)

  def hold_larger_transaction():
    try:
      with transaction.atomic():
        for index in range(1, 32):
          Semaphore.objects.filter(key=f"account:{index}").update(value=0)
        other_locked.set()
        assert caller_locked.wait(5)
        Semaphore.objects.filter(key="account:0").update(value=0)
    except Exception as error:
      errors.append(error)
    finally:
      connections.close_all()

  monkeypatch.setattr("dj_queue.wakeup.supports_listen_notify", lambda alias: True)
  monkeypatch.setattr("dj_queue.runtime.notify.notify_ready_queues", notifications)
  thread = Thread(target=hold_larger_transaction, name="deadlock-holder")
  thread.start()
  try:
    assert other_locked.wait(5)
    with pytest.raises(OperationalError) as raised, transaction.atomic():
      echo.enqueue("caller write")
      Semaphore.objects.filter(key="account:0").update(value=0)
      caller_locked.set()
      monkeypatch.setattr(enqueue, "dispatch_job", dispatch)
      limited.enqueue(1)
    assert raised.value.args[0] == 1213
  finally:
    caller_locked.set()
    thread.join(timeout=10)
  assert not thread.is_alive()
  assert errors == []
  dispatch.assert_called_once()
  notifications.assert_not_called()
  assert not Job.objects.exists()
