from contextlib import nullcontext
from datetime import timedelta
from unittest.mock import Mock

import pytest
from django.db import OperationalError, transaction
from django.utils import timezone

from dj_queue.db import TRANSIENT_DATABASE_RETRY_ATTEMPTS
from dj_queue.models import BlockedExecution, FailedExecution, Job, ReadyExecution, Semaphore
from dj_queue.operations import concurrency
from tests.tasks import echo, limited


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("failure_mode", ["once", "always", "nested"])
def test_blocked_maintenance_retries_whole_owned_transaction(monkeypatch, failure_mode):
  jobs = []
  expired_at = timezone.now() - timedelta(seconds=10)
  for index in range(3):
    job = Job.objects.create(
      task_path=limited.module_path if index else "missing.task",
      backend_alias="default",
      concurrency_key=f"account:{index}",
    )
    jobs.append(job)
    BlockedExecution.objects.create(
      job=job,
      backend_alias=job.backend_alias,
      queue_name=job.queue_name,
      priority=job.priority,
      concurrency_key=job.concurrency_key,
      expires_at=expired_at + timedelta(seconds=index),
    )
  notify = Mock()
  log = Mock()
  create_ready = concurrency._create_ready_execution_locked
  calls = 0
  conflict = OperationalError(1213, "deadlock found when trying to get lock")

  def create_then_conflict(*args, **kwargs):
    nonlocal calls
    calls += 1
    result = create_ready(*args, **kwargs)
    if calls % 2 == 0 and (failure_mode != "once" or calls == 2):
      raise conflict
    return result

  monkeypatch.setattr(concurrency, "_create_ready_execution_locked", create_then_conflict)
  monkeypatch.setattr(concurrency, "notify_ready_queues_on_commit", notify)
  monkeypatch.setattr(concurrency, "log_event", log)

  if failure_mode == "once":
    promoted = concurrency.promote_expired_blocked_jobs()
    assert [job.id for job in promoted] == [job.id for job in jobs[1:]]
    assert calls == 4
    assert ReadyExecution.objects.count() == Semaphore.objects.count() == 2
    assert not BlockedExecution.objects.exists()
    assert FailedExecution.objects.get().job_id == jobs[0].id
    assert notify.call_count == 2
    assert [call.args[0] for call in log.call_args_list] == [
      "job.unblocked",
      "job.unblocked",
      "job.failed",
    ]
    return

  with (
    pytest.raises(OperationalError) as raised,
    transaction.atomic() if failure_mode == "nested" else nullcontext(),
  ):
    if failure_mode == "nested":
      echo.enqueue("caller write")
    concurrency.promote_expired_blocked_jobs()

  assert raised.value is conflict
  attempts = 1 if failure_mode == "nested" else TRANSIENT_DATABASE_RETRY_ATTEMPTS
  assert calls == attempts * 2
  assert Job.objects.count() == BlockedExecution.objects.count() == 3
  assert not ReadyExecution.objects.exists()
  assert not FailedExecution.objects.exists()
  assert not Semaphore.objects.exists()
  notify.assert_not_called()
  log.assert_not_called()
