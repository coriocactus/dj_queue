import asyncio
from datetime import timedelta
import uuid

import pytest
from django.db import transaction
from django.tasks import TaskResultStatus
from django.tasks.exceptions import TaskResultDoesNotExist
from django.utils import timezone

from dj_queue.api import enqueue_on_commit
from dj_queue.backend import _async_backend_call
from dj_queue.exceptions import EnqueueError
from dj_queue.models import (
  ClaimedExecution,
  FailedExecution,
  Job,
  Process,
  ReadyExecution,
  RecurringExecution,
  ScheduledExecution,
)
from dj_queue.operations.cleanup import (
  clear_failed_jobs,
  clear_finished_jobs,
  clear_recurring_executions,
)
from dj_queue.operations.jobs import (
  discard_failed_job,
  discard_ready_jobs,
  discard_scheduled_jobs,
  retry_failed_job,
)
from tests.tasks import add, async_echo, echo, limited, limited_discard


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


def make_process(**overrides):
  return Process.objects.create(
    backend_alias=overrides.pop("backend_alias", "default"),
    kind=overrides.pop("kind", "Worker"),
    pid=overrides.pop("pid", 12345),
    hostname=overrides.pop("hostname", "localhost"),
    name=overrides.pop("name", "worker-1"),
    metadata=overrides.pop("metadata", {}),
    last_heartbeat_at=overrides.pop("last_heartbeat_at", timezone.now()),
    **overrides,
  )


def snapshot_jobs():
  return [
    (
      job.queue_name,
      job.priority,
      job.status,
      tuple(job.payload["args"]),
      tuple(sorted(job.payload["kwargs"].items())),
      job.concurrency_key,
    )
    for job in Job.objects.order_by("created_at")
  ]


@pytest.mark.django_db
def test_enqueue_immediate_uses_ready_path():
  result = echo.enqueue("ready")

  job = Job.objects.get(pk=result.id)

  assert result.status == TaskResultStatus.READY
  assert result.id == str(job.id)
  assert result.task.func is echo.func
  assert result.args == ["ready"]
  assert result.kwargs == {}
  assert ReadyExecution.objects.filter(job=job).exists() is True
  assert ScheduledExecution.objects.exists() is False


@pytest.mark.django_db
def test_enqueue_immediate_does_not_reread_result(monkeypatch):
  backend = echo.get_backend()

  def unexpected_get_result(_result_id):
    raise AssertionError("enqueue should not call get_result")

  monkeypatch.setattr(backend, "get_result", unexpected_get_result)

  result = backend.enqueue(echo, ("ready",), {})

  assert result.status == TaskResultStatus.READY
  assert result.args == ["ready"]
  assert result.kwargs == {}


@pytest.mark.django_db
def test_enqueue_future_uses_scheduled_path():
  future = timezone.now() + timedelta(minutes=5)

  result = echo.using(run_after=future).enqueue("later")

  job = Job.objects.get(pk=result.id)

  assert result.status == TaskResultStatus.READY
  assert ScheduledExecution.objects.filter(job=job, scheduled_at=future).exists() is True
  assert ReadyExecution.objects.exists() is False


@pytest.mark.django_db
def test_enqueue_bulk_immediate_matches_single_enqueue_semantics():
  backend = echo.get_backend()

  echo.enqueue("one")
  add.enqueue(1, 2)
  single_snapshot = snapshot_jobs()

  Job.objects.all().delete()

  results = backend.enqueue_all(
    [
      (echo, ("one",), {}),
      (add, (1, 2), {}),
    ]
  )

  assert [result.status for result in results] == [TaskResultStatus.READY, TaskResultStatus.READY]
  assert snapshot_jobs() == single_snapshot
  assert ReadyExecution.objects.count() == 2


@pytest.mark.django_db
def test_enqueue_bulk_mixed_states():
  backend = echo.get_backend()
  future = timezone.now() + timedelta(minutes=5)

  results = backend.enqueue_all(
    [
      (echo, ("immediate",), {}),
      (echo.using(run_after=future), ("later",), {}),
      (limited, (1,), {"value": "first"}),
      (limited, (1,), {"value": "second"}),
    ]
  )

  assert [result.status for result in results] == [TaskResultStatus.READY] * 4
  assert ReadyExecution.objects.count() == 2
  assert ScheduledExecution.objects.count() == 1
  assert Job.objects.blocked().count() == 1


@pytest.mark.django_db
def test_get_result_ready():
  result = echo.enqueue("ready")

  fetched = echo.get_backend().get_result(result.id)

  assert fetched.status == TaskResultStatus.READY
  assert fetched.args == ["ready"]
  assert fetched.kwargs == {}
  assert fetched.errors == []


@pytest.mark.django_db
def test_get_result_running():
  job = make_job(args=["running"])
  process = make_process()
  ClaimedExecution.objects.create(job=job, process=process)

  fetched = echo.get_backend().get_result(str(job.id))

  assert fetched.status == TaskResultStatus.RUNNING
  assert fetched.started_at is not None
  assert fetched.last_attempted_at == fetched.started_at
  assert fetched.worker_ids == [process.name]


@pytest.mark.django_db
def test_get_result_successful():
  finished_at = timezone.now()
  job = make_job(args=["ok"], finished_at=finished_at, return_value={"ok": True})

  fetched = echo.get_backend().get_result(str(job.id))

  assert fetched.status == TaskResultStatus.SUCCESSFUL
  assert fetched.finished_at == finished_at
  assert fetched.return_value == {"ok": True}


@pytest.mark.django_db
def test_get_result_failed():
  job = make_job(args=["boom"])
  FailedExecution.objects.create(
    job=job,
    exception_class="builtins.ValueError",
    message="boom",
    traceback="traceback",
  )

  fetched = echo.get_backend().get_result(str(job.id))

  assert fetched.status == TaskResultStatus.FAILED
  assert len(fetched.errors) == 1
  assert fetched.errors[0].exception_class_path == "builtins.ValueError"
  assert fetched.errors[0].traceback == "traceback"
  with pytest.raises(ValueError, match="Task failed"):
    _ = fetched.return_value


@pytest.mark.django_db
def test_retry_failed_job_reuses_normal_dispatch_path():
  job = make_job(args=["retry"])
  FailedExecution.objects.create(
    job=job,
    exception_class="builtins.ValueError",
    message="boom",
    traceback="traceback",
  )

  retry_failed_job(job.id)

  job.refresh_from_db()

  assert FailedExecution.objects.filter(job=job).exists() is False
  assert ReadyExecution.objects.filter(job=job).exists() is True


@pytest.mark.django_db
def test_discard_failed_job_removes_job():
  job = make_job()
  FailedExecution.objects.create(
    job=job,
    exception_class="builtins.ValueError",
    message="boom",
    traceback="traceback",
  )

  deleted = discard_failed_job(job.id)

  assert deleted == 1
  assert Job.objects.filter(pk=job.pk).exists() is False


@pytest.mark.django_db
def test_discard_ready_jobs_in_batches():
  for index in range(3):
    job = make_job(args=[index])
    ReadyExecution.objects.create(job=job, queue_name=job.queue_name, priority=job.priority)

  deleted = discard_ready_jobs(batch_size=2)

  assert deleted == 2
  assert Job.objects.count() == 1
  assert ReadyExecution.objects.count() == 1


@pytest.mark.django_db
def test_discard_scheduled_jobs_in_batches():
  future = timezone.now() + timedelta(minutes=5)
  for index in range(3):
    job = make_job(args=[index], scheduled_at=future)
    ScheduledExecution.objects.create(
      job=job,
      queue_name=job.queue_name,
      priority=job.priority,
      scheduled_at=future,
    )

  deleted = discard_scheduled_jobs(batch_size=2)

  assert deleted == 2
  assert Job.objects.count() == 1
  assert ScheduledExecution.objects.count() == 1


@pytest.mark.django_db
def test_clear_finished_jobs_by_age_and_task_path():
  old_finished = make_job(
    task=echo,
    finished_at=timezone.now() - timedelta(minutes=10),
    return_value="old",
  )
  make_job(
    task=add,
    finished_at=timezone.now() - timedelta(minutes=10),
    return_value=3,
  )
  recent_finished = make_job(
    task=echo,
    finished_at=timezone.now() - timedelta(seconds=10),
    return_value="recent",
  )

  deleted = clear_finished_jobs(older_than=60, task_path=echo.module_path, batch_size=10)

  assert deleted == 1
  assert Job.objects.filter(pk=old_finished.pk).exists() is False
  assert Job.objects.filter(pk=recent_finished.pk).exists() is True
  assert Job.objects.filter(task_path=add.module_path).exists() is True


@pytest.mark.django_db
def test_clear_failed_jobs_by_age_and_task_path():
  old_job = make_job(task=echo)
  old_failed = FailedExecution.objects.create(
    job=old_job,
    exception_class="ValueError",
    message="old",
    traceback="old",
  )
  FailedExecution.objects.filter(pk=old_failed.pk).update(
    created_at=timezone.now() - timedelta(minutes=10)
  )

  other_job = make_job(task=add)
  other_failed = FailedExecution.objects.create(
    job=other_job,
    exception_class="ValueError",
    message="other",
    traceback="other",
  )
  FailedExecution.objects.filter(pk=other_failed.pk).update(
    created_at=timezone.now() - timedelta(minutes=10)
  )

  recent_job = make_job(task=echo)
  recent_failed = FailedExecution.objects.create(
    job=recent_job,
    exception_class="ValueError",
    message="recent",
    traceback="recent",
  )
  FailedExecution.objects.filter(pk=recent_failed.pk).update(
    created_at=timezone.now() - timedelta(seconds=10)
  )

  deleted = clear_failed_jobs(older_than=60, task_path=echo.module_path, batch_size=10)

  assert deleted == 1
  assert Job.objects.filter(pk=old_job.pk).exists() is False
  assert Job.objects.filter(pk=other_job.pk).exists() is True
  assert Job.objects.filter(pk=recent_job.pk).exists() is True


@pytest.mark.django_db
def test_clear_recurring_executions_by_age_and_task_key():
  old_execution = RecurringExecution.objects.create(
    backend_alias="default",
    task_key="nightly",
    run_at=timezone.now(),
  )
  RecurringExecution.objects.filter(pk=old_execution.pk).update(
    run_at=timezone.now() - timedelta(minutes=10)
  )

  other_execution = RecurringExecution.objects.create(
    backend_alias="default",
    task_key="hourly",
    run_at=timezone.now(),
  )
  RecurringExecution.objects.filter(pk=other_execution.pk).update(
    run_at=timezone.now() - timedelta(minutes=10)
  )

  recent_execution = RecurringExecution.objects.create(
    backend_alias="default",
    task_key="nightly",
    run_at=timezone.now(),
  )
  RecurringExecution.objects.filter(pk=recent_execution.pk).update(
    run_at=timezone.now() - timedelta(seconds=10)
  )

  deleted = clear_recurring_executions(older_than=60, task_key="nightly", batch_size=10)

  assert deleted == 1
  assert RecurringExecution.objects.filter(pk=old_execution.pk).exists() is False
  assert RecurringExecution.objects.filter(pk=other_execution.pk).exists() is True
  assert RecurringExecution.objects.filter(pk=recent_execution.pk).exists() is True


@pytest.mark.django_db
def test_on_conflict_discard_takes_terminal_discard_path():
  limited_discard.enqueue(1, value="first")

  result = limited_discard.enqueue(1, value="second")
  job = Job.objects.get(pk=result.id)

  assert result.status == TaskResultStatus.SUCCESSFUL
  assert job.finished_at is not None
  assert ReadyExecution.objects.filter(job=job).exists() is False
  assert Job.objects.blocked().filter(pk=job.pk).exists() is False
  assert Job.objects.claimed().filter(pk=job.pk).exists() is False
  assert Job.objects.failed().filter(pk=job.pk).exists() is False


@pytest.mark.django_db
def test_get_result_missing_id_raises_task_result_does_not_exist():
  with pytest.raises(TaskResultDoesNotExist):
    echo.get_backend().get_result(str(uuid.uuid4()))


@pytest.mark.django_db(transaction=True)
def test_enqueue_on_commit_helper_defers_insert_until_commit():
  job_count = Job.objects.count()

  with transaction.atomic():
    enqueue_on_commit(echo, "later")
    assert Job.objects.count() == job_count

  assert Job.objects.count() == job_count + 1
  assert Job.objects.order_by("created_at").last().payload == {"args": ["later"], "kwargs": {}}


@pytest.mark.django_db(transaction=True)
def test_enqueue_on_commit_helper_rollback_drops_work():
  job_count = Job.objects.count()

  with pytest.raises(RuntimeError):
    with transaction.atomic():
      enqueue_on_commit(echo, "never")
      raise RuntimeError("rollback")

  assert Job.objects.count() == job_count


@pytest.mark.django_db(transaction=True)
def test_async_backend_variants_match_sync_behavior():
  result = asyncio.run(async_echo.aenqueue("async"))

  async_fetched = asyncio.run(async_echo.aget_result(result.id))
  sync_fetched = async_echo.get_result(result.id)

  assert async_fetched.status == TaskResultStatus.READY
  assert async_fetched.status == sync_fetched.status
  assert async_fetched.args == sync_fetched.args == ["async"]
  assert async_fetched.kwargs == sync_fetched.kwargs == {}


def test_async_backend_call_closes_thread_owned_connections(monkeypatch):
  events = []

  monkeypatch.setattr("dj_queue.backend.close_old_connections", lambda: events.append("old"))
  monkeypatch.setattr(
    "dj_queue.backend.connections",
    type("DummyConnections", (), {"close_all": lambda self: events.append("all")})(),
  )

  result = _async_backend_call(lambda value: value, value="ok")

  assert result == "ok"
  assert events == ["old", "all"]


@pytest.mark.django_db
def test_enqueue_rejects_non_json_round_trippable_payload():
  job_count = Job.objects.count()

  with pytest.raises(EnqueueError, match="JSON"):
    echo.enqueue(object())

  assert Job.objects.count() == job_count
