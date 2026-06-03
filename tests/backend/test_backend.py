import asyncio
import copy
from datetime import timedelta
import math
from types import SimpleNamespace
import uuid

import pytest
from django.db import connection, transaction
from django.test.utils import CaptureQueriesContext
from django.tasks import TaskResultStatus
from django.tasks.exceptions import TaskResultDoesNotExist
from django.utils import timezone

from dj_queue.api import enqueue_on_commit
from dj_queue.backend import DjQueueBackend, _async_backend_call
from dj_queue.exceptions import DjQueueError, EnqueueError
from dj_queue.models import (
  ClaimedExecution,
  BlockedExecution,
  FailedExecution,
  Job,
  Process,
  ReadyExecution,
  RecurringExecution,
  Semaphore,
  ScheduledExecution,
)
from dj_queue.operations.concurrency import promote_expired_blocked_jobs
from dj_queue.operations.cleanup import (
  clear_failed_jobs,
  clear_finished_jobs,
  clear_recurring_executions,
)
from dj_queue.operations.jobs import (
  claim_ready_jobs,
  DispatchOutcome,
  dispatch_scheduled_job_now,
  discard_failed_job,
  discard_ready_jobs,
  discard_scheduled_jobs,
  enqueue_job_with_dispatch,
  promote_scheduled_jobs,
  retry_failed_job,
  retry_failed_jobs,
)
import dj_queue.operations._helpers as operation_helpers
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


def task_on_queue(task, queue_name):
  queued_task = copy.copy(task)
  object.__setattr__(queued_task, "queue_name", queue_name)
  return queued_task


def task_with_priority(task, priority):
  queued_task = copy.copy(task)
  object.__setattr__(queued_task, "priority", priority)
  return queued_task


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
    for job in Job.objects.order_by("created_at", "id")
  ]


def test_backend_missing_queues_means_any_queue():
  backend = DjQueueBackend(
    "default",
    {"BACKEND": "dj_queue.backend.DjQueueBackend", "OPTIONS": {}},
  )

  assert backend.queues == set()


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
def test_enqueue_immediate_uses_fresh_ready_query_budget():
  with CaptureQueriesContext(connection) as ctx:
    echo.enqueue("ready")

  assert len(ctx.captured_queries) == 5


@pytest.mark.django_db
def test_enqueue_job_with_dispatch_returns_explicit_outcome():
  job, dispatch_outcome = enqueue_job_with_dispatch(echo, ("ready"), {}, backend_alias="default")

  assert dispatch_outcome is DispatchOutcome.READY
  assert ReadyExecution.objects.filter(job=job).exists() is True


@pytest.mark.django_db
def test_enqueue_rejects_queue_outside_backend_allow_list(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": ["default"],
      "OPTIONS": {},
    }
  }
  job_count = Job.objects.count()

  with pytest.raises(EnqueueError, match="queue 'other' is not allowed"):
    echo.get_backend().enqueue(task_on_queue(echo, "other"), ("rejected",), {})

  assert Job.objects.count() == job_count


@pytest.mark.django_db
def test_enqueue_all_rejects_queue_outside_backend_allow_list_atomically(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": ["default"],
      "OPTIONS": {},
    }
  }
  backend = echo.get_backend()

  with pytest.raises(EnqueueError, match="queue 'other' is not allowed"):
    backend.enqueue_all(
      [
        (echo, ("accepted",), {}),
        (task_on_queue(echo, "other"), ("rejected",), {}),
      ]
    )

  assert Job.objects.count() == 0


@pytest.mark.django_db
@pytest.mark.parametrize("priority", (-101, 101, "1", True))
def test_enqueue_rejects_invalid_priority(priority):
  job_count = Job.objects.count()

  with pytest.raises(EnqueueError, match="priority must be an integer from -100 to 100"):
    echo.get_backend().enqueue(task_with_priority(echo, priority), ("rejected",), {})

  assert Job.objects.count() == job_count


@pytest.mark.django_db
def test_enqueue_all_rejects_invalid_priority_atomically():
  backend = echo.get_backend()

  with pytest.raises(EnqueueError, match="priority must be an integer from -100 to 100"):
    backend.enqueue_all(
      [
        (echo, ("accepted",), {}),
        (task_with_priority(echo, 101), ("rejected",), {}),
      ]
    )

  assert Job.objects.count() == 0


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
def test_enqueue_bulk_preserves_claim_order_without_timestamp_sequence():
  backend = echo.get_backend()

  results = backend.enqueue_all(
    [
      (echo, ("one",), {}),
      (echo, ("two",), {}),
      (echo, ("three",), {}),
    ]
  )

  jobs = list(Job.objects.order_by("id"))
  assert all(job.created_at is not None and job.updated_at is not None for job in jobs)

  claimed_jobs = claim_ready_jobs(limit=3)

  assert [str(claimed_job.job.id) for claimed_job in claimed_jobs] == [
    result.id for result in results
  ]


@pytest.mark.django_db
def test_enqueue_bulk_immediate_logs_one_aggregate_event(monkeypatch):
  backend = echo.get_backend()
  calls = []

  monkeypatch.setattr("dj_queue.operations.jobs.event_logging_enabled", lambda **kwargs: True)
  monkeypatch.setattr(
    "dj_queue.operations.jobs.log_event",
    lambda event, **fields: calls.append((event, fields)),
  )

  backend.enqueue_all(
    [
      (echo, ("one",), {}),
      (add, (1, 2), {}),
    ]
  )

  assert calls == [
    (
      "jobs.enqueued",
      {
        "backend_alias": "default",
        "job_count": 2,
        "ready_count": 2,
        "scheduled_count": 0,
        "blocked_count": 0,
        "discarded_count": 0,
      },
    )
  ]


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
def test_enqueue_bulk_mixed_logs_outcome_counts(monkeypatch):
  backend = echo.get_backend()
  future = timezone.now() + timedelta(minutes=5)
  calls = []

  monkeypatch.setattr("dj_queue.operations.jobs.event_logging_enabled", lambda **kwargs: True)
  monkeypatch.setattr(
    "dj_queue.operations.jobs.log_event",
    lambda event, **fields: calls.append((event, fields)),
  )

  backend.enqueue_all(
    [
      (echo, ("immediate",), {}),
      (echo.using(run_after=future), ("later",), {}),
      (limited, (1,), {"value": "first"}),
      (limited, (1,), {"value": "second"}),
    ]
  )

  assert calls == [
    (
      "jobs.enqueued",
      {
        "backend_alias": "default",
        "job_count": 4,
        "ready_count": 2,
        "scheduled_count": 1,
        "blocked_count": 1,
        "discarded_count": 0,
      },
    )
  ]


@pytest.mark.django_db
def test_enqueue_bulk_caches_formatted_concurrency_key_signature(monkeypatch):
  backend = limited.get_backend()
  calls = []
  job_operations = __import__("dj_queue.operations.jobs", fromlist=["inspect"])
  job_operations._task_call_signature.cache_clear()
  original_signature = job_operations.inspect.signature

  def counted_signature(func):
    calls.append(func)
    return original_signature(func)

  monkeypatch.setattr("dj_queue.operations.jobs.inspect.signature", counted_signature)

  backend.enqueue_all(
    [
      (limited, (1,), {"value": "first"}),
      (limited, (1,), {"value": "second"}),
      (limited, (1,), {"value": "third"}),
    ]
  )

  assert calls == [limited.func]


@pytest.mark.django_db
def test_enqueue_bulk_groups_concurrency_slot_acquisition(monkeypatch):
  backend = limited.get_backend()
  acquire_calls = []

  def acquire_many(key, *, count, limit, duration_seconds, backend_alias):
    acquire_calls.append(
      {
        "key": key,
        "count": count,
        "limit": limit,
        "duration_seconds": duration_seconds,
        "backend_alias": backend_alias,
      }
    )
    return 1

  def acquire_one(*args, **kwargs):
    raise AssertionError("single acquire used")

  monkeypatch.setattr("dj_queue.operations.jobs.semaphore_acquire_many", acquire_many)
  monkeypatch.setattr(
    "dj_queue.operations.jobs.semaphore_acquire",
    acquire_one,
  )

  backend.enqueue_all(
    [
      (limited, (1,), {"value": "first"}),
      (limited, (1,), {"value": "second"}),
      (limited, (1,), {"value": "third"}),
    ]
  )

  assert acquire_calls == [
    {
      "key": "account:1",
      "count": 3,
      "limit": 1,
      "duration_seconds": 60,
      "backend_alias": "default",
    }
  ]
  assert ReadyExecution.objects.count() == 1
  assert Job.objects.blocked().count() == 2


@pytest.mark.django_db
def test_enqueue_bulk_grouped_concurrency_preserves_discard_outcome():
  backend = limited_discard.get_backend()

  results = backend.enqueue_all(
    [
      (limited_discard, (1,), {"value": "first"}),
      (limited_discard, (1,), {"value": "second"}),
    ]
  )

  assert [result.status for result in results] == [
    TaskResultStatus.READY,
    TaskResultStatus.SUCCESSFUL,
  ]
  assert ReadyExecution.objects.count() == 1
  assert Job.objects.finished().count() == 1
  assert Job.objects.blocked().count() == 0


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
def test_get_result_failed_does_not_require_task_import():
  job = make_job(task_path="tests.tasks.missing_result_task")
  FailedExecution.objects.create(
    job=job,
    exception_class="builtins.ImportError",
    message="missing",
    traceback="traceback",
  )

  fetched = echo.get_backend().get_result(str(job.id))

  assert fetched.status == TaskResultStatus.FAILED
  assert fetched.task.module_path == job.task_path
  assert fetched.errors[0].exception_class_path == "builtins.ImportError"


@pytest.mark.django_db
def test_get_result_reports_invalid_execution_state_as_failed():
  job = make_job(args=["corrupt"])
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

  fetched = echo.get_backend().get_result(str(job.id))

  assert fetched.status == TaskResultStatus.FAILED
  assert fetched.errors[0].exception_class_path == (
    f"{DjQueueError.__module__}.{DjQueueError.__qualname__}"
  )
  assert "invalid execution state" in fetched.errors[0].traceback


@pytest.mark.django_db
def test_get_result_stays_backend_scoped_on_shared_queue_db():
  job = make_job(args=["secondary"], backend_alias="secondary")

  with pytest.raises(TaskResultDoesNotExist):
    echo.get_backend().get_result(str(job.id))


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
def test_retry_failed_job_rejects_conflicting_execution_state():
  job = make_job(args=["retry"])
  FailedExecution.objects.create(
    job=job,
    exception_class="builtins.ValueError",
    message="boom",
    traceback="traceback",
  )
  ReadyExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
  )

  with pytest.raises(EnqueueError, match="already has an execution-state row"):
    retry_failed_job(job.id)


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
    ReadyExecution.objects.create(
      job=job,
      backend_alias=job.backend_alias,
      queue_name=job.queue_name,
      priority=job.priority,
    )

  deleted = discard_ready_jobs(batch_size=2)

  assert deleted == 2
  assert Job.objects.count() == 1
  assert ReadyExecution.objects.count() == 1


@pytest.mark.django_db(transaction=True)
def test_discard_ready_jobs_skips_rows_consumed_by_another_transition(monkeypatch):
  import dj_queue.operations.jobs as job_operations

  job = make_job()
  ReadyExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
  )

  def consume_elsewhere(alias, model, rows):
    model.objects.using(alias).filter(pk__in=[row.pk for row in rows]).delete()
    return []

  monkeypatch.setattr(job_operations, "_consume_selected_rows", consume_elsewhere)

  deleted = discard_ready_jobs(batch_size=1)

  assert deleted == 0
  assert Job.objects.filter(pk=job.pk).exists() is True
  assert ReadyExecution.objects.filter(job=job).exists() is False


@pytest.mark.django_db
def test_discard_scheduled_jobs_in_batches():
  future = timezone.now() + timedelta(minutes=5)
  for index in range(3):
    job = make_job(args=[index], scheduled_at=future)
    ScheduledExecution.objects.create(
      job=job,
      backend_alias=job.backend_alias,
      queue_name=job.queue_name,
      priority=job.priority,
      scheduled_at=future,
    )

  deleted = discard_scheduled_jobs(batch_size=2)

  assert deleted == 2
  assert Job.objects.count() == 1
  assert ScheduledExecution.objects.count() == 1


@pytest.mark.django_db(transaction=True)
def test_dispatch_scheduled_job_now_skips_rows_consumed_by_another_transition(monkeypatch):
  import dj_queue.operations.jobs as job_operations

  future = timezone.now() + timedelta(minutes=5)
  job = make_job(scheduled_at=future)
  ScheduledExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
    scheduled_at=future,
  )

  def consume_elsewhere(alias, model, rows):
    model.objects.using(alias).filter(pk__in=[row.pk for row in rows]).delete()
    return []

  monkeypatch.setattr(job_operations, "_consume_selected_rows", consume_elsewhere)

  with pytest.raises(EnqueueError, match="job is not scheduled"):
    dispatch_scheduled_job_now(job.id)

  job.refresh_from_db()
  assert job.scheduled_at == future
  assert ReadyExecution.objects.filter(job=job).exists() is False


@pytest.mark.django_db
def test_discard_scheduled_job_does_not_release_semaphore_slot():
  Semaphore.objects.create(
    key="account:1",
    value=0,
    limit=1,
    expires_at=timezone.now() + timedelta(minutes=1),
  )
  future = timezone.now() + timedelta(minutes=5)
  job = make_job(task=limited, args=[1], scheduled_at=future, concurrency_key="account:1")
  ScheduledExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
    scheduled_at=future,
  )

  deleted = discard_scheduled_jobs(job_ids=[job.id], batch_size=1)

  assert deleted == 1
  assert Semaphore.objects.get(key="account:1").value == 0


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
def test_clear_finished_jobs_stays_backend_scoped_on_shared_queue_db():
  default_job = make_job(
    task=echo,
    finished_at=timezone.now() - timedelta(minutes=10),
    return_value="default",
    backend_alias="default",
  )
  secondary_job = make_job(
    task=echo,
    finished_at=timezone.now() - timedelta(minutes=10),
    return_value="secondary",
    backend_alias="secondary",
  )

  deleted = clear_finished_jobs(older_than=60, batch_size=10, backend_alias="default")

  assert deleted == 1
  assert Job.objects.filter(pk=default_job.pk).exists() is False
  assert Job.objects.filter(pk=secondary_job.pk).exists() is True


@pytest.mark.django_db
def test_clear_finished_jobs_locks_job_rows(monkeypatch):
  import dj_queue.operations.cleanup as cleanup_operations

  job = make_job(
    task=echo,
    finished_at=timezone.now() - timedelta(minutes=10),
    return_value="old",
  )
  calls = []
  original_locked_queryset = cleanup_operations.locked_queryset

  def locked(queryset, *, use_skip_locked):
    calls.append(use_skip_locked)
    return original_locked_queryset(queryset, use_skip_locked=use_skip_locked)

  monkeypatch.setattr(cleanup_operations, "locked_queryset", locked)

  deleted = clear_finished_jobs(older_than=60, batch_size=10)

  assert deleted == 1
  assert calls == [True]
  assert Job.objects.filter(pk=job.pk).exists() is False


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
def test_clear_failed_jobs_stays_backend_scoped_on_shared_queue_db():
  default_job = make_job(task=echo, backend_alias="default")
  default_failed = FailedExecution.objects.create(
    job=default_job,
    exception_class="ValueError",
    message="default",
    traceback="default",
  )
  FailedExecution.objects.filter(pk=default_failed.pk).update(
    created_at=timezone.now() - timedelta(minutes=10)
  )

  secondary_job = make_job(task=echo, backend_alias="secondary")
  secondary_failed = FailedExecution.objects.create(
    job=secondary_job,
    exception_class="ValueError",
    message="secondary",
    traceback="secondary",
  )
  FailedExecution.objects.filter(pk=secondary_failed.pk).update(
    created_at=timezone.now() - timedelta(minutes=10)
  )

  deleted = clear_failed_jobs(older_than=60, batch_size=10, backend_alias="default")

  assert deleted == 1
  assert Job.objects.filter(pk=default_job.pk).exists() is False
  assert Job.objects.filter(pk=secondary_job.pk).exists() is True


@pytest.mark.django_db
def test_clear_failed_jobs_locks_failed_rows(monkeypatch):
  import dj_queue.operations.cleanup as cleanup_operations

  job = make_job(task=echo)
  failed = FailedExecution.objects.create(
    job=job,
    exception_class="ValueError",
    message="old",
    traceback="old",
  )
  FailedExecution.objects.filter(pk=failed.pk).update(
    created_at=timezone.now() - timedelta(minutes=10)
  )
  calls = []
  original_locked_queryset = cleanup_operations.locked_queryset

  def locked(queryset, *, use_skip_locked):
    calls.append(use_skip_locked)
    return original_locked_queryset(queryset, use_skip_locked=use_skip_locked)

  monkeypatch.setattr(cleanup_operations, "locked_queryset", locked)

  deleted = clear_failed_jobs(older_than=60, batch_size=10)

  assert deleted == 1
  assert calls == [True]
  assert Job.objects.filter(pk=job.pk).exists() is False


@pytest.mark.django_db(transaction=True)
def test_clear_failed_jobs_skips_rows_consumed_by_another_transition(monkeypatch):
  import dj_queue.operations.cleanup as cleanup_operations

  job = make_job(task=echo)
  failed = FailedExecution.objects.create(
    job=job,
    exception_class="ValueError",
    message="old",
    traceback="old",
  )
  FailedExecution.objects.filter(pk=failed.pk).update(
    created_at=timezone.now() - timedelta(minutes=10)
  )

  def consume_elsewhere(alias, model, rows):
    model.objects.using(alias).filter(pk__in=[row.pk for row in rows]).delete()
    return []

  monkeypatch.setattr(cleanup_operations, "_consume_selected_rows", consume_elsewhere)

  deleted = clear_failed_jobs(older_than=60, batch_size=10)

  assert deleted == 0
  assert Job.objects.filter(pk=job.pk).exists() is True
  assert FailedExecution.objects.filter(job=job).exists() is False


@pytest.mark.django_db(transaction=True)
def test_retry_failed_jobs_skips_rows_consumed_by_another_transition(monkeypatch):
  import dj_queue.operations.jobs as job_operations

  job = make_job(task=echo)
  FailedExecution.objects.create(
    job=job,
    exception_class="ValueError",
    message="old",
    traceback="old",
  )

  def consume_elsewhere(alias, model, rows):
    model.objects.using(alias).filter(pk__in=[row.pk for row in rows]).delete()
    return []

  monkeypatch.setattr(job_operations, "_consume_selected_rows", consume_elsewhere)

  retried = retry_failed_jobs(batch_size=10)

  assert retried == 0
  assert Job.objects.filter(pk=job.pk).exists() is True
  assert ReadyExecution.objects.filter(job=job).exists() is False
  assert FailedExecution.objects.filter(job=job).exists() is False


@pytest.mark.django_db
def test_scheduled_promotion_stays_backend_scoped_on_shared_queue_db():
  due_at = timezone.now() - timedelta(seconds=1)
  default_job = make_job(task=echo, scheduled_at=due_at, backend_alias="default")
  secondary_job = make_job(task=echo, scheduled_at=due_at, backend_alias="secondary")
  for job in (default_job, secondary_job):
    ScheduledExecution.objects.create(
      job=job,
      backend_alias=job.backend_alias,
      queue_name=job.queue_name,
      priority=job.priority,
      scheduled_at=due_at,
    )

  promoted = promote_scheduled_jobs(batch_size=10, backend_alias="default")

  assert [job.pk for job in promoted] == [default_job.pk]
  assert ReadyExecution.objects.filter(job=default_job).exists() is True
  assert ScheduledExecution.objects.filter(job=secondary_job).exists() is True


@pytest.mark.django_db
def test_scheduled_promotion_bulk_promotes_jobs_without_importing_tasks(monkeypatch):
  due_at = timezone.now() - timedelta(seconds=1)
  jobs = [make_job(task=echo, args=[index], scheduled_at=due_at) for index in range(3)]
  for job in jobs:
    ScheduledExecution.objects.create(
      job=job,
      backend_alias=job.backend_alias,
      queue_name=job.queue_name,
      priority=job.priority,
      scheduled_at=due_at,
    )

  def fail_import(_task_path):
    raise AssertionError("simple scheduled promotion should not import tasks")

  monkeypatch.setattr("dj_queue.operations.jobs.import_string", fail_import)

  promoted = promote_scheduled_jobs(batch_size=10)

  assert [job.pk for job in promoted] == [job.pk for job in jobs]
  assert ReadyExecution.objects.count() == len(jobs)
  assert ScheduledExecution.objects.exists() is False


@pytest.mark.django_db
def test_blocked_promotion_stays_backend_scoped_on_shared_queue_db():
  default_job = make_job(
    task=limited,
    args=[1],
    concurrency_key="account:1",
    backend_alias="default",
  )
  secondary_job = make_job(
    task=limited,
    args=[2],
    concurrency_key="account:2",
    backend_alias="secondary",
  )
  for job in (default_job, secondary_job):
    BlockedExecution.objects.create(
      job=job,
      backend_alias=job.backend_alias,
      queue_name=job.queue_name,
      priority=job.priority,
      concurrency_key=job.concurrency_key,
      expires_at=timezone.now() - timedelta(seconds=1),
    )

  promoted = promote_expired_blocked_jobs(batch_size=10, backend_alias="default")

  assert [job.pk for job in promoted] == [default_job.pk]
  assert ReadyExecution.objects.filter(job=default_job).exists() is True
  assert BlockedExecution.objects.filter(job=secondary_job).exists() is True


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
def test_clear_recurring_executions_locks_rows(monkeypatch):
  import dj_queue.operations.cleanup as cleanup_operations

  execution = RecurringExecution.objects.create(
    backend_alias="default",
    task_key="nightly",
    run_at=timezone.now(),
  )
  RecurringExecution.objects.filter(pk=execution.pk).update(
    run_at=timezone.now() - timedelta(minutes=10)
  )
  calls = []
  original_locked_queryset = cleanup_operations.locked_queryset

  def locked(queryset, *, use_skip_locked):
    calls.append(use_skip_locked)
    return original_locked_queryset(queryset, use_skip_locked=use_skip_locked)

  monkeypatch.setattr(cleanup_operations, "locked_queryset", locked)

  deleted = clear_recurring_executions(older_than=60, batch_size=10)

  assert deleted == 1
  assert calls == [True]
  assert RecurringExecution.objects.filter(pk=execution.pk).exists() is False


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
  bulk_results = asyncio.run(async_echo.get_backend().aenqueue_all([(async_echo, ["bulk"], {})]))

  async_fetched = asyncio.run(async_echo.aget_result(result.id))
  sync_fetched = async_echo.get_result(result.id)

  assert async_fetched.status == TaskResultStatus.READY
  assert async_fetched.status == sync_fetched.status
  assert async_fetched.args == sync_fetched.args == ["async"]
  assert async_fetched.kwargs == sync_fetched.kwargs == {}
  assert len(bulk_results) == 1
  assert bulk_results[0].args == ["bulk"]


def test_async_backend_call_uses_obsolete_connection_cleanup_by_default(monkeypatch):
  events = []

  monkeypatch.setattr("dj_queue.backend.close_old_connections", lambda: events.append("old"))
  monkeypatch.setattr(
    "dj_queue.backend.connections",
    type("DummyConnections", (), {"close_all": lambda self: events.append("all")})(),
  )

  result = _async_backend_call(lambda value: value, value="ok")

  assert result == "ok"
  assert events == ["old", "old"]


def test_async_backend_call_can_close_all_thread_owned_connections(monkeypatch):
  events = []

  monkeypatch.setattr("dj_queue.backend.close_old_connections", lambda: events.append("old"))
  monkeypatch.setattr(
    "dj_queue.backend.connections",
    type("DummyConnections", (), {"close_all": lambda self: events.append("all")})(),
  )

  result = _async_backend_call(lambda value: value, close_connections=True, value="ok")

  assert result == "ok"
  assert events == ["old", "all"]


@pytest.mark.django_db
def test_enqueue_rejects_non_json_round_trippable_payload():
  job_count = Job.objects.count()

  with pytest.raises(EnqueueError, match="JSON"):
    echo.enqueue(object())

  assert Job.objects.count() == job_count


@pytest.mark.django_db
def test_enqueue_json_payload_primitives_use_fast_validation(monkeypatch):
  def dumps(*args, **kwargs):
    raise AssertionError("json round trip used")

  monkeypatch.setattr(
    operation_helpers,
    "json",
    SimpleNamespace(dumps=dumps, loads=operation_helpers.json.loads),
  )

  result = echo.enqueue({"items": [1, "two", None, True]})

  assert Job.objects.get(pk=result.id).payload == {
    "args": [{"items": [1, "two", None, True]}],
    "kwargs": {},
  }


@pytest.mark.django_db
def test_enqueue_rejects_non_standard_json_numbers():
  job_count = Job.objects.count()

  with pytest.raises(EnqueueError, match="JSON"):
    echo.enqueue(math.nan)

  assert Job.objects.count() == job_count


@pytest.mark.django_db
@pytest.mark.parametrize(
  ("option", "value", "message"),
  (
    ("concurrency_limit", 0, "concurrency_limit must be a positive integer"),
    ("concurrency_limit", -1, "concurrency_limit must be a positive integer"),
    ("concurrency_limit", True, "concurrency_limit must be a positive integer"),
    ("concurrency_limit", 1.9, "concurrency_limit must be a positive integer"),
    ("concurrency_limit", "many", "concurrency_limit must be a positive integer"),
    ("concurrency_duration", 0, "concurrency_duration must be a positive integer"),
    ("concurrency_duration", -1, "concurrency_duration must be a positive integer"),
    ("concurrency_duration", True, "concurrency_duration must be a positive integer"),
    ("concurrency_duration", 1.9, "concurrency_duration must be a positive integer"),
    ("concurrency_duration", "soon", "concurrency_duration must be a positive integer"),
  ),
)
def test_enqueue_rejects_invalid_concurrency_options(monkeypatch, option, value, message):
  job_count = Job.objects.count()
  monkeypatch.setattr(limited.func, option, value)

  with pytest.raises(EnqueueError, match=message):
    limited.enqueue(1, value="rejected")

  assert Job.objects.count() == job_count
