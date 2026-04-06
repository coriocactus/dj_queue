from datetime import timedelta

import pytest
from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
from django.utils import timezone

from dj_queue.models import (
  BlockedExecution,
  ClaimedExecution,
  FailedExecution,
  Job,
  Process,
  ReadyExecution,
  ScheduledExecution,
)

STATUS_NAMES = ("ready", "claimed", "blocked", "scheduled", "failed", "finished")
STATUS_RELATIONS = (
  "ready_execution",
  "scheduled_execution",
  "claimed_execution",
  "blocked_execution",
  "failed_execution",
)


def make_job(**overrides):
  payload = {
    "args": [],
    "kwargs": {},
  }
  payload.update(overrides.pop("payload", {}))

  return Job.objects.create(
    task_path=overrides.pop("task_path", "tests.tasks.example"),
    queue_name=overrides.pop("queue_name", "default"),
    priority=overrides.pop("priority", 0),
    payload=payload,
    backend_name=overrides.pop("backend_name", "default"),
    scheduled_at=overrides.pop("scheduled_at", None),
    concurrency_key=overrides.pop("concurrency_key", None),
    finished_at=overrides.pop("finished_at", None),
    return_value=overrides.pop("return_value", None),
    **overrides,
  )


def make_process(**overrides):
  return Process.objects.create(
    kind=overrides.pop("kind", "Worker"),
    pid=overrides.pop("pid", 12345),
    hostname=overrides.pop("hostname", "localhost"),
    name=overrides.pop("name", "worker-1"),
    metadata=overrides.pop("metadata", {}),
    last_heartbeat_at=overrides.pop("last_heartbeat_at", timezone.now()),
    **overrides,
  )


def assert_job_state(job, expected_status):
  fresh_job = Job.objects.get(pk=job.pk)
  prefetched_job = Job.objects.select_related(*STATUS_RELATIONS).get(pk=job.pk)

  for instance in (fresh_job, prefetched_job):
    assert instance.status == expected_status
    for status_name in STATUS_NAMES:
      assert getattr(instance, status_name) is (status_name == expected_status)


@pytest.mark.django_db
def test_job_create_immediate_has_ready_row():
  job = make_job()

  ready_execution = ReadyExecution.objects.create(
    job=job,
    queue_name=job.queue_name,
    priority=job.priority,
  )

  assert ready_execution.job == job
  assert job.ready_execution == ready_execution
  assert ReadyExecution.objects.filter(job=job).exists() is True


@pytest.mark.django_db
def test_job_create_scheduled_has_scheduled_row():
  job = make_job(scheduled_at=timezone.now() + timedelta(minutes=5))

  scheduled_execution = ScheduledExecution.objects.create(
    job=job,
    queue_name=job.queue_name,
    priority=job.priority,
    scheduled_at=job.scheduled_at,
  )

  assert scheduled_execution.job == job
  assert job.scheduled_execution == scheduled_execution
  assert ScheduledExecution.objects.filter(job=job).exists() is True


@pytest.mark.django_db
def test_live_state_invariant_enforced():
  job = make_job()
  ReadyExecution.objects.create(
    job=job,
    queue_name=job.queue_name,
    priority=job.priority,
  )

  with pytest.raises(ValidationError, match="live execution state"):
    ScheduledExecution.objects.create(
      job=job,
      queue_name=job.queue_name,
      priority=job.priority,
      scheduled_at=timezone.now() + timedelta(minutes=1),
    )


@pytest.mark.django_db
def test_priority_range_constraint():
  with pytest.raises(IntegrityError), transaction.atomic():
    make_job(priority=101)


@pytest.mark.django_db
def test_job_delete_cascades_to_all_execution_rows():
  ready_job = make_job(task_path="tests.tasks.ready")
  scheduled_job = make_job(
    task_path="tests.tasks.scheduled",
    scheduled_at=timezone.now() + timedelta(minutes=5),
  )
  process = make_process()
  claimed_job = make_job(task_path="tests.tasks.claimed")
  blocked_job = make_job(
    task_path="tests.tasks.blocked",
    concurrency_key="account:1",
  )
  failed_job = make_job(task_path="tests.tasks.failed")

  ReadyExecution.objects.create(
    job=ready_job,
    queue_name=ready_job.queue_name,
    priority=ready_job.priority,
  )
  ScheduledExecution.objects.create(
    job=scheduled_job,
    queue_name=scheduled_job.queue_name,
    priority=scheduled_job.priority,
    scheduled_at=scheduled_job.scheduled_at,
  )
  ClaimedExecution.objects.create(job=claimed_job, process=process)
  BlockedExecution.objects.create(
    job=blocked_job,
    queue_name=blocked_job.queue_name,
    priority=blocked_job.priority,
    concurrency_key=blocked_job.concurrency_key,
    expires_at=timezone.now() + timedelta(minutes=5),
  )
  FailedExecution.objects.create(
    job=failed_job,
    exception_class="ValueError",
    message="boom",
    traceback="traceback",
  )

  ready_job.delete()
  scheduled_job.delete()
  claimed_job.delete()
  blocked_job.delete()
  failed_job.delete()

  assert ReadyExecution.objects.exists() is False
  assert ScheduledExecution.objects.exists() is False
  assert ClaimedExecution.objects.exists() is False
  assert BlockedExecution.objects.exists() is False
  assert FailedExecution.objects.exists() is False


@pytest.mark.django_db
def test_job_status_ready():
  job = make_job()
  ReadyExecution.objects.create(
    job=job,
    queue_name=job.queue_name,
    priority=job.priority,
  )

  assert_job_state(job, "ready")


@pytest.mark.django_db
def test_job_status_claimed():
  job = make_job()
  ClaimedExecution.objects.create(job=job, process=make_process())

  assert_job_state(job, "claimed")


@pytest.mark.django_db
def test_job_status_blocked():
  job = make_job(concurrency_key="account:1")
  BlockedExecution.objects.create(
    job=job,
    queue_name=job.queue_name,
    priority=job.priority,
    concurrency_key=job.concurrency_key,
    expires_at=timezone.now() + timedelta(minutes=5),
  )

  assert_job_state(job, "blocked")


@pytest.mark.django_db
def test_job_status_scheduled():
  job = make_job(scheduled_at=timezone.now() + timedelta(minutes=5))
  ScheduledExecution.objects.create(
    job=job,
    queue_name=job.queue_name,
    priority=job.priority,
    scheduled_at=job.scheduled_at,
  )

  assert_job_state(job, "scheduled")


@pytest.mark.django_db
def test_job_status_failed():
  job = make_job()
  FailedExecution.objects.create(
    job=job,
    exception_class="ValueError",
    message="boom",
    traceback="traceback",
  )

  assert_job_state(job, "failed")


@pytest.mark.django_db
def test_job_status_finished():
  job = make_job(finished_at=timezone.now(), return_value={"ok": True})

  assert_job_state(job, "finished")


@pytest.mark.django_db
def test_job_queryset_scopes_match_statuses():
  ready_job = make_job(task_path="tests.tasks.ready")
  ReadyExecution.objects.create(
    job=ready_job,
    queue_name=ready_job.queue_name,
    priority=ready_job.priority,
  )

  claimed_job = make_job(task_path="tests.tasks.claimed")
  ClaimedExecution.objects.create(job=claimed_job, process=make_process(name="worker-2"))

  blocked_job = make_job(task_path="tests.tasks.blocked", concurrency_key="account:2")
  BlockedExecution.objects.create(
    job=blocked_job,
    queue_name=blocked_job.queue_name,
    priority=blocked_job.priority,
    concurrency_key=blocked_job.concurrency_key,
    expires_at=timezone.now() + timedelta(minutes=5),
  )

  scheduled_job = make_job(
    task_path="tests.tasks.scheduled",
    scheduled_at=timezone.now() + timedelta(minutes=5),
  )
  ScheduledExecution.objects.create(
    job=scheduled_job,
    queue_name=scheduled_job.queue_name,
    priority=scheduled_job.priority,
    scheduled_at=scheduled_job.scheduled_at,
  )

  failed_job = make_job(task_path="tests.tasks.failed")
  FailedExecution.objects.create(
    job=failed_job,
    exception_class="ValueError",
    message="boom",
    traceback="traceback",
  )

  finished_job = make_job(
    task_path="tests.tasks.finished",
    finished_at=timezone.now(),
    return_value={"ok": True},
  )

  assert set(Job.objects.ready().values_list("pk", flat=True)) == {ready_job.pk}
  assert set(Job.objects.claimed().values_list("pk", flat=True)) == {claimed_job.pk}
  assert set(Job.objects.blocked().values_list("pk", flat=True)) == {blocked_job.pk}
  assert set(Job.objects.scheduled().values_list("pk", flat=True)) == {scheduled_job.pk}
  assert set(Job.objects.failed().values_list("pk", flat=True)) == {failed_job.pk}
  assert set(Job.objects.finished().values_list("pk", flat=True)) == {finished_job.pk}
