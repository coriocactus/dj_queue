import pytest
from django.utils import timezone

from dj_queue.models import Job, ReadyExecution, ScheduledExecution
from dj_queue.queue_state import (
  QUEUE_STATE_COUNT_KEYS,
  filter_queue_state,
  queue_state_count_fields,
  queue_state_count_key,
  queue_state_counts,
  queue_state_queryset,
  status_rank_expression,
)


pytestmark = pytest.mark.django_db


def make_job(**overrides):
  return Job.objects.create(
    task_path=overrides.pop("task_path", "tests.tasks.echo"),
    queue_name=overrides.pop("queue_name", "default"),
    priority=overrides.pop("priority", 0),
    payload=overrides.pop("payload", {"args": [], "kwargs": {}}),
    backend_alias=overrides.pop("backend_alias", "default"),
    scheduled_at=overrides.pop("scheduled_at", None),
    **overrides,
  )


def test_queue_state_queryset_applies_state_filter_and_ordering():
  low_priority = make_job(priority=0)
  high_priority = make_job(priority=10)
  scheduled = make_job(scheduled_at=timezone.now())
  ReadyExecution.objects.create(
    job=low_priority,
    backend_alias="default",
    queue_name="default",
    priority=low_priority.priority,
  )
  ReadyExecution.objects.create(
    job=high_priority,
    backend_alias="default",
    queue_name="default",
    priority=high_priority.priority,
  )
  ScheduledExecution.objects.create(
    job=scheduled,
    backend_alias="default",
    queue_name="default",
    priority=scheduled.priority,
    scheduled_at=scheduled.scheduled_at,
  )

  jobs = list(queue_state_queryset(backend_alias="default", queue_name="default", state="ready"))

  assert jobs == [high_priority, low_priority]


def test_queue_state_counts_and_count_fields_follow_state_definitions():
  ready = make_job()
  scheduled = make_job(scheduled_at=timezone.now())
  ReadyExecution.objects.create(
    job=ready,
    backend_alias="default",
    queue_name="default",
    priority=ready.priority,
  )
  ScheduledExecution.objects.create(
    job=scheduled,
    backend_alias="default",
    queue_name="default",
    priority=scheduled.priority,
    scheduled_at=scheduled.scheduled_at,
  )

  counts = queue_state_counts(backend_alias="default", queue_name="default")

  assert counts["ready"] == 1
  assert counts["scheduled"] == 1
  assert queue_state_count_key("ready") == "ready_count"
  assert set(queue_state_count_fields(counts)) == set(QUEUE_STATE_COUNT_KEYS)
  assert queue_state_count_fields(counts)["ready_count"] == 1


def test_filter_queue_state_uses_the_canonical_state_definition():
  ready = make_job()
  scheduled = make_job(scheduled_at=timezone.now())
  ReadyExecution.objects.create(
    job=ready,
    backend_alias="default",
    queue_name="default",
    priority=ready.priority,
  )
  ScheduledExecution.objects.create(
    job=scheduled,
    backend_alias="default",
    queue_name="default",
    priority=scheduled.priority,
    scheduled_at=scheduled.scheduled_at,
  )

  jobs = filter_queue_state(Job.objects.order_by("id"), "scheduled")

  assert list(jobs) == [scheduled]


def test_status_rank_expression_preserves_admin_status_ordering():
  ready = make_job()
  scheduled = make_job(scheduled_at=timezone.now())
  ReadyExecution.objects.create(
    job=ready,
    backend_alias="default",
    queue_name="default",
    priority=ready.priority,
  )
  ScheduledExecution.objects.create(
    job=scheduled,
    backend_alias="default",
    queue_name="default",
    priority=scheduled.priority,
    scheduled_at=scheduled.scheduled_at,
  )

  ranked = Job.objects.annotate(status_rank=status_rank_expression()).order_by("status_rank")

  assert list(ranked) == [ready, scheduled]
