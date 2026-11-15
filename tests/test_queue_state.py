from datetime import timedelta

import pytest
from django.db import connection
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from dj_queue.models import FailedExecution, Job, ReadyExecution
from dj_queue.queue_state import (
  filter_queue_state,
  queue_state_count_fields,
  queue_state_count_key,
  queue_state_queryset,
  queue_state_summaries_by_queue,
  queue_state_summary,
  status_rank_expression,
)
from tests.factories import (
  enqueue_ready_job,
  make_blocked_job,
  make_failed_job,
  make_job,
  make_raw_ready_job,
  make_scheduled_job,
)

pytestmark = pytest.mark.django_db


def test_queue_state_queryset_applies_state_filter_and_ordering():
  low_priority = enqueue_ready_job(priority=0)
  high_priority = enqueue_ready_job(priority=10)
  make_scheduled_job(scheduled_at=timezone.now())

  jobs = list(queue_state_queryset(backend_alias="default", queue_name="default", state="ready"))

  assert jobs == [high_priority, low_priority]


def test_queue_state_counts_and_count_fields_follow_state_definitions():
  enqueue_ready_job()
  make_scheduled_job(scheduled_at=timezone.now())

  summary = queue_state_summary(backend_alias="default", queue_name="default")

  assert summary.count_fields() == {
    "ready_count": 1,
    "scheduled_count": 1,
    "claimed_count": 0,
    "blocked_count": 0,
    "failed_count": 0,
  }
  assert queue_state_count_key("ready") == "ready_count"
  assert queue_state_count_fields({"ready": 1})["ready_count"] == 1


def test_queue_state_querysets_separate_invalid_jobs():
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

  assert (
    list(queue_state_queryset(backend_alias="default", queue_name="default", state="ready")) == []
  )
  assert list(
    queue_state_queryset(backend_alias="default", queue_name="default", state="invalid")
  ) == [job]


def test_queue_state_summaries_by_queue_count_state_rows():
  now = timezone.now()
  ready = enqueue_ready_job(queue_name="alpha")
  ReadyExecution.objects.filter(job=ready).update(
    queue_name="drifted",
    created_at=now - timedelta(seconds=20),
    latency_started_at=now - timedelta(seconds=10),
  )
  make_blocked_job(
    queue_name="alpha",
    concurrency_key="account:1",
    expires_at=now + timedelta(minutes=1),
  )
  make_scheduled_job(queue_name="beta", scheduled_at=now + timedelta(minutes=5))
  make_failed_job(queue_name="beta")
  make_job(queue_name="alpha", finished_at=now)
  make_raw_ready_job(queue_name="ignored", backend_alias="other")

  summaries = queue_state_summaries_by_queue(backend_alias="default")

  assert sorted(summaries) == ["alpha", "beta", "drifted"]
  alpha = summaries["alpha"].count_fields()
  assert alpha["ready_count"] == 0
  assert alpha["blocked_count"] == 1
  assert "finished_count" not in alpha
  assert alpha["failed_count"] == 0
  assert summaries["drifted"].count_fields()["ready_count"] == 1
  assert summaries["drifted"].oldest_ready_at == now - timedelta(seconds=10)
  assert summaries["beta"].count_fields()["scheduled_count"] == 1
  assert summaries["beta"].count_fields()["failed_count"] == 1


def test_queue_state_summaries_by_queue_read_one_aggregate_per_state():
  enqueue_ready_job(queue_name="alpha")
  make_failed_job(queue_name="beta")

  with CaptureQueriesContext(connection) as captured:
    summaries = queue_state_summaries_by_queue(backend_alias="default")

  assert sorted(summaries) == ["alpha", "beta"]
  assert len(captured.captured_queries) == 5


def test_filter_queue_state_uses_the_canonical_state_definition():
  enqueue_ready_job()
  scheduled = make_scheduled_job(scheduled_at=timezone.now())

  jobs = filter_queue_state(Job.objects.order_by("id"), "scheduled")

  assert list(jobs) == [scheduled]


def test_status_rank_expression_preserves_admin_status_ordering():
  ready = enqueue_ready_job()
  scheduled = make_scheduled_job(scheduled_at=timezone.now())

  ranked = Job.objects.annotate(status_rank=status_rank_expression()).order_by("status_rank")

  assert list(ranked) == [ready, scheduled]
