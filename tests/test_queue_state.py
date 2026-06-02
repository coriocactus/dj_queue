from datetime import timedelta

import pytest
from django.db import connection
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from dj_queue.models import FailedExecution, Job, ReadyExecution
from dj_queue.queue_state import (
  QUEUE_STATE_COUNT_KEYS,
  filter_queue_state,
  queue_state_count_fields,
  queue_state_count_key,
  queue_state_counts,
  queue_state_summaries_by_queue,
  queue_state_queryset,
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

  counts = queue_state_counts(backend_alias="default", queue_name="default")

  assert counts["ready"] == 1
  assert counts["scheduled"] == 1
  assert queue_state_count_key("ready") == "ready_count"
  assert set(queue_state_count_fields(counts)) == set(QUEUE_STATE_COUNT_KEYS)
  assert queue_state_count_fields(counts)["ready_count"] == 1


def test_queue_state_counts_report_invalid_jobs_without_double_counting():
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

  counts = queue_state_counts(backend_alias="default", queue_name="default")

  assert counts["ready"] == 0
  assert counts["failed"] == 0
  assert counts["invalid"] == 1
  assert (
    list(queue_state_queryset(backend_alias="default", queue_name="default", state="ready")) == []
  )
  assert list(
    queue_state_queryset(backend_alias="default", queue_name="default", state="invalid")
  ) == [job]


def test_queue_state_summaries_by_queue_follow_canonical_job_rows():
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

  assert sorted(summaries) == ["alpha", "beta"]
  alpha = summaries["alpha"]
  assert alpha.count("ready") == 1
  assert alpha.count("blocked") == 1
  assert alpha.count("finished") == 1
  assert alpha.count("failed") == 0
  assert alpha.count_fields()["ready_count"] == 1
  assert alpha.oldest_ready_at == now - timedelta(seconds=10)
  assert summaries["beta"].count("scheduled") == 1
  assert summaries["beta"].count("failed") == 1


def test_queue_state_summaries_by_queue_uses_one_canonical_aggregate_for_live_counts():
  enqueue_ready_job(queue_name="alpha")
  make_failed_job(queue_name="beta")

  with CaptureQueriesContext(connection) as captured:
    summaries = queue_state_summaries_by_queue(backend_alias="default")

  assert sorted(summaries) == ["alpha", "beta"]
  sql = "\n".join(query["sql"] for query in captured.captured_queries)
  assert "dj_queue_jobs" in sql
  assert "dj_queue_ready_executions" in sql
  assert "dj_queue_failed_executions" in sql
  assert len(captured.captured_queries) == 1


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
