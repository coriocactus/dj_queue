import logging

import pytest
from django.utils import timezone

from dj_queue.log import event_logging_enabled, log_event
from dj_queue.models import Process
from dj_queue.operations.jobs import claim_ready_jobs, enqueue_job_with_dispatch
from tests.tasks import echo


def assert_event_record(record, *, event, level, payload, backend_alias="default"):
  assert record.name == "dj_queue"
  assert record.levelno == level
  assert record.getMessage() == event
  assert record.event == event
  assert record.dj_queue == {"backend_alias": backend_alias, **payload}


def test_structured_event_job_enqueued(caplog):
  payload = {
    "job_id": "job-1",
    "task_path": "tests.tasks.example",
    "queue_name": "default",
    "priority": 5,
  }

  with caplog.at_level(logging.INFO, logger="dj_queue"):
    log_event("job.enqueued", **payload)

  assert_event_record(
    caplog.records[-1],
    event="job.enqueued",
    level=logging.INFO,
    payload=payload,
  )


def test_structured_event_job_executed(caplog):
  payload = {
    "job_id": "job-1",
    "duration_ms": 123,
    "status": "success",
  }

  with caplog.at_level(logging.INFO, logger="dj_queue"):
    log_event("job.executed", **payload)

  assert_event_record(
    caplog.records[-1],
    event="job.executed",
    level=logging.INFO,
    payload=payload,
  )


def test_structured_event_job_failed(caplog):
  payload = {
    "job_id": "job-1",
    "exception_class": "ValueError",
    "message": "boom",
  }

  with caplog.at_level(logging.INFO, logger="dj_queue"):
    log_event("job.failed", **payload)

  assert_event_record(
    caplog.records[-1],
    event="job.failed",
    level=logging.INFO,
    payload=payload,
  )


def test_structured_event_process_replaced(caplog):
  payload = {
    "old_pid": 101,
    "new_pid": 202,
    "kind": "worker",
  }

  with caplog.at_level(logging.INFO, logger="dj_queue"):
    log_event("process.replaced", **payload)

  assert_event_record(
    caplog.records[-1],
    event="process.replaced",
    level=logging.INFO,
    payload=payload,
  )


def test_structured_event_includes_selected_backend_alias(caplog):
  payload = {"queue_name": "default"}

  with caplog.at_level(logging.INFO, logger="dj_queue"):
    log_event("queue.paused", backend_alias="secondary", **payload)

  assert_event_record(
    caplog.records[-1],
    event="queue.paused",
    level=logging.INFO,
    payload=payload,
    backend_alias="secondary",
  )


@pytest.mark.django_db
def test_operation_events_include_selected_backend_alias(settings, caplog):
  settings.TASKS = {
    "secondary": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {"database_alias": "default"},
    }
  }

  with caplog.at_level(logging.INFO, logger="dj_queue"):
    job, _ = enqueue_job_with_dispatch(echo, ("value",), {}, backend_alias="secondary")
    process = Process.objects.using("default").create(
      backend_alias="secondary",
      kind="Worker",
      pid=123,
      hostname="localhost",
      name="worker-1",
      metadata={},
      last_heartbeat_at=timezone.now(),
    )
    claimed_jobs = claim_ready_jobs(limit=1, process=process, backend_alias="secondary")

  assert_event_record(
    caplog.records[-2],
    event="job.enqueued",
    level=logging.INFO,
    payload={
      "job_id": str(job.id),
      "task_path": job.task_path,
      "queue_name": job.queue_name,
      "priority": job.priority,
    },
    backend_alias="secondary",
  )
  assert_event_record(
    caplog.records[-1],
    event="job.claimed",
    level=logging.INFO,
    payload={
      "job_id": str(claimed_jobs[0].job.id),
      "queue_name": job.queue_name,
      "priority": job.priority,
    },
    backend_alias="secondary",
  )


def test_event_logging_enabled_respects_logger_level():
  logger = logging.getLogger("dj_queue")
  original_level = logger.level
  try:
    logger.setLevel(logging.WARNING)

    assert event_logging_enabled(logging.INFO) is False
    assert event_logging_enabled(logging.WARNING) is True
  finally:
    logger.setLevel(original_level)
