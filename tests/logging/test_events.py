import logging

from dj_queue.log import event_logging_enabled, log_event


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


def test_event_logging_enabled_respects_logger_level():
  logger = logging.getLogger("dj_queue")
  original_level = logger.level
  try:
    logger.setLevel(logging.WARNING)

    assert event_logging_enabled(logging.INFO) is False
    assert event_logging_enabled(logging.WARNING) is True
  finally:
    logger.setLevel(original_level)
