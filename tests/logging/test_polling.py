import logging

from dj_queue.log import log_event


def test_silence_polling_suppresses_dj_queue_poll_noise_only(settings, caplog):
  db_logger = logging.getLogger("django.db.backends")
  original_level = db_logger.level

  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {
        "silence_polling": True,
      },
    }
  }

  caplog.set_level(logging.WARNING)
  with caplog.at_level(logging.DEBUG, logger="dj_queue"):
    log_event("worker.poll", level=logging.DEBUG, polling=True, queue_name="default")
    log_event("job.executed", job_id="job-1", duration_ms=12, status="success")
    db_logger.warning("db logger still visible")

  assert [record.getMessage() for record in caplog.records if record.name == "dj_queue"] == [
    "job.executed"
  ]
  assert any(record.getMessage() == "db logger still visible" for record in caplog.records)
  assert db_logger.level == original_level

  caplog.clear()
  settings.TASKS["default"]["OPTIONS"]["silence_polling"] = False

  with caplog.at_level(logging.DEBUG, logger="dj_queue"):
    log_event("worker.poll", level=logging.DEBUG, polling=True, queue_name="default")

  assert [record.getMessage() for record in caplog.records if record.name == "dj_queue"] == [
    "worker.poll"
  ]
