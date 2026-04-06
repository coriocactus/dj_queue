import logging

from tests import runtime_callbacks

from dj_queue.runtime.base import handle_thread_error


def _tasks_with_callback(callback_path):
  return {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {
        "on_thread_error": callback_path,
      },
    }
  }


def test_on_thread_error_callback_receives_infrastructure_exception(settings):
  runtime_callbacks.reset()
  settings.TASKS = _tasks_with_callback("tests.runtime_callbacks.record_error")
  error = RuntimeError("heartbeat failed")

  handle_thread_error(error, context="heartbeat")

  assert runtime_callbacks.CAPTURED_ERRORS == [error]


def test_on_thread_error_callback_failure_is_isolated(settings, caplog):
  settings.TASKS = _tasks_with_callback("tests.runtime_callbacks.raise_on_error")
  error = RuntimeError("heartbeat failed")

  with caplog.at_level(logging.ERROR, logger="dj_queue"):
    handle_thread_error(error, context="heartbeat")

  assert "on_thread_error callback raised" in caplog.text
