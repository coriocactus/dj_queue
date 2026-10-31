import logging

from django.utils.module_loading import import_string

logger = logging.getLogger("dj_queue")


def handle_thread_error(error, *, context, config):
  callback_path = config.on_thread_error
  if callback_path:
    try:
      callback = import_string(callback_path)
      callback(error)
      return
    except Exception:
      logger.exception(
        "on_thread_error callback raised",
        extra={
          "event": "dj_queue.thread_error_callback_failed",
          "backend_alias": config.backend_alias,
          "thread_error_context": context,
          "on_thread_error": callback_path,
          "thread_error_type": error.__class__.__name__,
        },
      )

  logger.error(
    "dj_queue infrastructure error",
    exc_info=(error.__class__, error, error.__traceback__),
    extra={
      "event": "dj_queue.thread_error",
      "backend_alias": config.backend_alias,
      "thread_error_context": context,
      "thread_error_type": error.__class__.__name__,
    },
  )
