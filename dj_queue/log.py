import logging
from typing import Any

logger = logging.getLogger("dj_queue")


def event_logging_enabled(level: int = logging.INFO):
  return logger.isEnabledFor(level)


def log_event(
  event: str,
  *,
  level: int = logging.INFO,
  backend_alias: str = "default",
  **fields: Any,
):
  if not event_logging_enabled(level):
    return

  logger.log(
    level,
    event,
    extra={
      "event": event,
      "dj_queue": {"backend_alias": backend_alias, **fields},
    },
  )
