import logging
from typing import Any

from dj_queue.config import load_backend_config

logger = logging.getLogger("dj_queue")


def event_logging_enabled(
  level: int = logging.INFO,
  *,
  backend_alias: str = "default",
  polling: bool = False,
):
  if polling and load_backend_config(backend_alias).silence_polling:
    return False
  return logger.isEnabledFor(level)


def log_event(
  event: str,
  *,
  level: int = logging.INFO,
  backend_alias: str = "default",
  polling: bool = False,
  **fields: Any,
):
  if not event_logging_enabled(level, backend_alias=backend_alias, polling=polling):
    return

  logger.log(
    level,
    event,
    extra={
      "event": event,
      "dj_queue": {"backend_alias": backend_alias, **fields},
    },
  )
