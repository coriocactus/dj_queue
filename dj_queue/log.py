import logging
from typing import Any

logger = logging.getLogger("dj_queue")


def log_event(event: str, *, level: int = logging.INFO, **fields: Any):
  logger.log(
    level,
    event,
    extra={
      "event": event,
      "dj_queue": fields,
    },
  )
