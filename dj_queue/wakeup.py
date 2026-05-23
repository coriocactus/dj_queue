from functools import partial

from django.db import transaction

from dj_queue.config import load_backend_config
from dj_queue.db import supports_listen_notify
from dj_queue.runtime import notify as runtime_notify


def notify_ready_queues_on_commit(queue_names, *, backend_alias="default"):
  ready_queue_names = tuple(dict.fromkeys(queue_names))
  if not ready_queue_names:
    return None

  config = load_backend_config(backend_alias)
  alias = config.database_alias
  if not config.listen_notify or not supports_listen_notify(alias):
    return None

  transaction.on_commit(
    partial(runtime_notify.notify_ready_queues, ready_queue_names, backend_alias=backend_alias),
    using=alias,
  )
  return None
