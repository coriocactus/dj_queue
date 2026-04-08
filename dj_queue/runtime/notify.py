from dj_queue.db import get_database_alias, supports_listen_notify


class NoopWakeupBackend:
  def start(self):
    return None

  def stop(self):
    return None


def build_wakeup_backend(*, backend_alias="default", queues=(), wake_up=None):
  alias = get_database_alias(backend_alias)
  if supports_listen_notify(alias):
    return NoopWakeupBackend()
  return NoopWakeupBackend()
