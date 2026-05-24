import threading
import time

import pytest


_heartbeat_thread_prefix = "dj_queue-heartbeat-"


def _live_heartbeat_threads(*, excluding=frozenset()):
  return [
    thread
    for thread in threading.enumerate()
    if thread not in excluding
    and thread.name.startswith(_heartbeat_thread_prefix)
    and thread.is_alive()
  ]


def _wait_for_no_heartbeat_threads(*, excluding, timeout=1):
  deadline = time.monotonic() + timeout
  while True:
    threads = _live_heartbeat_threads(excluding=excluding)
    if not threads:
      return
    if time.monotonic() >= deadline:
      names = ", ".join(sorted(thread.name for thread in threads))
      pytest.fail(f"leaked heartbeat threads: {names}")
    time.sleep(0.01)


@pytest.fixture(autouse=True)
def no_heartbeat_thread_leaks():
  existing_threads = frozenset(threading.enumerate())
  yield
  _wait_for_no_heartbeat_threads(excluding=existing_threads)
