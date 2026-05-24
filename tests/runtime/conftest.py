import threading
import time

import pytest


_heartbeat_thread_prefix = "dj_queue-heartbeat-"


def _live_heartbeat_threads():
  return [
    thread
    for thread in threading.enumerate()
    if thread.name.startswith(_heartbeat_thread_prefix) and thread.is_alive()
  ]


def _wait_for_no_heartbeat_threads(timeout=1):
  deadline = time.monotonic() + timeout
  while True:
    threads = _live_heartbeat_threads()
    if not threads:
      return
    if time.monotonic() >= deadline:
      names = ", ".join(sorted(thread.name for thread in threads))
      pytest.fail(f"leaked heartbeat threads: {names}")
    time.sleep(0.01)


@pytest.fixture(autouse=True)
def no_heartbeat_thread_leaks():
  _wait_for_no_heartbeat_threads()
  yield
  _wait_for_no_heartbeat_threads()
