import os
import threading
import time

from dj_queue.runtime.interruptible import InterruptibleSleeper


def test_interruptible_sleep_returns_after_timeout():
  sleeper = InterruptibleSleeper()

  try:
    started_at = time.monotonic()
    sleeper.sleep(0.05)
    elapsed = time.monotonic() - started_at

    assert elapsed >= 0.04
    assert elapsed < 0.5
  finally:
    sleeper.close()


def test_interruptible_sleep_wakes_immediately():
  sleeper = InterruptibleSleeper()
  finished = threading.Event()
  elapsed = {}

  def run_sleep():
    started_at = time.monotonic()
    sleeper.sleep(60)
    elapsed["value"] = time.monotonic() - started_at
    finished.set()

  thread = threading.Thread(target=run_sleep)
  thread.start()

  try:
    time.sleep(0.01)
    sleeper.wake_up()

    assert finished.wait(timeout=0.2) is True
    assert elapsed["value"] < 0.2
  finally:
    thread.join(timeout=1)
    sleeper.close()


def test_wakeup_before_sleep_is_consumed_once():
  sleeper = InterruptibleSleeper()

  try:
    sleeper.wake_up()

    started_at = time.monotonic()
    sleeper.sleep(60)
    first_elapsed = time.monotonic() - started_at

    started_at = time.monotonic()
    sleeper.sleep(0.05)
    second_elapsed = time.monotonic() - started_at

    assert first_elapsed < 0.2
    assert second_elapsed >= 0.04
    assert second_elapsed < 0.5
  finally:
    sleeper.close()


def test_interruptible_create_close_cycles_do_not_leak_descriptors():
  baseline = len(os.listdir("/dev/fd"))

  for _ in range(200):
    sleeper = InterruptibleSleeper()
    sleeper.close()

  assert len(os.listdir("/dev/fd")) <= baseline + 2
