from contextlib import contextmanager
import time
from types import SimpleNamespace
from uuid import uuid4

import pytest

from dj_queue.hooks import clear_hooks, register_hook
from dj_queue.models import Process
from dj_queue.runtime.base import BaseRunner

pytestmark = pytest.mark.django_db(transaction=True)


class DummyRunner(BaseRunner):
  process_kind = "Worker"
  hook_prefix = "worker"

  def __init__(self, *, polling_interval=0.01, heartbeat_interval=0.01, sleeper=None, name=None):
    super().__init__(
      SimpleNamespace(polling_interval=polling_interval),
      name=name or f"dummy-runner-{uuid4()}",
      pid=12345,
      hostname="localhost",
      sleeper=sleeper,
      heartbeat_interval=heartbeat_interval,
    )
    self.poll_count = 0

  def poll_once(self):
    self.poll_count += 1

  def process_metadata(self):
    return {"polling_interval": self.config.polling_interval}


class FallbackRunner(BaseRunner):
  def poll_once(self):
    return None


def wait_until(predicate, timeout=1):
  deadline = time.monotonic() + timeout
  while time.monotonic() < deadline:
    if predicate():
      return
    time.sleep(0.01)
  assert predicate()


def test_runner_heartbeat_updates_and_stop_deregisters():
  runner = DummyRunner(heartbeat_interval=0.01)
  process = runner.start()
  initial_heartbeat = process.last_heartbeat_at

  wait_until(lambda: Process.objects.get(pk=process.pk).last_heartbeat_at > initial_heartbeat)

  runner.stop()

  assert Process.objects.filter(pk=process.pk).exists() is False


def test_runner_heartbeat_uses_fallback_interval_when_disabled():
  runner = FallbackRunner(
    SimpleNamespace(polling_interval=0.01, process_alive_threshold=0.05),
    name=f"fallback-heartbeat-runner-{uuid4()}",
    pid=12345,
    hostname="localhost",
    heartbeat_interval=0,
  )
  process = runner.start()
  initial_heartbeat = process.last_heartbeat_at

  try:
    wait_until(lambda: Process.objects.get(pk=process.pk).last_heartbeat_at > initial_heartbeat)
  finally:
    runner.stop()

  assert Process.objects.filter(pk=process.pk).exists() is False


def test_runner_deleted_process_row_stops_cleanly():
  runner = DummyRunner()
  runner.start()

  Process.objects.filter(pk=runner.process.pk).delete()

  assert runner.should_continue() is False
  runner.stop()
  assert runner.process is None


def test_runner_liveness_check_uses_app_executor(monkeypatch):
  entered = []

  @contextmanager
  def executor():
    entered.append(True)
    yield

  monkeypatch.setattr("dj_queue.runtime.base.app_executor", executor)
  runner = DummyRunner(heartbeat_interval=60)
  runner.start()

  try:
    assert runner.should_continue() is True
  finally:
    runner.stop()

  assert entered == [True]


def test_managed_poll_loop_reports_runner_stop_as_failure():
  runner = DummyRunner()
  runner.start()

  def stop_during_poll():
    runner.poll_count += 1
    runner.request_stop()

  runner.poll_once = stop_during_poll

  try:
    clean_exit = runner.run_managed_poll_loop(host_stop_requested=lambda: False)
  finally:
    runner.stop()

  assert clean_exit is False
  assert runner.poll_count == 1


def test_runner_start_stop_fires_runner_hooks():
  events = []
  clear_hooks()

  @register_hook("worker.start")
  def on_start(process):
    events.append(("start", process.kind, process.name))

  @register_hook("worker.stop")
  def on_stop(process):
    events.append(("stop", process.kind, process.name))

  @register_hook("worker.exit")
  def on_exit(process):
    events.append(("exit", process.kind, process.name))

  runner = DummyRunner(name="hooked-runner")
  runner.start()
  runner.stop()

  assert events == [
    ("start", "Worker", "hooked-runner"),
    ("stop", "Worker", "hooked-runner"),
    ("exit", "Worker", "hooked-runner"),
  ]

  clear_hooks()


def test_runner_registers_backend_alias_on_process():
  runner = DummyRunner()

  process = runner.start()

  assert process.backend_alias == "default"
  assert process.metadata == {
    "polling_interval": runner.config.polling_interval,
  }
  runner.stop()


@pytest.mark.parametrize("polling_interval", (None, 0, -1, "fast"))
def test_runner_uses_safe_polling_interval_when_config_is_missing_or_invalid(polling_interval):
  if polling_interval is None:
    config = SimpleNamespace()
  else:
    config = SimpleNamespace(polling_interval=polling_interval)

  runner = FallbackRunner(
    config,
    name=f"fallback-runner-{uuid4()}",
    pid=12345,
    hostname="localhost",
    heartbeat_interval=0.01,
  )

  assert runner.polling_interval == 1.0
