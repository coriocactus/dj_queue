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


def test_runner_deleted_process_row_stops_cleanly():
  runner = DummyRunner()
  runner.start()

  Process.objects.filter(pk=runner.process.pk).delete()

  assert runner.should_continue() is False
  runner.stop()
  assert runner.process is None


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
