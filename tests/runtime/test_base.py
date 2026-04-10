import pytest

from dj_queue.runtime.base import app_executor


def test_app_executor_closes_old_connections_on_entry_and_exit(monkeypatch):
  events = []

  def record_close():
    events.append("old")

  class DummyConnections:
    def close_all(self):
      events.append("all")

  monkeypatch.setattr("dj_queue.runtime.base.close_old_connections", record_close)
  monkeypatch.setattr("dj_queue.runtime.base.connections", DummyConnections())

  with app_executor():
    events.append("body")

  assert events == ["old", "body", "all"]


def test_app_executor_closes_old_connections_on_exit_when_body_raises(monkeypatch):
  events = []

  def record_close():
    events.append("old")

  class DummyConnections:
    def close_all(self):
      events.append("all")

  monkeypatch.setattr("dj_queue.runtime.base.close_old_connections", record_close)
  monkeypatch.setattr("dj_queue.runtime.base.connections", DummyConnections())

  with pytest.raises(RuntimeError, match="boom"):
    with app_executor():
      events.append("body")
      raise RuntimeError("boom")

  assert events == ["old", "body", "all"]
