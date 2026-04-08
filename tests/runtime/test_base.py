import pytest

from dj_queue.runtime.base import app_executor


def test_app_executor_closes_old_connections_on_entry_and_exit(monkeypatch):
  events = []

  def record_close():
    events.append("close")

  monkeypatch.setattr("dj_queue.runtime.base.close_old_connections", record_close)

  with app_executor():
    events.append("body")

  assert events == ["close", "body", "close"]


def test_app_executor_closes_old_connections_on_exit_when_body_raises(monkeypatch):
  events = []

  def record_close():
    events.append("close")

  monkeypatch.setattr("dj_queue.runtime.base.close_old_connections", record_close)

  with pytest.raises(RuntimeError, match="boom"):
    with app_executor():
      events.append("body")
      raise RuntimeError("boom")

  assert events == ["close", "body", "close"]
