from unittest.mock import patch

import pytest

from dj_queue.runtime.base import app_executor


def test_app_executor_closes_old_connections_on_entry_and_exit():
  events = []

  def record_close():
    events.append("close")

  with patch("dj_queue.runtime.base.close_old_connections", side_effect=record_close):
    with app_executor():
      events.append("body")

  assert events == ["close", "body", "close"]


def test_app_executor_closes_old_connections_on_exit_when_body_raises():
  events = []

  def record_close():
    events.append("close")

  with patch("dj_queue.runtime.base.close_old_connections", side_effect=record_close):
    with pytest.raises(RuntimeError, match="boom"):
      with app_executor():
        events.append("body")
        raise RuntimeError("boom")

  assert events == ["close", "body", "close"]
