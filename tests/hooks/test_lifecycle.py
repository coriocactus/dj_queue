from unittest.mock import patch

from dj_queue.hooks import clear_hooks, fire_hooks, on_exit, on_start, on_stop, register_hook


def test_hook_order_start_stop_exit():
  events = []
  process = object()

  clear_hooks()

  @on_start
  def first_start(instance):
    events.append(("start", "first", instance is process))

  @on_start
  def second_start(instance):
    events.append(("start", "second", instance is process))

  @on_stop
  def stop(instance):
    events.append(("stop", "only", instance is process))

  @on_exit
  def exit_hook(instance):
    events.append(("exit", "only", instance is process))

  fire_hooks("supervisor.start", process)
  fire_hooks("supervisor.stop", process)
  fire_hooks("supervisor.exit", process)

  assert events == [
    ("start", "first", True),
    ("start", "second", True),
    ("stop", "only", True),
    ("exit", "only", True),
  ]

  clear_hooks()


def test_hook_failure_is_isolated_and_later_hooks_still_fire():
  events = []
  process = object()

  clear_hooks()

  @register_hook("worker.start")
  def broken_hook(instance):
    events.append(("broken", instance is process))
    raise RuntimeError("hook boom")

  @register_hook("worker.start")
  def later_hook(instance):
    events.append(("later", instance is process))

  with patch("dj_queue.hooks.handle_thread_error") as handle_thread_error:
    fire_hooks("worker.start", process)

  assert events == [
    ("broken", True),
    ("later", True),
  ]
  handle_thread_error.assert_called_once()
  error = handle_thread_error.call_args.args[0]
  assert isinstance(error, RuntimeError)
  assert str(error) == "hook boom"
  assert handle_thread_error.call_args.kwargs == {
    "context": "hook:worker.start",
    "backend_alias": "default",
  }

  clear_hooks()
