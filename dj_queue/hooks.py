from collections import defaultdict
from collections.abc import Callable
from typing import Any

from dj_queue.runtime.base import handle_thread_error

_hooks: dict[str, list[Callable[..., Any]]] = defaultdict(list)


def register_hook(event: str, fn: Callable[..., Any] | None = None):
  if fn is not None:
    _hooks[event].append(fn)
    return fn

  def decorator(callback: Callable[..., Any]):
    _hooks[event].append(callback)
    return callback

  return decorator


def on_start(fn: Callable[..., Any]):
  return register_hook("supervisor.start", fn)


def on_stop(fn: Callable[..., Any]):
  return register_hook("supervisor.stop", fn)


def on_exit(fn: Callable[..., Any]):
  return register_hook("supervisor.exit", fn)


def on_worker_start(fn: Callable[..., Any]):
  return register_hook("worker.start", fn)


def on_worker_stop(fn: Callable[..., Any]):
  return register_hook("worker.stop", fn)


def on_worker_exit(fn: Callable[..., Any]):
  return register_hook("worker.exit", fn)


def on_dispatcher_start(fn: Callable[..., Any]):
  return register_hook("dispatcher.start", fn)


def on_dispatcher_stop(fn: Callable[..., Any]):
  return register_hook("dispatcher.stop", fn)


def on_dispatcher_exit(fn: Callable[..., Any]):
  return register_hook("dispatcher.exit", fn)


def on_scheduler_start(fn: Callable[..., Any]):
  return register_hook("scheduler.start", fn)


def on_scheduler_stop(fn: Callable[..., Any]):
  return register_hook("scheduler.stop", fn)


def on_scheduler_exit(fn: Callable[..., Any]):
  return register_hook("scheduler.exit", fn)


def fire_hooks(event: str, process: Any, *, backend_alias: str = "default"):
  for hook in tuple(_hooks.get(event, ())):
    try:
      hook(process)
    except Exception as error:
      handle_thread_error(
        error,
        context=f"hook:{event}",
        backend_alias=backend_alias,
      )


def clear_hooks(event: str | None = None):
  if event is None:
    _hooks.clear()
    return
  _hooks.pop(event, None)
