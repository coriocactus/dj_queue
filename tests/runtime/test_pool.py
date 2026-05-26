import threading

import pytest

from dj_queue.runtime.pool import WorkerPool


def wait_until(predicate, timeout=1):
  finished = threading.Event()

  def check():
    if predicate():
      finished.set()
      return True
    return False

  deadline = threading.Timer(timeout, finished.set)
  deadline.start()
  try:
    while not finished.wait(0.01):
      check()
    assert predicate()
  finally:
    deadline.cancel()


def test_worker_pool_closes_thread_connections_on_drained_shutdown(monkeypatch):
  closed_threads = []

  def close_all():
    closed_threads.append(threading.get_ident())

  monkeypatch.setattr("dj_queue.runtime.pool.connections.close_all", close_all)
  pool = WorkerPool(2, wake_up=lambda: None)
  barrier = threading.Barrier(2)

  futures = [pool.submit(lambda: barrier.wait(timeout=1)) for _index in range(2)]
  for future in futures:
    future.result(timeout=1)

  assert closed_threads == []
  assert pool.shutdown(timeout=1) is True
  assert len(set(closed_threads)) == 2


def test_worker_pool_closes_running_thread_connection_after_timeout(monkeypatch):
  closed_threads = []
  started = threading.Event()
  release = threading.Event()

  def close_all():
    closed_threads.append(threading.get_ident())

  def block():
    started.set()
    release.wait(timeout=1)

  monkeypatch.setattr("dj_queue.runtime.pool.connections.close_all", close_all)
  pool = WorkerPool(1, wake_up=lambda: None)
  future = pool.submit(block)
  assert started.wait(timeout=1)

  assert pool.shutdown(timeout=0.01) is False
  assert closed_threads == []

  release.set()
  future.result(timeout=1)
  wait_until(lambda: len(closed_threads) == 1)


def test_worker_pool_rejects_submission_after_shutdown():
  pool = WorkerPool(1, wake_up=lambda: None)

  assert pool.shutdown(timeout=1) is True

  with pytest.raises(RuntimeError, match="worker pool is shutting down"):
    pool.submit(lambda: None)
  assert pool.idle_capacity == 1
