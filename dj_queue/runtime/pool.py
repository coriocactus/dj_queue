from concurrent.futures import ThreadPoolExecutor, wait
import threading

from django.db import connections


class WorkerPool:
  def __init__(self, max_workers, *, wake_up):
    self.max_workers = max_workers
    self._wake_up = wake_up
    self._executor = ThreadPoolExecutor(max_workers=max_workers)
    self._lock = threading.Lock()
    self._futures = set()
    self._in_flight = 0
    self._on_drained = None
    self._closing = threading.Event()
    self._worker_thread_ids = set()

  @property
  def idle_capacity(self):
    with self._lock:
      return max(0, self.max_workers - self._in_flight)

  @property
  def in_flight(self):
    with self._lock:
      return self._in_flight

  def submit(self, fn, *args, **kwargs):
    with self._lock:
      if self._closing.is_set():
        raise RuntimeError("worker pool is shutting down")
      self._in_flight += 1

    try:
      future = self._executor.submit(self._run, fn, args, kwargs)
    except Exception:
      self._forget_unsubmitted_work()
      raise
    with self._lock:
      self._futures.add(future)
    future.add_done_callback(self._complete)
    return future

  def shutdown(self, timeout, *, on_drained=None):
    self._closing.set()
    with self._lock:
      self._on_drained = on_drained
      futures = list(self._futures)
      worker_thread_count = len(self._worker_thread_ids)
    if not futures:
      self._close_worker_connections(worker_thread_count)
      self._executor.shutdown(wait=True, cancel_futures=False)
      self._notify_drained()
      return True

    _, not_done = wait(futures, timeout=timeout)
    drained = not not_done
    cleanup_count = worker_thread_count - len(not_done)
    if cleanup_count > 0:
      self._close_worker_connections(cleanup_count)
    self._executor.shutdown(wait=drained, cancel_futures=False)
    if drained:
      self._notify_drained()
    return drained

  def _run(self, fn, args, kwargs):
    with self._lock:
      self._worker_thread_ids.add(threading.get_ident())
    try:
      return fn(*args, **kwargs)
    finally:
      if self._closing.is_set():
        connections.close_all()

  def _complete(self, future):
    callback = None
    with self._lock:
      self._futures.discard(future)
      self._in_flight -= 1
      if self._in_flight == 0 and not self._futures:
        callback = self._on_drained
        self._on_drained = None
    if callback is not None:
      callback()
    self._wake_up()

  def _forget_unsubmitted_work(self):
    callback = None
    with self._lock:
      self._in_flight -= 1
      if self._in_flight == 0 and not self._futures:
        callback = self._on_drained
        self._on_drained = None
    if callback is not None:
      callback()
    self._wake_up()

  def _close_worker_connections(self, count):
    if count <= 0:
      return None

    barrier = threading.Barrier(count)

    def close_connections():
      try:
        barrier.wait(timeout=1)
      except threading.BrokenBarrierError:
        pass
      connections.close_all()

    futures = [self._executor.submit(close_connections) for _index in range(count)]
    wait(futures, timeout=2)
    return None

  def _notify_drained(self):
    with self._lock:
      callback = self._on_drained
      self._on_drained = None
    if callback is not None:
      callback()
