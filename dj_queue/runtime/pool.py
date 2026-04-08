from concurrent.futures import ThreadPoolExecutor, wait
import threading


class WorkerPool:
  def __init__(self, max_workers, *, wake_up):
    self.max_workers = max_workers
    self._wake_up = wake_up
    self._executor = ThreadPoolExecutor(max_workers=max_workers)
    self._lock = threading.Lock()
    self._futures = set()
    self._in_flight = 0
    self._on_drained = None

  @property
  def idle_capacity(self):
    with self._lock:
      return max(0, self.max_workers - self._in_flight)

  def submit(self, fn, *args, **kwargs):
    with self._lock:
      self._in_flight += 1

    future = self._executor.submit(fn, *args, **kwargs)
    with self._lock:
      self._futures.add(future)
    future.add_done_callback(self._complete)
    return future

  def shutdown(self, timeout, *, on_drained=None):
    with self._lock:
      self._on_drained = on_drained
      futures = list(self._futures)
    if not futures:
      self._executor.shutdown(wait=False, cancel_futures=False)
      self._notify_drained()
      return True

    _, not_done = wait(futures, timeout=timeout)
    self._executor.shutdown(wait=False, cancel_futures=False)
    if not not_done:
      self._notify_drained()
    return len(not_done) == 0

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

  def _notify_drained(self):
    with self._lock:
      callback = self._on_drained
      self._on_drained = None
    if callback is not None:
      callback()
