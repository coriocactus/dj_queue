import os
import select
import threading


class InterruptibleSleeper:
  def __init__(self):
    self._read_fd, self._write_fd = os.pipe()
    os.set_blocking(self._read_fd, False)
    os.set_blocking(self._write_fd, False)
    self._closed = False
    self._lock = threading.Lock()

  def sleep(self, seconds):
    if self._closed:
      return

    ready, _, _ = select.select([self._read_fd], [], [], max(seconds, 0))
    if ready:
      self._drain()

  def wake_up(self):
    with self._lock:
      if self._closed:
        return

      try:
        os.write(self._write_fd, b".")
      except (BlockingIOError, OSError):
        pass

  def close(self):
    with self._lock:
      if self._closed:
        return

      self._closed = True
      os.close(self._read_fd)
      os.close(self._write_fd)

  def _drain(self):
    try:
      while os.read(self._read_fd, 1024):
        continue
    except BlockingIOError:
      return
