import fcntl
import os
from pathlib import Path


class PidFile:
  def __init__(self, path, *, pid=None, probe=None):
    self.path = Path(path)
    self.pid = os.getpid() if pid is None else pid
    self._probe = probe or _process_alive
    self._file = None

  def acquire(self):
    self.path.parent.mkdir(parents=True, exist_ok=True)
    lock_file = self.path.open("a+")
    try:
      fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
      existing_pid = _read_pid_file(lock_file)
      lock_file.close()
      if existing_pid is not None and self._probe(existing_pid):
        raise RuntimeError(f"a dj_queue supervisor is already running (pid={existing_pid})")
      raise RuntimeError("a dj_queue supervisor is already running")

    lock_file.seek(0)
    lock_file.truncate()
    lock_file.write(str(self.pid))
    lock_file.flush()
    os.fsync(lock_file.fileno())
    self._file = lock_file

  def release(self):
    lock_file = self._file
    self._file = None
    if lock_file is None:
      return
    try:
      if _read_pid_file(lock_file) == self.pid:
        self.path.unlink(missing_ok=True)
    except OSError:
      pass
    try:
      fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
    finally:
      lock_file.close()


def _read_pid(path):
  try:
    return int(path.read_text().strip())
  except (OSError, TypeError, ValueError):
    return None


def _read_pid_file(lock_file):
  try:
    lock_file.seek(0)
    return int(lock_file.read().strip())
  except (OSError, TypeError, ValueError):
    return None


def _process_alive(pid):
  try:
    os.kill(pid, 0)
  except (ProcessLookupError, PermissionError):
    return False
  return True
