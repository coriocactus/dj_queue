import os
from pathlib import Path


class PidFile:
  def __init__(self, path, *, pid=None, probe=None):
    self.path = Path(path)
    self.pid = os.getpid() if pid is None else pid
    self._probe = probe or _process_alive

  def acquire(self):
    if self.path.exists():
      existing_pid = _read_pid(self.path)
      if existing_pid is not None and self._probe(existing_pid):
        raise RuntimeError(f"a dj_queue supervisor is already running (pid={existing_pid})")

    self.path.parent.mkdir(parents=True, exist_ok=True)
    self.path.write_text(str(self.pid))

  def release(self):
    try:
      self.path.unlink(missing_ok=True)
    except OSError:
      pass


def _read_pid(path):
  try:
    return int(path.read_text().strip())
  except (OSError, TypeError, ValueError):
    return None


def _process_alive(pid):
  try:
    os.kill(pid, 0)
  except (ProcessLookupError, PermissionError):
    return False
  return True
