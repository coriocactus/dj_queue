import json
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib.request import urlopen


def _style(text: str, *codes: str) -> str:
  if not sys.stdout.isatty():
    return text
  prefix = "".join(f"\033[{code}m" for code in codes)
  return f"{prefix}{text}\033[0m"


def project_root() -> Path:
  return Path(__file__).resolve().parent.parent


def ensure_project_on_path() -> None:
  root = str(project_root())
  if root not in sys.path:
    sys.path.insert(0, root)


def title(example_id: str, description: str) -> None:
  print(_style(f"# {example_id}: {description}", "35"))


def step(number: int, description: str) -> None:
  print()
  print(f"[{number:02d}] {_style(description, '1')}")


def result(message: str) -> None:
  print(f">>>> {message}")


def takeaway(message: str) -> None:
  line = f"[!!] {message}"
  print()
  print(_style(line, "32"))


def status_name(status) -> str:
  return str(getattr(status, "value", status)).lower()


def wait_until(check, *, timeout: float = 2.0, interval: float = 0.01):
  deadline = time.monotonic() + timeout
  last_error = None
  while time.monotonic() < deadline:
    try:
      value = check()
    except Exception as exc:
      last_error = exc
      value = None
    if value:
      return value
    time.sleep(interval)

  try:
    value = check()
  except Exception as exc:
    last_error = exc
    value = None
  if value:
    return value
  if last_error is not None:
    raise RuntimeError(f"condition not met within {timeout:.2f}s") from last_error
  raise RuntimeError(f"condition not met within {timeout:.2f}s")


def find_free_port() -> int:
  with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    return sock.getsockname()[1]


def read_json(url: str, *, timeout: float = 1.0) -> dict:
  with urlopen(url, timeout=timeout) as response:
    return json.loads(response.read().decode("utf-8"))


def stop_process(process: subprocess.Popen, *, timeout: float = 5.0) -> None:
  if process.poll() is not None:
    return

  process.terminate()
  try:
    process.wait(timeout=timeout)
  except subprocess.TimeoutExpired:
    process.kill()
    process.wait(timeout=timeout)
