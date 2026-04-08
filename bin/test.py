#!/usr/bin/env -S uv run --script

import os
from pathlib import Path
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
import re
import shutil

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_SERVICES = ["mysql", "mariadb", "postgres"]
TEST_RESULT_CHARS = frozenset(".FsxXEpP")
COLLECTED_TOTAL_RE = re.compile(r"^(\d+) tests? collected\b", re.MULTILINE)
ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")
GREEN = "\033[32m"
YELLOW = "\033[33m"
RED = "\033[31m"
RESET = "\033[0m"


@dataclass(slots=True)
class BackendProgress:
  backend: str
  phase: str = "collecting"
  total: int | None = None
  completed: int = 0
  exit_code: int | None = None
  has_warnings: bool = False
  started_at: float = field(default_factory=time.monotonic)
  in_escape: bool = False


def start_services():
  needed = []
  for svc in DB_SERVICES:
    result = subprocess.run(
      ["docker", "compose", "ps", svc, "--format", "{{.Health}}"],
      capture_output=True,
      text=True,
    )
    if result.stdout.strip().lower() != "healthy":
      needed.append(svc)
  if needed:
    print(f"Starting {', '.join(needed)}...")
    subprocess.run(
      ["docker", "compose", "up", "-d", *needed],
      check=True,
      stdout=subprocess.DEVNULL,
      stderr=subprocess.DEVNULL,
    )


def wait_for_healthy(timeout=60):
  """Wait until all db services report healthy via docker compose."""
  deadline = time.monotonic() + timeout
  pending = set(DB_SERVICES)
  while pending and time.monotonic() < deadline:
    for svc in list(pending):
      result = subprocess.run(
        ["docker", "compose", "ps", svc, "--format", "{{.Health}}"],
        capture_output=True,
        text=True,
      )
      if result.stdout.strip().lower() == "healthy":
        pending.discard(svc)
    if pending:
      time.sleep(1)
  if pending:
    print(f"\033[31mTimed out waiting for: {', '.join(sorted(pending))}\033[0m")
    sys.exit(1)


def parse_collected_total(output):
  match = COLLECTED_TOTAL_RE.search(ANSI_RE.sub("", output))
  if match is None:
    return None
  return int(match.group(1))


def strip_ansi(text):
  return ANSI_RE.sub("", text)


def extract_warning_output(output):
  lines = output.splitlines(True)
  for i, line in enumerate(lines):
    stripped = strip_ansi(line).strip().lower()
    if stripped.startswith("===") and "warnings summary" in stripped:
      return "".join(lines[i:])
  return ""


def line_color(progress):
  if progress.exit_code is None:
    return ""
  if progress.exit_code != 0:
    return RED
  if progress.has_warnings:
    return YELLOW
  return GREEN


def colorize_line(progress, line):
  color = line_color(progress)
  if not color:
    return line
  return f"{color}{line}{RESET}"


def format_duration(seconds):
  total_seconds = max(int(seconds), 0)
  hours, remainder = divmod(total_seconds, 3600)
  minutes, secs = divmod(remainder, 60)
  if hours:
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"
  return f"{minutes:02d}:{secs:02d}"


def format_progress_line(progress, columns=None):
  if columns is None:
    columns = shutil.get_terminal_size(fallback=(100, 24)).columns

  prefix = f"[{progress.backend}] "
  elapsed = time.monotonic() - progress.started_at

  if progress.total is None:
    status = progress.phase.upper() if progress.exit_code is None else "DONE"
    line = f"{prefix}{status} [{format_duration(elapsed)}]"
    return colorize_line(progress, line[:columns])

  completed = min(progress.completed, progress.total)
  ratio = completed / progress.total if progress.total else 0
  percent = int(ratio * 100)

  if completed and elapsed > 0:
    rate = completed / elapsed
    eta = 0 if completed == progress.total else (progress.total - completed) / rate
    rate_text = f"{rate:0.2f} t/s"
    eta_text = format_duration(eta)
  else:
    rate_text = "? t/s"
    eta_text = "?"

  suffix = f" {completed}/{progress.total} [{format_duration(elapsed)}<{eta_text}, {rate_text}]"

  bar_prefix = f"{percent:3d}%|"
  bar_width = columns - len(prefix) - len(bar_prefix) - len(suffix) - 1
  if bar_width < 10:
    line = prefix + f"{percent:3d}% {suffix.lstrip()}"
    return colorize_line(progress, line[:columns])

  filled = int(bar_width * ratio)
  bar = "#" * filled + "-" * (bar_width - filled)
  line = prefix + bar_prefix + bar + "|" + suffix
  return colorize_line(progress, line[:columns])


def draw_backend_line(progress, idx, backends_count, lock):
  with lock:
    offset = backends_count - idx
    sys.stdout.write(f"\033[{offset}A\033[2K\r{format_progress_line(progress)}\033[{offset}B\r")
    sys.stdout.flush()


def collect_total(backend):
  result = subprocess.run(
    ["pytest", "--collect-only", "-q"],
    capture_output=True,
    text=True,
    env={**os.environ, "DB_BACKEND": backend},
  )
  return parse_collected_total(result.stdout)


def note_result_char(progress, char):
  if progress.in_escape:
    if char.isalpha():
      progress.in_escape = False
    return False

  if char == "\033":
    progress.in_escape = True
    return False

  if progress.total is None or progress.completed >= progress.total:
    return False

  if char in TEST_RESULT_CHARS:
    progress.completed += 1
    return True

  return False


def trim_failure_output(output):
  lines = output.splitlines(True)
  for i, line in enumerate(lines):
    if strip_ansi(line).startswith("==="):
      return "".join(lines[i:])
  return output


def run_tests(backend, idx, backends_count, lock, results, progress):
  progress.total = collect_total(backend)
  progress.started_at = time.monotonic()
  progress.phase = "running"
  draw_backend_line(progress, idx, backends_count, lock)

  proc = subprocess.Popen(
    ["pytest", "-q", "--color=yes", "--tb=short"],
    env={**os.environ, "DB_BACKEND": backend, "PYTHONUNBUFFERED": "1"},
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
  )

  full_output = []
  while True:
    char = proc.stdout.read(1)
    if not char:
      break

    full_output.append(char)
    if note_result_char(progress, char):
      draw_backend_line(progress, idx, backends_count, lock)

  exit_code = proc.wait()
  output = "".join(full_output)
  progress.has_warnings = bool(extract_warning_output(output))
  if exit_code == 0 and progress.total is not None:
    progress.completed = progress.total
  progress.exit_code = exit_code
  draw_backend_line(progress, idx, backends_count, lock)

  results[backend] = (exit_code, trim_failure_output(output), extract_warning_output(output))


def main():
  os.chdir(PROJECT_ROOT)

  start_services()
  wait_for_healthy()

  backends = ["sqlite", "mysql", "mariadb", "postgres"]
  lock = threading.Lock()
  results = {}
  progress = {backend: BackendProgress(backend=backend) for backend in backends}

  print("Running tests across databases:")
  for backend in backends:
    print(format_progress_line(progress[backend]))

  sys.stdout.write("\033[?25l\033[?7l")  # hide cursor, disable line wrap
  sys.stdout.flush()

  try:
    threads = []
    for i, backend in enumerate(backends):
      thread = threading.Thread(
        target=run_tests,
        args=(backend, i, len(backends), lock, results, progress[backend]),
      )
      thread.start()
      threads.append(thread)

    for thread in threads:
      thread.join()
  finally:
    sys.stdout.write("\033[?25h\033[?7h")  # restore cursor and line wrap
    sys.stdout.flush()

  failed = False
  for backend in backends:
    code, _, _ = results.get(backend, (1, "", ""))
    if code != 0:
      failed = True

  if failed:
    for backend in backends:
      code, output, _ = results.get(backend, (1, "", ""))
      if code != 0:
        print(f"\n{RED}── {backend} {'─' * (55 - len(backend))}{RESET}")
        print(output.rstrip())

  for backend in backends:
    code, _, warning_output = results.get(backend, (1, "", ""))
    if code == 0 and warning_output:
      label = f"{backend} warnings"
      print(f"\n{YELLOW}── {label} {'─' * (55 - len(label))}{RESET}")
      print(warning_output.rstrip())

  if failed:
    print()
    sys.exit(1)


if __name__ == "__main__":
  main()
