#!/usr/bin/env -S uv run --script

import os
import subprocess
import sys
import threading
import time

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_SERVICES = ["mysql", "mariadb", "postgres"]


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


# states for progress line display
STREAMING = 0  # normal: show current line
JOINING = 1    # saw [100%], waiting to see if next line is summary or failures
SUMMARY = 2    # showing summary line, freeze on next newline
FROZEN = 3     # done updating the progress line


def run_tests(backend, idx, backends_count, lock, results):
  proc = subprocess.Popen(
    ["pytest", "-q", "--color=yes", "--tb=short"],
    env={**os.environ, "DB_BACKEND": backend, "PYTHONUNBUFFERED": "1"},
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
  )

  line_buf = []
  full_output = []
  in_escape = False
  state = STREAMING

  while True:
    char = proc.stdout.read(1)
    if not char:
      break

    full_output.append(char)

    if state == FROZEN:
      continue

    if char in ("\n", "\r"):
      if state == JOINING:
        # after [100%], join the next line onto the progress bar
        line_buf.append(" ")
        state = SUMMARY
        continue
      if state == SUMMARY:
        state = FROZEN
      line_buf.clear()
      continue

    if char == " " and line_buf and line_buf[-1] == " ":
      continue

    # first visible char after joining — check if it's failures header
    if state == SUMMARY and not in_escape and char == "=":
      state = FROZEN
      continue

    line_buf.append(char)

    if char == "\033":
      in_escape = True
    elif in_escape and char.isalpha():
      in_escape = False

    if not in_escape:
      line_str = "".join(line_buf)
      if state == STREAMING and "[100%]" in line_str:
        state = JOINING
      with lock:
        offset = backends_count - idx
        sys.stdout.write(f"\033[{offset}A\033[2K\r[{backend}] {line_str}\033[{offset}B\r")
        sys.stdout.flush()

  exit_code = proc.wait()
  output = "".join(full_output)
  # strip progress dots, keep from first === separator onward
  for i, line in enumerate(output.splitlines(True)):
    if line.startswith("==="):
      output = "".join(output.splitlines(True)[i:])
      break
  results[backend] = (exit_code, output)


def main():
  start_services()
  wait_for_healthy()

  backends = ["sqlite", "mysql", "mariadb", "postgres"]
  lock = threading.Lock()
  results = {}

  print("Running tests across databases:")
  for backend in backends:
    print(f"[{backend}] Starting...")

  sys.stdout.write("\033[?25l\033[?7l")  # hide cursor, disable line wrap
  sys.stdout.flush()

  try:
    threads = []
    for i, backend in enumerate(backends):
      t = threading.Thread(target=run_tests, args=(backend, i, len(backends), lock, results))
      t.start()
      threads.append(t)

    for t in threads:
      t.join()
  finally:
    sys.stdout.write("\033[?25h\033[?7h")  # restore cursor and line wrap
    sys.stdout.flush()

  failed = False
  for backend in backends:
    code, out = results.get(backend, (1, ""))
    if code != 0:
      failed = True

  if failed:
    for backend in backends:
      code, out = results.get(backend, (1, ""))
      if code != 0:
        print(f"\n\033[31m── {backend} {'─' * (55 - len(backend))}\033[0m")
        print(out.rstrip())
    print()
    sys.exit(1)


if __name__ == "__main__":
  main()
