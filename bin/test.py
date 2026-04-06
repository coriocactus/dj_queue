#!/usr/bin/env -S uv run --script

import os
import subprocess
import sys
import threading

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def start_services():
  services = ["mysql", "mariadb", "postgres"]
  for svc in services:
    result = subprocess.run(
      ["docker", "compose", "ps", svc, "--status", "running", "--format", "{{.Names}}"],
      capture_output=True,
      text=True,
    )
    if not result.stdout.strip():
      print(f"Starting {svc}...")
      subprocess.run(["docker", "compose", "up", "-d", svc], check=True)


def run_tests(backend, idx, backends_count, lock, results):
  proc = subprocess.Popen(
    ["pytest", "-q", "--color=yes"],
    env={**os.environ, "DB_BACKEND": backend, "PYTHONUNBUFFERED": "1"},
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
  )

  line_buf = []
  full_output = []
  in_escape = False

  while True:
    char = proc.stdout.read(1)
    if not char:
      break

    full_output.append(char)

    if char in ("\n", "\r"):
      char = " "

    if char == " " and line_buf and line_buf[-1] == " ":
      continue

    line_buf.append(char)

    if char == "\033":
      in_escape = True
    elif in_escape and char.isalpha():
      in_escape = False

    if not in_escape:
      line_str = "".join(line_buf)
      with lock:
        offset = backends_count - idx
        sys.stdout.write(f"\033[{offset}A\033[2K\r[{backend}] {line_str}\033[{offset}B\r")
        sys.stdout.flush()

  exit_code = proc.wait()
  results[backend] = (exit_code, "".join(full_output))


def main():
  start_services()

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
      print(f"\n\033[31m{'=' * 20} {backend} ERRORS {'=' * 20}\033[0m")
      print(out)

  if failed:
    sys.exit(1)


if __name__ == "__main__":
  main()
