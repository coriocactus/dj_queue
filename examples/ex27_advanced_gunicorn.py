#!/usr/bin/env -S uv run --with gunicorn

"""launch a real gunicorn server with dj_queue embedded."""

import os
import subprocess
import sys
import tempfile
from pathlib import Path

from _example import (
  ensure_project_on_path,
  find_free_port,
  project_root,
  read_json,
  result,
  step,
  stop_process,
  takeaway,
  title,
  wait_until,
)

ensure_project_on_path()

title("ex27", "launch a real gunicorn server and execute queue work through HTTP")

tempdir = tempfile.TemporaryDirectory()
db_path = Path(tempdir.name) / "gunicorn.sqlite3"
log_path = Path(tempdir.name) / "gunicorn.log"
port = find_free_port()
base_url = f"http://127.0.0.1:{port}"
command = [
  sys.executable,
  "-m",
  "gunicorn",
  "--bind",
  f"127.0.0.1:{port}",
  "--workers",
  "1",
  "--config",
  "python:examples._server_gunicorn_conf",
  "examples._server_app:wsgi_application",
]
environment = {
  **os.environ,
  "DJ_QUEUE_EXAMPLE_SERVER_DB": str(db_path),
}

os.environ["DJ_QUEUE_EXAMPLE_SERVER_DB"] = str(db_path)

from examples import _server_app

_server_app.prepare_database()

step(1, "start gunicorn with dj_queue.contrib.gunicorn hooks")
result(f"command={' '.join(command)}")

with log_path.open("w") as log_file:
  process = subprocess.Popen(
    command,
    cwd=project_root(),
    env=environment,
    stdout=log_file,
    stderr=subprocess.STDOUT,
    text=True,
  )

  try:
    try:
      wait_until(lambda: read_json(f"{base_url}/health").get("ok"), timeout=20.0, interval=0.1)
    except Exception as exc:
      raise RuntimeError(log_path.read_text()) from exc

    result(f"health_url={base_url}/health")

    step(2, "enqueue one task through the WSGI application")
    enqueue_payload = read_json(f"{base_url}/enqueue?value=gunicorn")
    result(f"job_id={enqueue_payload['job_id']}")
    result(f"initial_status={enqueue_payload['status']}")

    step(3, "poll the HTTP result endpoint until the embedded worker finishes the job")

    def result_ready():
      payload = read_json(f"{base_url}/result/{enqueue_payload['job_id']}")
      if payload["status"] != "successful":
        return None
      return payload

    job_payload = wait_until(result_ready, timeout=20.0, interval=0.1)
    result(f"status={job_payload['status']}")
    result(f"return_value={job_payload['return_value']}")
    result(f"worker_processes={job_payload['worker_processes']}")

    takeaway(
      "gunicorn can start one embedded async supervisor inside the worker process via post_fork"
    )
  finally:
    stop_process(process)
