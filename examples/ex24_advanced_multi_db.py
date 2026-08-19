#!/usr/bin/env -S uv run

"""route queue work to a dedicated database alias."""

import tempfile
from pathlib import Path

import django
from _example import ensure_project_on_path, result, step, takeaway, title
from django.conf import settings

ensure_project_on_path()

tempdir = tempfile.TemporaryDirectory()
default_db = Path(tempdir.name) / "default.sqlite3"
queue_db = Path(tempdir.name) / "queue.sqlite3"

settings.configure(
  SECRET_KEY="examples",
  DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
  USE_TZ=True,
  DATABASES={
    "default": {"ENGINE": "django.db.backends.sqlite3", "NAME": str(default_db)},
    "queue": {"ENGINE": "django.db.backends.sqlite3", "NAME": str(queue_db)},
  },
  INSTALLED_APPS=["dj_queue"],
  DATABASE_ROUTERS=["dj_queue.routers.DjQueueRouter"],
  TASKS={
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {"database_alias": "queue"},
    }
  },
)

django.setup()

from django.core.management import call_command
from django.db import connections
from django.tasks import task

from dj_queue.api import claim_ready_jobs
from dj_queue.models import Job, ReadyExecution


@task
def deliver(label):
  return label


def queue_tables(alias):
  return sorted(
    table_name
    for table_name in connections[alias].introspection.table_names()
    if table_name.startswith("dj_queue_")
  )


title("ex24", "route queue tables and queue traffic to a dedicated database alias")

step(1, "migrate dj_queue only on the queue database")
call_command("migrate", "dj_queue", database="queue", interactive=False, verbosity=0)
result(f"default_tables={queue_tables('default')}")
result(f"queue_tables={queue_tables('queue')}")

step(2, "enqueue one task through the default backend alias")
task_result = deliver.enqueue("multi-db")
job = Job.objects.get(pk=task_result.id)
result(f"job_state_db={job._state.db}")
result(f"job_exists_on_queue={Job.objects.using('queue').filter(pk=task_result.id).exists()}")
result(
  f"ready_execution_exists_on_queue={ReadyExecution.objects.using('queue').filter(job_id=task_result.id).exists()}"
)

step(3, "claim ready work through the backend and inspect the queue alias")
claimed_jobs = claim_ready_jobs(limit=1, backend_alias="default")
result(f"claimed_job_db={claimed_jobs[0].job._state.db}")
result(f"remaining_ready_on_queue={ReadyExecution.objects.using('queue').count()}")

takeaway(
  "database_alias and DjQueueRouter keep queue migrations, writes, and reads on the queue database"
)

connections.close_all()
