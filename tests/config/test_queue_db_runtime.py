import time

import pytest
from django.core.management import call_command
from django.db import connections

from dj_queue.config import WorkerConfig
from dj_queue.models import ClaimedExecution, Job, ReadyExecution
from dj_queue.operations.jobs import claim_ready_jobs
from dj_queue.runtime.worker import Worker
from tests.tasks import echo


pytestmark = pytest.mark.filterwarnings(
  r"ignore:Overriding setting DATABASES can lead to unexpected behavior\.:UserWarning"
)


def _queue_tasks(*, database_alias="queue", use_skip_locked=True):
  return {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {
        "database_alias": database_alias,
        "use_skip_locked": use_skip_locked,
        "workers": [{"queues": "*", "threads": 1, "processes": 1, "polling_interval": 0.01}],
        "dispatchers": [],
        "scheduler": None,
      },
    }
  }


def _sqlite_databases(tmp_path):
  return {
    "default": {
      "ENGINE": "django.db.backends.sqlite3",
      "NAME": str(tmp_path / "default.sqlite3"),
    },
    "queue": {
      "ENGINE": "django.db.backends.sqlite3",
      "NAME": str(tmp_path / "queue.sqlite3"),
    },
  }


def _make_ready_job(alias="queue", **overrides):
  job = Job.objects.using(alias).create(
    task_path=overrides.pop("task_path", echo.module_path),
    queue_name=overrides.pop("queue_name", echo.queue_name),
    priority=overrides.pop("priority", echo.priority),
    payload=overrides.pop("payload", {"args": ["queued"], "kwargs": {}}),
    backend_alias=overrides.pop("backend_alias", "default"),
    scheduled_at=overrides.pop("scheduled_at", None),
    concurrency_key=overrides.pop("concurrency_key", None),
    finished_at=overrides.pop("finished_at", None),
    return_value=overrides.pop("return_value", None),
    **overrides,
  )
  ReadyExecution.objects.using(alias).create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
  )
  return job


def _dj_queue_tables(alias):
  return {
    table_name
    for table_name in connections[alias].introspection.table_names()
    if table_name.startswith("dj_queue_")
  }


def wait_until(predicate, timeout=1):
  deadline = time.monotonic() + timeout
  while time.monotonic() < deadline:
    if predicate():
      return
    time.sleep(0.01)
  assert predicate()


class InlinePool:
  def __init__(self, max_workers):
    self.idle_capacity = max_workers

  def submit(self, fn, *args, **kwargs):
    from concurrent.futures import Future

    future = Future()
    try:
      future.set_result(fn(*args, **kwargs))
    except Exception as exc:
      future.set_exception(exc)
    return future

  def shutdown(self, timeout, *, on_drained=None):
    if on_drained is not None:
      on_drained()
    return True


class FakeSleeper:
  def wake_up(self):
    return None


class FakeWakeupBackend:
  def start(self):
    return None

  def stop(self):
    return None


def test_enqueue_writes_to_queue_db(tmp_path, django_db_blocker, queue_test_settings):
  queue_test_settings(databases=_sqlite_databases(tmp_path), tasks=_queue_tasks())

  with django_db_blocker.unblock():
    call_command("migrate", "dj_queue", database="queue", interactive=False, verbosity=0)

    result = echo.enqueue("queued")

    assert Job.objects.using("queue").filter(pk=result.id).exists() is True
    assert ReadyExecution.objects.using("queue").filter(job_id=result.id).exists() is True
    assert _dj_queue_tables("default") == set()


def test_worker_reads_and_updates_queue_db(tmp_path, django_db_blocker, queue_test_settings):
  queue_test_settings(databases=_sqlite_databases(tmp_path), tasks=_queue_tasks())

  with django_db_blocker.unblock():
    call_command("migrate", "dj_queue", database="queue", interactive=False, verbosity=0)

    job = _make_ready_job()
    worker = Worker(
      WorkerConfig(queues=("*",), threads=1, processes=1, polling_interval=0.01),
      backend_alias="default",
      name="queue-worker",
      pid=12345,
      hostname="localhost",
      sleeper=FakeSleeper(),
      pool=InlinePool(1),
      wakeup_backend=FakeWakeupBackend(),
    )
    worker.start()

    worker.poll_once()

    wait_until(
      lambda: Job.objects.using("queue").filter(pk=job.pk, finished_at__isnull=False).exists()
    )
    assert ClaimedExecution.objects.using("queue").filter(job=job).exists() is False
    assert _dj_queue_tables("default") == set()

    worker.stop()


def test_use_skip_locked_false_preserves_correctness(
  tmp_path, django_db_blocker, queue_test_settings
):
  queue_test_settings(
    databases=_sqlite_databases(tmp_path),
    tasks=_queue_tasks(use_skip_locked=False),
  )

  with django_db_blocker.unblock():
    call_command("migrate", "dj_queue", database="queue", interactive=False, verbosity=0)

    first = _make_ready_job(payload={"args": ["first"], "kwargs": {}}, priority=10)
    second = _make_ready_job(payload={"args": ["second"], "kwargs": {}}, priority=0)

    claimed = claim_ready_jobs(limit=2, backend_alias="default")

    assert [job.pk for job in claimed] == [first.pk, second.pk]
    assert ClaimedExecution.objects.using("queue").count() == 2
    assert ReadyExecution.objects.using("queue").count() == 0


def test_claim_ready_jobs_stays_backend_scoped_on_shared_queue_db(
  tmp_path, django_db_blocker, queue_test_settings
):
  queue_test_settings(
    databases=_sqlite_databases(tmp_path),
    tasks={
      "default": {
        "BACKEND": "dj_queue.backend.DjQueueBackend",
        "QUEUES": [],
        "OPTIONS": {
          "database_alias": "queue",
          "workers": [{"queues": "*", "threads": 1, "processes": 1, "polling_interval": 0.01}],
          "dispatchers": [],
          "scheduler": None,
        },
      },
      "secondary": {
        "BACKEND": "dj_queue.backend.DjQueueBackend",
        "QUEUES": [],
        "OPTIONS": {
          "database_alias": "queue",
          "workers": [{"queues": "*", "threads": 1, "processes": 1, "polling_interval": 0.01}],
          "dispatchers": [],
          "scheduler": None,
        },
      },
    },
  )

  with django_db_blocker.unblock():
    call_command("migrate", "dj_queue", database="queue", interactive=False, verbosity=0)

    default_job = _make_ready_job(backend_alias="default", priority=10)
    secondary_job = _make_ready_job(backend_alias="secondary", priority=20)

    claimed = claim_ready_jobs(limit=2, backend_alias="default")

    assert [job.pk for job in claimed] == [default_job.pk]
    assert ClaimedExecution.objects.using("queue").filter(job=default_job).exists() is True
    assert ReadyExecution.objects.using("queue").filter(job=secondary_job).exists() is True
