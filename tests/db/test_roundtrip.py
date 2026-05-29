import os

import pytest
from django.tasks import TaskResultStatus

from dj_queue.config import WorkerConfig
from dj_queue.models import ClaimedExecution, Job, ReadyExecution
from dj_queue.runtime.worker import Worker
from tests.tasks import echo

pytestmark = pytest.mark.django_db(transaction=True)


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

  def stop(self, *, timeout=None):
    return None


def make_worker():
  return Worker(
    WorkerConfig(queues=("*",), threads=1, processes=1, polling_interval=0.01),
    backend_alias="default",
    name="roundtrip-worker",
    pid=12345,
    hostname="localhost",
    sleeper=FakeSleeper(),
    pool=InlinePool(1),
    wakeup_backend=FakeWakeupBackend(),
  )


def assert_roundtrip():
  result = echo.enqueue("roundtrip")

  assert ReadyExecution.objects.filter(job_id=result.id).exists() is True

  worker = make_worker()
  worker.start()
  worker.poll_once()
  worker.stop()

  job = Job.objects.get(pk=result.id)
  fetched = echo.get_backend().get_result(result.id)

  assert job.return_value == "roundtrip"
  assert job.finished_at is not None
  assert ClaimedExecution.objects.filter(job=job).exists() is False
  assert ReadyExecution.objects.filter(job=job).exists() is False
  assert fetched.status == TaskResultStatus.SUCCESSFUL
  assert fetched.return_value == "roundtrip"


@pytest.mark.mysql
def test_mysql_8_enqueue_claim_execute_roundtrip(django_db_blocker):
  with django_db_blocker.unblock():
    assert_roundtrip()


@pytest.mark.mariadb
def test_mariadb_10_6_enqueue_claim_execute_roundtrip(django_db_blocker):
  with django_db_blocker.unblock():
    assert_roundtrip()


@pytest.mark.skipif(
  os.environ.get("DB_BACKEND", "sqlite") != "sqlite",
  reason="requires DB_BACKEND=sqlite",
)
def test_sqlite_single_worker_roundtrip_without_skip_locked(django_db_blocker, settings):
  settings.TASKS = {
    **settings.TASKS,
    "default": {
      **settings.TASKS["default"],
      "OPTIONS": {
        **settings.TASKS["default"]["OPTIONS"],
        "use_skip_locked": False,
      },
    },
  }

  with django_db_blocker.unblock():
    assert_roundtrip()
