import logging
import os
import threading
import time
from concurrent.futures import Future

import pytest
from django.db import connections

from dj_queue.config import WorkerConfig
from dj_queue.models import BlockedExecution, ClaimedExecution, Job, Semaphore
from dj_queue.runtime.worker import Worker
from tests.runtime.test_supervisor import wait_until
from tests.tasks import limited

logger = logging.getLogger("dj_queue.stress")


pytestmark = pytest.mark.django_db(transaction=True)


class BlockingPool:
  def __init__(self, max_workers, *, started, release, active_count, max_active, lock):
    self._max_workers = max_workers
    self._started = started
    self._release = release
    self._active_count = active_count
    self._max_active = max_active
    self._lock = lock
    self._in_flight = 0

  @property
  def idle_capacity(self):
    with self._lock:
      return max(0, self._max_workers - self._in_flight)

  def submit(self, fn, *args, **kwargs):
    future = Future()
    with self._lock:
      self._in_flight += 1

    def run():
      with self._lock:
        self._active_count[0] += 1
        self._max_active[0] = max(self._max_active[0], self._active_count[0])
      self._started.set()
      try:
        self._release.wait(timeout=2)
        future.set_result(fn(*args, **kwargs))
      except Exception as exc:
        future.set_exception(exc)
      finally:
        with self._lock:
          self._active_count[0] -= 1
          self._in_flight -= 1
        connections.close_all()

    threading.Thread(target=run, daemon=True).start()
    return future

  def shutdown(self, timeout, *, on_drained=None):
    self._release.set()
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


def make_worker(index, *, pool):
  return Worker(
    WorkerConfig(queues=("*",), threads=1, processes=1, polling_interval=0.1),
    backend_alias="default",
    name=f"load-worker-{index}",
    pid=12000 + index,
    hostname="localhost",
    sleeper=FakeSleeper(),
    pool=pool,
    wakeup_backend=FakeWakeupBackend(),
  )


def drain_workers(workers, *, total_jobs, timeout):
  deadline = time.monotonic() + timeout
  while time.monotonic() < deadline:
    for worker in workers:
      worker.poll_once()
    if Job.objects.filter(finished_at__isnull=False).count() == total_jobs:
      return
    time.sleep(0.01)
  assert Job.objects.filter(finished_at__isnull=False).count() == total_jobs


@pytest.mark.skipif(
  os.environ.get("DB_BACKEND", "sqlite") == "sqlite",
  reason="requires a shared test database across threads",
)
def test_concurrency_limit_respected_under_load():
  worker_count = 4
  total_jobs = 8
  started = threading.Event()
  release = threading.Event()
  active_count = [0]
  max_active = [0]
  lock = threading.Lock()
  workers = []

  for index in range(total_jobs):
    limited.enqueue(1, value=f"job-{index}")
  logger.info("concurrency load start total_jobs=%s workers=%s", total_jobs, worker_count)

  try:
    for index in range(worker_count):
      pool = BlockingPool(
        1,
        started=started,
        release=release,
        active_count=active_count,
        max_active=max_active,
        lock=lock,
      )
      worker = make_worker(index, pool=pool)
      worker.start()
      workers.append(worker)

    for worker in workers:
      worker.poll_once()

    wait_until(lambda: started.is_set())
    wait_until(lambda: ClaimedExecution.objects.count() == 1)
    logger.info(
      "concurrency load claimed=%s blocked=%s finished=%s semaphore=%s",
      ClaimedExecution.objects.count(),
      BlockedExecution.objects.count(),
      Job.objects.filter(finished_at__isnull=False).count(),
      Semaphore.objects.get(key="account:1").value,
    )

    assert max_active[0] == 1
    assert ClaimedExecution.objects.count() == 1
    assert BlockedExecution.objects.count() == total_jobs - 1
    assert Job.objects.filter(finished_at__isnull=False).count() == 0
    assert Semaphore.objects.get(key="account:1").value == 0

    release.set()
    logger.info("concurrency load release workers")
    drain_workers(workers, total_jobs=total_jobs, timeout=2)
    logger.info(
      "concurrency load complete claimed=%s blocked=%s finished=%s semaphore=%s",
      ClaimedExecution.objects.count(),
      BlockedExecution.objects.count(),
      Job.objects.filter(finished_at__isnull=False).count(),
      Semaphore.objects.get(key="account:1").value,
    )

    assert max_active[0] == 1
    assert ClaimedExecution.objects.count() == 0
    assert BlockedExecution.objects.count() == 0
    assert Job.objects.filter(finished_at__isnull=False).count() == total_jobs
  finally:
    release.set()
    logger.info("concurrency load stop")
    for worker in workers:
      worker.stop(timeout=1)
