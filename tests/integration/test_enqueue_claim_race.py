import os
import threading
import time

import pytest
from django.db import connections
from django.db.utils import OperationalError

from dj_queue.models import ClaimedExecution, Job, ReadyExecution
from dj_queue.operations.jobs import claim_ready_jobs
from tests.tasks import echo


pytestmark = pytest.mark.django_db(transaction=True)


def _is_transient_claim_error(error):
  message = str(error).lower()
  return "deadlock" in message or "lock wait timeout" in message


@pytest.mark.skipif(
  os.environ.get("DB_BACKEND", "sqlite") == "sqlite",
  reason="requires a shared test database across threads",
)
def test_concurrent_enqueue_and_claim_no_lost_jobs():
  producer_count = 4
  jobs_per_producer = 25
  claimer_count = 4
  total_jobs = producer_count * jobs_per_producer
  start_barrier = threading.Barrier(producer_count + claimer_count)
  producers_done = threading.Event()
  claimed_ids = []
  claimed_lock = threading.Lock()
  remaining_producers = producer_count
  producer_lock = threading.Lock()
  expected_values = {
    f"producer-{producer_index}-job-{job_index}"
    for producer_index in range(producer_count)
    for job_index in range(jobs_per_producer)
  }

  def producer(producer_index):
    nonlocal remaining_producers

    try:
      start_barrier.wait()
      for job_index in range(jobs_per_producer):
        echo.enqueue(f"producer-{producer_index}-job-{job_index}")
    finally:
      with producer_lock:
        remaining_producers -= 1
        if remaining_producers == 0:
          producers_done.set()
      connections.close_all()

  def claimer():
    try:
      start_barrier.wait()
      while True:
        try:
          claimed_jobs = claim_ready_jobs(limit=3)
        except OperationalError as error:
          if not _is_transient_claim_error(error):
            raise
          connections.close_all()
          time.sleep(0.005)
          continue
        if claimed_jobs:
          with claimed_lock:
            claimed_ids.extend(str(claimed_job.job.id) for claimed_job in claimed_jobs)
          continue

        if producers_done.is_set() and Job.objects.count() == total_jobs:
          if ReadyExecution.objects.exists() is False:
            return
        time.sleep(0.005)
    finally:
      connections.close_all()

  threads = [
    *[
      threading.Thread(target=producer, args=(producer_index,))
      for producer_index in range(producer_count)
    ],
    *[threading.Thread(target=claimer) for _ in range(claimer_count)],
  ]

  for thread in threads:
    thread.start()
  for thread in threads:
    thread.join(timeout=5)

  assert all(thread.is_alive() is False for thread in threads)
  assert Job.objects.count() == total_jobs
  assert ClaimedExecution.objects.count() == total_jobs
  assert ReadyExecution.objects.count() == 0
  assert len(claimed_ids) == total_jobs
  assert len(set(claimed_ids)) == total_jobs
  assert set(claimed_ids) == {str(job_id) for job_id in Job.objects.values_list("id", flat=True)}
  assert set(Job.objects.values_list("payload__args__0", flat=True)) == expected_values
