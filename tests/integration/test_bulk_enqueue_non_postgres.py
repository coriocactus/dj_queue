import logging
import os
from time import perf_counter

import pytest
from django.tasks import TaskResultStatus

from dj_queue.models import Job, ReadyExecution
from tests.tasks import echo


logger = logging.getLogger("dj_queue.stress")


pytestmark = [
  pytest.mark.django_db(transaction=True),
  pytest.mark.skipif(
    os.environ.get("STRESS") != "1",
    reason="run separately with STRESS=1",
  ),
  pytest.mark.skipif(
    os.environ.get("DB_BACKEND", "sqlite") == "postgres",
    reason="covers non-postgres backends only",
  ),
]


def test_bulk_enqueue_stress_smoke_on_non_postgres_backends():
  db_backend = os.environ.get("DB_BACKEND", "sqlite")
  task_count = 5_000 if db_backend == "sqlite" else 3_000
  backend = echo.get_backend()
  bulk_calls = [(echo, (f"bulk-{index}",), {}) for index in range(task_count)]
  logger.info("bulk enqueue smoke start backend=%s task_count=%s", db_backend, task_count)

  Job.objects.count()

  bulk_started_at = perf_counter()
  bulk_results = backend.enqueue_all(bulk_calls)
  bulk_duration = perf_counter() - bulk_started_at
  logger.info(
    "bulk enqueue smoke bulk backend=%s task_count=%s duration=%.3fs",
    db_backend,
    task_count,
    bulk_duration,
  )

  assert len(bulk_results) == task_count
  assert {result.status for result in bulk_results} == {TaskResultStatus.READY}
  assert Job.objects.count() == task_count
  assert ReadyExecution.objects.count() == task_count

  Job.objects.all().delete()

  single_started_at = perf_counter()
  single_results = [echo.enqueue(f"single-{index}") for index in range(task_count)]
  single_duration = perf_counter() - single_started_at
  logger.info(
    "bulk enqueue smoke single backend=%s task_count=%s duration=%.3fs speedup=%.2fx",
    db_backend,
    task_count,
    single_duration,
    single_duration / bulk_duration if bulk_duration else float("inf"),
  )

  assert len(single_results) == task_count
  assert {result.status for result in single_results} == {TaskResultStatus.READY}
  assert bulk_duration * 2 <= single_duration
