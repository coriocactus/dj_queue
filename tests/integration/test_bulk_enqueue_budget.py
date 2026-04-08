import os
from time import perf_counter

import pytest
from django.db import connections
from django.tasks import TaskResultStatus
from django.test.utils import CaptureQueriesContext

from dj_queue.models import Job, ReadyExecution
from tests.tasks import echo


pytestmark = [
  pytest.mark.django_db(transaction=True),
  pytest.mark.postgres,
  pytest.mark.skipif(
    os.environ.get("RUN_STRESS") != "1",
    reason="run separately with RUN_STRESS=1",
  ),
]


def test_bulk_enqueue_10k_within_budget():
  task_count = 10_000
  backend = echo.get_backend()
  connection = connections["default"]
  bulk_calls = [(echo, (f"bulk-{index}",), {}) for index in range(task_count)]

  connection.ensure_connection()
  Job.objects.count()

  with CaptureQueriesContext(connection) as captured:
    bulk_started_at = perf_counter()
    bulk_results = backend.enqueue_all(bulk_calls)
    bulk_duration = perf_counter() - bulk_started_at

  assert len(captured) <= 6
  assert len(bulk_results) == task_count
  assert {result.status for result in bulk_results} == {TaskResultStatus.READY}
  assert Job.objects.count() == task_count
  assert ReadyExecution.objects.count() == task_count

  Job.objects.all().delete()

  single_started_at = perf_counter()
  single_results = [echo.enqueue(f"single-{index}") for index in range(task_count)]
  single_duration = perf_counter() - single_started_at

  assert len(single_results) == task_count
  assert {result.status for result in single_results} == {TaskResultStatus.READY}
  assert bulk_duration * 5 <= single_duration
