from django.db import connection
from django.tasks import TaskResultStatus
from django.test.utils import CaptureQueriesContext

from benchmarks.harness import Timer, latency_summary, throughput
from benchmarks.tasks import noop
from dj_queue.models import Job, ReadyExecution


def single_enqueue(size):
  durations = []
  query_count_sample = None
  with Timer() as timer:
    for index in range(size):
      if query_count_sample is None:
        with CaptureQueriesContext(connection) as captured:
          with Timer() as enqueue_timer:
            result = noop.enqueue(f"single-{index}")
        query_count_sample = len(captured)
      else:
        with Timer() as enqueue_timer:
          result = noop.enqueue(f"single-{index}")
      durations.append(enqueue_timer.duration)
      if result.status != TaskResultStatus.READY:
        raise AssertionError(f"unexpected enqueue status: {result.status}")

  ready_count = ReadyExecution.objects.count()
  job_count = Job.objects.count()
  if ready_count != size or job_count != size:
    raise AssertionError(f"expected {size} jobs and ready rows, got {job_count}/{ready_count}")

  return {
    "duration_seconds": timer.duration,
    "jobs_per_second": throughput(size, timer.duration),
    "query_count_sample": query_count_sample,
    "job_count": job_count,
    "ready_count": ready_count,
    **latency_summary(durations),
  }


def bulk_enqueue(size):
  backend = noop.get_backend()
  task_calls = [(noop, (f"bulk-{index}",), {}) for index in range(size)]

  with CaptureQueriesContext(connection) as captured:
    with Timer() as timer:
      results = backend.enqueue_all(task_calls)

  ready_count = ReadyExecution.objects.count()
  job_count = Job.objects.count()
  if len(results) != size or {result.status for result in results} != {TaskResultStatus.READY}:
    raise AssertionError("bulk enqueue did not return ready task results")
  if ready_count != size or job_count != size:
    raise AssertionError(f"expected {size} jobs and ready rows, got {job_count}/{ready_count}")

  return {
    "duration_seconds": timer.duration,
    "jobs_per_second": throughput(size, timer.duration),
    "query_count": len(captured),
    "job_count": job_count,
    "ready_count": ready_count,
  }
