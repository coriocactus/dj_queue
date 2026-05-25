import time
from contextlib import contextmanager

from django.db import connection
from django.utils import timezone

from benchmarks.harness import Timer, throughput
from benchmarks.tasks import limited, noop
from dj_queue.config import load_backend_config
from dj_queue.models import BlockedExecution, ClaimedExecution, Job, ReadyExecution
from dj_queue.operations.jobs import claim_ready_jobs, execute_claimed_job
from dj_queue.runtime.supervisor import AsyncSupervisor


def worker_drain(size):
  now = timezone.now()
  jobs = [
    Job(
      task_path=noop.module_path,
      queue_name=noop.queue_name,
      priority=noop.priority,
      payload={"args": [f"worker-{index}"], "kwargs": {}},
      backend_alias="default",
      created_at=now,
      updated_at=now,
    )
    for index in range(size)
  ]
  Job.objects.bulk_create(jobs, batch_size=1000)
  ReadyExecution.objects.bulk_create(
    [
      ReadyExecution(
        job=job,
        backend_alias=job.backend_alias,
        queue_name=job.queue_name,
        priority=job.priority,
        created_at=now,
        latency_started_at=now,
      )
      for job in jobs
    ],
    batch_size=1000,
  )

  preserve_finished_jobs = load_backend_config("default").preserve_finished_jobs
  supervisor = AsyncSupervisor.from_backend_config(backend_alias="default", standalone=False)
  supervisor.start()
  runner_count = len(supervisor.runners)
  try:
    with Timer() as timer:
      _wait_for_drain(
        size, preserve_finished_jobs=preserve_finished_jobs, timeout=max(30, size / 25)
      )
  finally:
    supervisor.stop()

  finished_count = Job.objects.filter(finished_at__isnull=False).count()
  job_count = Job.objects.count()
  completed_count = finished_count if preserve_finished_jobs else size - job_count
  ready_count = ReadyExecution.objects.count()
  claimed_count = ClaimedExecution.objects.count()
  if completed_count != size or ready_count or claimed_count:
    raise AssertionError(
      f"expected all jobs drained, got completed={completed_count} "
      f"ready={ready_count} claimed={claimed_count}"
    )

  return {
    "duration_seconds": timer.duration,
    "jobs_per_second": throughput(size, timer.duration),
    "completed_count": completed_count,
    "finished_count": finished_count,
    "job_count": job_count,
    "ready_count": ready_count,
    "claimed_count": claimed_count,
    "runner_count": runner_count,
    "preserve_finished_jobs": preserve_finished_jobs,
  }


def concurrency_contention(size):
  with _capture_query_count() as captured:
    with Timer() as enqueue_timer:
      for index in range(size):
        limited.enqueue(1, value=f"limited-{index}")
  enqueue_query_count = captured["count"]

  blocked_count = BlockedExecution.objects.count()
  ready_count = ReadyExecution.objects.count()
  if size > 0 and (ready_count != 1 or blocked_count != size - 1):
    raise AssertionError(f"expected one ready and {size - 1} blocked jobs")

  completed = 0
  claim_query_count = 0
  execute_query_count = 0
  with Timer() as drain_timer:
    while completed < size:
      with _capture_query_count() as captured:
        jobs = claim_ready_jobs(limit=1)
      claim_query_count += captured["count"]
      if not jobs:
        time.sleep(0.001)
        continue
      claimed_job = jobs[0]
      with _capture_query_count() as captured:
        execute_claimed_job(claimed_job)
      execute_query_count += captured["count"]
      completed += 1

  finished_count = Job.objects.filter(finished_at__isnull=False).count()
  if (
    finished_count != size or ReadyExecution.objects.exists() or BlockedExecution.objects.exists()
  ):
    raise AssertionError("contention drain left jobs in non-terminal states")

  return {
    "enqueue_duration_seconds": enqueue_timer.duration,
    "enqueue_jobs_per_second": throughput(size, enqueue_timer.duration),
    "enqueue_query_count": enqueue_query_count,
    "drain_duration_seconds": drain_timer.duration,
    "drain_jobs_per_second": throughput(size, drain_timer.duration),
    "drain_query_count": claim_query_count + execute_query_count,
    "claim_query_count": claim_query_count,
    "execute_query_count": execute_query_count,
    "finished_count": finished_count,
  }


def runtime_hot_key_contention(size):
  with Timer() as enqueue_timer:
    for index in range(size):
      limited.enqueue(1, value=f"runtime-limited-{index}")

  blocked_count = BlockedExecution.objects.count()
  ready_count = ReadyExecution.objects.count()
  if size > 0 and (ready_count != 1 or blocked_count != size - 1):
    raise AssertionError(f"expected one ready and {size - 1} blocked jobs")

  preserve_finished_jobs = load_backend_config("default").preserve_finished_jobs
  supervisor = AsyncSupervisor.from_backend_config(backend_alias="default", standalone=False)
  supervisor.start()
  runner_count = len(supervisor.runners)
  try:
    with Timer() as drain_timer:
      _wait_for_drain(
        size,
        preserve_finished_jobs=preserve_finished_jobs,
        timeout=max(60, size / 20),
      )
  finally:
    supervisor.stop()

  finished_count = Job.objects.filter(finished_at__isnull=False).count()
  job_count = Job.objects.count()
  completed_count = finished_count if preserve_finished_jobs else size - job_count
  ready_count = ReadyExecution.objects.count()
  blocked_count = BlockedExecution.objects.count()
  claimed_count = ClaimedExecution.objects.count()
  if completed_count != size or ready_count or blocked_count or claimed_count:
    raise AssertionError(
      f"expected all hot-key jobs drained, got completed={completed_count} "
      f"ready={ready_count} blocked={blocked_count} claimed={claimed_count}"
    )

  return {
    "enqueue_duration_seconds": enqueue_timer.duration,
    "enqueue_jobs_per_second": throughput(size, enqueue_timer.duration),
    "drain_duration_seconds": drain_timer.duration,
    "drain_jobs_per_second": throughput(size, drain_timer.duration),
    "completed_count": completed_count,
    "finished_count": finished_count,
    "job_count": job_count,
    "ready_count": ready_count,
    "blocked_count": blocked_count,
    "claimed_count": claimed_count,
    "runner_count": runner_count,
    "preserve_finished_jobs": preserve_finished_jobs,
  }


def ordered_selector_claim(size):
  now = timezone.now()
  jobs = []
  ready_rows = []
  queue_names = ("alpha", "beta", "gamma")
  selectors = ("alpha", "beta", "gamma")

  for index in range(size):
    queue_name = queue_names[index % len(queue_names)]
    job = Job(
      task_path=noop.module_path,
      queue_name=queue_name,
      priority=noop.priority,
      payload={"args": [f"selector-{index}"], "kwargs": {}},
      backend_alias="default",
      created_at=now,
      updated_at=now,
    )
    jobs.append(job)

  Job.objects.bulk_create(jobs, batch_size=1000)
  for job in jobs:
    ready_rows.append(
      ReadyExecution(
        job=job,
        backend_alias=job.backend_alias,
        queue_name=job.queue_name,
        priority=job.priority,
        created_at=now,
        latency_started_at=now,
      )
    )
  ReadyExecution.objects.bulk_create(ready_rows, batch_size=1000)

  claim_query_count = 0
  claim_duration = 0
  execute_query_count = 0
  execute_duration = 0
  completed = 0
  with Timer() as timer:
    while completed < size:
      with _capture_query_count() as captured:
        with Timer() as claim_timer:
          claimed_jobs = claim_ready_jobs(limit=3, queues=selectors)
      claim_query_count += captured["count"]
      claim_duration += claim_timer.duration
      if not claimed_jobs:
        break
      with _capture_query_count() as captured:
        with Timer() as execute_timer:
          for claimed_job in claimed_jobs:
            execute_claimed_job(claimed_job)
      execute_query_count += captured["count"]
      execute_duration += execute_timer.duration
      completed += len(claimed_jobs)

  finished_count = Job.objects.filter(finished_at__isnull=False).count()
  if completed != size or finished_count != size or ReadyExecution.objects.exists():
    raise AssertionError("ordered selector claim did not drain all ready jobs")

  return {
    "duration_seconds": timer.duration,
    "jobs_per_second": throughput(size, timer.duration),
    "claim_duration_seconds": claim_duration,
    "claim_query_count": claim_query_count,
    "execute_duration_seconds": execute_duration,
    "execute_query_count": execute_query_count,
    "finished_count": finished_count,
  }


def _wait_for_drain(size, *, preserve_finished_jobs, timeout):
  deadline = time.monotonic() + timeout
  while time.monotonic() < deadline:
    finished = Job.objects.filter(finished_at__isnull=False).count()
    job_count = Job.objects.count()
    completed = finished if preserve_finished_jobs else size - job_count
    if (
      completed == size
      and ReadyExecution.objects.count() == 0
      and ClaimedExecution.objects.count() == 0
    ):
      return
    time.sleep(0.02)
  raise AssertionError("timed out waiting for worker drain")


@contextmanager
def _capture_query_count():
  state = {"count": 0}

  def wrapper(execute, sql, params, many, context):
    state["count"] += 1
    return execute(sql, params, many, context)

  with connection.execute_wrapper(wrapper):
    yield state
