import time
from datetime import timedelta

import pytest
from django.db import OperationalError, connection
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from dj_queue.models import (
  BlockedExecution,
  ClaimedExecution,
  FailedExecution,
  Job,
  Pause,
  Process,
  ReadyExecution,
  RecurringExecution,
  RecurringTask,
  ScheduledExecution,
  Semaphore,
)
from dj_queue.operations.enqueue import enqueue_job
from tests.tasks import echo


def make_job(task=None, **overrides):
  payload = {
    "args": list(overrides.pop("args", [])),
    "kwargs": dict(overrides.pop("kwargs", {})),
  }
  payload.update(overrides.pop("payload", {}))

  return Job.objects.create(
    task_path=overrides.pop("task_path", getattr(task, "module_path", "tests.tasks.echo")),
    queue_name=overrides.pop("queue_name", getattr(task, "queue_name", "default")),
    priority=overrides.pop("priority", getattr(task, "priority", 0)),
    payload=payload,
    backend_alias=overrides.pop("backend_alias", getattr(task, "backend", "default")),
    scheduled_at=overrides.pop("scheduled_at", None),
    concurrency_key=overrides.pop("concurrency_key", None),
    finished_at=overrides.pop("finished_at", None),
    return_value=overrides.pop("return_value", None),
    **overrides,
  )


def make_process(**overrides):
  return Process.objects.create(
    backend_alias=overrides.pop("backend_alias", "default"),
    kind=overrides.pop("kind", "Worker"),
    pid=overrides.pop("pid", 12345),
    hostname=overrides.pop("hostname", "localhost"),
    name=overrides.pop("name", "worker-1"),
    metadata=overrides.pop("metadata", {}),
    last_heartbeat_at=overrides.pop("last_heartbeat_at", timezone.now()),
    **overrides,
  )


def make_raw_ready_job(task=None, **overrides):
  job = make_job(task=task, **overrides)
  ReadyExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
  )
  return job


def make_ready_job(task=echo, **overrides):
  if task is None:
    task = echo
  supported = {"args", "kwargs", "queue_name", "priority", "backend_alias"}
  unsupported = set(overrides) - supported
  if unsupported:
    unsupported_names = ", ".join(sorted(unsupported))
    raise TypeError(f"use make_raw_ready_job for raw ready fields: {unsupported_names}")

  return enqueue_ready_job(
    task=task,
    args=overrides.pop("args", ()),
    kwargs=overrides.pop("kwargs", None),
    queue_name=overrides.pop("queue_name", None),
    priority=overrides.pop("priority", None),
    backend_alias=overrides.pop("backend_alias", None),
  )


def enqueue_ready_job(
  task=echo, *, args=(), kwargs=None, queue_name=None, priority=None, backend_alias=None
):
  if kwargs is None:
    kwargs = {}

  if backend_alias is None:
    backend_alias = task.backend
  task_options = {"backend": backend_alias}
  if queue_name is not None:
    task_options["queue_name"] = queue_name
  if priority is not None:
    task_options["priority"] = priority

  return enqueue_job(task.using(**task_options), args, kwargs, backend_alias=backend_alias)


def make_scheduled_job(task=None, **overrides):
  job = make_job(task=task, **overrides)
  ScheduledExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
    scheduled_at=job.scheduled_at,
  )
  return job


def make_blocked_job(task=None, **overrides):
  expires_at = overrides.pop("expires_at", timezone.now())
  job = make_job(task=task, **overrides)
  BlockedExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
    concurrency_key=job.concurrency_key,
    expires_at=expires_at,
  )
  return job


def make_failed_job(task=None, **overrides):
  exception_class = overrides.pop("exception_class", "builtins.ValueError")
  message = overrides.pop("message", "boom")
  traceback = overrides.pop("traceback", "traceback")
  job = make_job(task=task, **overrides)
  FailedExecution.objects.create(
    job=job,
    exception_class=exception_class,
    message=message,
    traceback=traceback,
  )
  return job


def read_query_counts(read):
  counts = []
  now = timezone.now()
  for prefix, queues, jobs_per_state in (("small", 1, 1), ("large", 4, 3)):
    supervisor = make_process(kind="Supervisor", name=f"{prefix}-supervisor")
    for index in range(queues):
      queue_name = f"queue-{index}"
      key = f"{prefix}:{index}"
      worker = make_process(
        name=f"{prefix}-worker-{index}", supervisor=supervisor, metadata={"queues": [queue_name]}
      )
      Pause.objects.get_or_create(queue_name=queue_name)
      Semaphore.objects.create(key=key, value=0, limit=1, expires_at=now + timedelta(minutes=5))
      RecurringTask.objects.create(
        key=key, task_path="tests.tasks.echo", schedule="0 0 * * *", queue_name=queue_name
      )
      for job_index in range(jobs_per_state):
        make_raw_ready_job(queue_name=queue_name)
        make_scheduled_job(queue_name=queue_name, scheduled_at=now + timedelta(minutes=5))
        make_blocked_job(
          queue_name=queue_name, concurrency_key=key, expires_at=now + timedelta(minutes=5)
        )
        make_failed_job(queue_name=queue_name)
        ClaimedExecution.objects.create(job=make_job(queue_name=queue_name), process=worker)
        finished = make_job(queue_name=queue_name, finished_at=now)
        RecurringExecution.objects.create(
          job=finished, task_key=key, run_at=now - timedelta(minutes=job_index)
        )

    with CaptureQueriesContext(connection) as ctx:
      read()
    counts.append(len(ctx.captured_queries))
  return counts


def wait_until(predicate, timeout=5):
  deadline = time.monotonic() + timeout
  while time.monotonic() < deadline:
    try:
      if predicate():
        return
    except OperationalError as error:
      if "locked" not in str(error).lower():
        raise
    time.sleep(0.01)

  try:
    assert predicate()
  except OperationalError as error:
    if "locked" not in str(error).lower():
      raise
    pytest.fail(f"timed out waiting for predicate after database lock: {error}")
