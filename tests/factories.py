from django.utils import timezone

from dj_queue.models import (
  BlockedExecution,
  FailedExecution,
  Job,
  Process,
  ReadyExecution,
  ScheduledExecution,
)


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


def make_ready_job(task=None, **overrides):
  job = make_job(task=task, **overrides)
  ReadyExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
  )
  return job


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
