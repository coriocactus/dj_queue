from datetime import timedelta

import pytest
from django.utils import timezone

from dj_queue.api import QueueInfo
from dj_queue.exceptions import UndiscardableError
from dj_queue.models import (
  ClaimedExecution,
  FailedExecution,
  Job,
  Pause,
  Process,
  ReadyExecution,
)


pytestmark = pytest.mark.django_db(transaction=True)


def make_job(**overrides):
  payload = {
    "args": list(overrides.pop("args", [])),
    "kwargs": dict(overrides.pop("kwargs", {})),
  }
  payload.update(overrides.pop("payload", {}))

  return Job.objects.create(
    task_path=overrides.pop("task_path", "tests.tasks.echo"),
    queue_name=overrides.pop("queue_name", "default"),
    priority=overrides.pop("priority", 0),
    payload=payload,
    backend_alias=overrides.pop("backend_alias", "default"),
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


def make_ready_job(**overrides):
  job = make_job(**overrides)
  ReadyExecution.objects.create(job=job, queue_name=job.queue_name, priority=job.priority)
  return job


def make_failed_job(**overrides):
  job = make_job(**overrides)
  FailedExecution.objects.create(
    job=job,
    exception_class=overrides.pop("exception_class", "builtins.ValueError"),
    message=overrides.pop("message", "boom"),
    traceback=overrides.pop("traceback", "traceback"),
  )
  return job


def test_queue_info_size():
  make_ready_job(queue_name="emails")
  make_ready_job(queue_name="emails")
  make_ready_job(queue_name="other")

  assert QueueInfo("emails").size == 2


def test_queue_info_stays_backend_scoped_on_shared_queue_db(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {"database_alias": "default"},
    },
    "secondary": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {"database_alias": "default"},
    },
  }
  make_ready_job(queue_name="emails", backend_alias="default")
  make_ready_job(queue_name="emails", backend_alias="secondary")

  assert QueueInfo("emails", backend_alias="default").size == 1
  assert QueueInfo("emails", backend_alias="secondary").size == 1


def test_queue_info_latency():
  old_job = make_ready_job(queue_name="emails")
  ReadyExecution.objects.filter(job=old_job).update(
    latency_started_at=timezone.now() - timedelta(seconds=5)
  )

  assert QueueInfo("emails").latency >= 5.0


def test_queue_info_pause_and_resume():
  job = make_ready_job(queue_name="emails")
  before_pause = timezone.now() - timedelta(seconds=5)
  ReadyExecution.objects.filter(job=job).update(
    created_at=before_pause,
    latency_started_at=before_pause,
  )
  queue = QueueInfo("emails")

  queue.pause()
  assert queue.paused is True
  assert Pause.objects.filter(queue_name="emails").exists() is True

  Pause.objects.filter(queue_name="emails").update(
    created_at=timezone.now() - timedelta(seconds=30)
  )

  queue.resume()
  assert queue.paused is False
  assert Pause.objects.filter(queue_name="emails").exists() is False
  assert QueueInfo("emails").latency < 10.0


def test_queue_info_clear():
  first = make_ready_job(queue_name="emails")
  second = make_ready_job(queue_name="emails")
  make_ready_job(queue_name="other")

  deleted = QueueInfo("emails").clear(batch_size=1)

  assert deleted == 2
  assert Job.objects.filter(pk__in=[first.pk, second.pk]).exists() is False
  assert QueueInfo("emails").size == 0
  assert QueueInfo("other").size == 1


def test_failed_execution_retry_all():
  failed_jobs = [make_failed_job() for _ in range(2)]

  retried = FailedExecution.retry_all(FailedExecution.objects.order_by("job_id"))

  assert retried == 2
  assert FailedExecution.objects.count() == 0
  assert ReadyExecution.objects.filter(job_id__in=[job.id for job in failed_jobs]).count() == 2


def test_failed_execution_discard_all_in_batches():
  failed_jobs = [make_failed_job() for _ in range(3)]

  deleted = FailedExecution.discard_all_in_batches(batch_size=2)

  assert deleted == 3
  assert Job.objects.filter(pk__in=[job.id for job in failed_jobs]).exists() is False
  assert FailedExecution.objects.count() == 0


def test_claimed_execution_discard_raises_undiscardable_error():
  job = make_job()
  ClaimedExecution.objects.create(job=job, process=make_process())

  with pytest.raises(UndiscardableError):
    ClaimedExecution.discard_all_in_batches()
