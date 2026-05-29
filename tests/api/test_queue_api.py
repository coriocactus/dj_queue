from datetime import timedelta

import pytest
from django.utils import timezone

from dj_queue.api import (
  QueueInfo,
  discard_failed_jobs,
  retry_failed_jobs,
)
from dj_queue.models import (
  FailedExecution,
  Job,
  Pause,
  ReadyExecution,
  RecurringTask,
)
from tests.factories import (
  make_blocked_job,
  make_failed_job,
  make_ready_job,
  make_scheduled_job,
)


pytestmark = pytest.mark.django_db(transaction=True)


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


def test_queue_info_latency_is_never_negative():
  job = make_ready_job(queue_name="emails")
  ReadyExecution.objects.filter(job=job).update(
    latency_started_at=timezone.now() + timedelta(seconds=5)
  )

  assert QueueInfo("emails").latency == 0.0


def test_queue_info_latency_is_none_while_paused():
  make_ready_job(queue_name="emails")
  Pause.objects.create(backend_alias="default", queue_name="emails")

  assert QueueInfo("emails").latency is None


def test_queue_info_all_uses_shared_queue_discovery():
  future = timezone.now() + timedelta(minutes=5)
  make_scheduled_job(queue_name="scheduled", scheduled_at=future)
  make_blocked_job(
    queue_name="blocked",
    concurrency_key="account:1",
    expires_at=timezone.now() + timedelta(minutes=1),
  )
  make_failed_job(queue_name="failed")
  Pause.objects.create(backend_alias="default", queue_name="paused")
  RecurringTask.objects.create(
    backend_alias="default",
    key="hourly",
    task_path="tests.tasks.echo",
    payload={"args": [], "kwargs": {}},
    schedule="* * * * *",
    queue_name="recurring",
  )

  assert [queue.queue_name for queue in QueueInfo.all()] == [
    "blocked",
    "failed",
    "paused",
    "recurring",
    "scheduled",
  ]


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
  assert Pause.objects.filter(backend_alias="default", queue_name="emails").exists() is True

  Pause.objects.filter(backend_alias="default", queue_name="emails").update(
    created_at=timezone.now() - timedelta(seconds=30)
  )

  queue.resume()
  assert queue.paused is False
  assert Pause.objects.filter(backend_alias="default", queue_name="emails").exists() is False
  latency = QueueInfo("emails").latency
  assert 0.0 <= latency < 10.0


def test_queue_info_resume_preserves_pre_pause_active_wait_time():
  job = make_ready_job(queue_name="emails")
  before_pause = timezone.now() - timedelta(seconds=40)
  ReadyExecution.objects.filter(job=job).update(
    created_at=before_pause,
    latency_started_at=before_pause,
  )
  queue = QueueInfo("emails")

  queue.pause()
  Pause.objects.filter(backend_alias="default", queue_name="emails").update(
    created_at=timezone.now() - timedelta(seconds=30)
  )

  queue.resume()

  latency = QueueInfo("emails").latency
  assert 5.0 <= latency <= 20.0


def test_queue_info_resume_stays_backend_scoped_on_shared_queue_db(settings):
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
  default_job = make_ready_job(queue_name="emails", backend_alias="default")
  secondary_job = make_ready_job(queue_name="emails", backend_alias="secondary")
  before_pause = timezone.now() - timedelta(seconds=5)
  ReadyExecution.objects.filter(job__in=[default_job, secondary_job]).update(
    created_at=before_pause,
    latency_started_at=before_pause,
  )
  queue = QueueInfo("emails", backend_alias="default")

  queue.pause()
  Pause.objects.filter(backend_alias="default", queue_name="emails").update(
    created_at=timezone.now() - timedelta(seconds=30)
  )

  default_before = ReadyExecution.objects.get(job=default_job).latency_started_at
  secondary_before = ReadyExecution.objects.get(job=secondary_job).latency_started_at

  queue.resume()

  default_after = ReadyExecution.objects.get(job=default_job).latency_started_at
  secondary_after = ReadyExecution.objects.get(job=secondary_job).latency_started_at

  assert default_after > default_before
  assert secondary_after == secondary_before


def test_queue_info_clear():
  first = make_ready_job(queue_name="emails")
  second = make_ready_job(queue_name="emails")
  make_ready_job(queue_name="other")

  deleted = QueueInfo("emails").clear(batch_size=1)

  assert deleted == 2
  assert Job.objects.filter(pk__in=[first.pk, second.pk]).exists() is False
  assert QueueInfo("emails").size == 0
  assert QueueInfo("other").size == 1


def test_queue_info_clear_stops_when_discard_batch_makes_no_progress(monkeypatch):
  calls = []

  def no_progress(queue_name, *, batch_size, backend_alias):
    calls.append((queue_name, batch_size, backend_alias))
    return 0

  monkeypatch.setattr("dj_queue.api.discard_ready_jobs_for_queue", no_progress)

  deleted = QueueInfo("emails", backend_alias="default").clear(batch_size=1)

  assert deleted == 0
  assert calls == [("emails", 1, "default")]


def test_failed_execution_retry_all():
  failed_jobs = [make_failed_job() for _ in range(2)]

  retried = retry_failed_jobs(batch_size=2)

  assert retried == 2
  assert FailedExecution.objects.count() == 0
  assert ReadyExecution.objects.filter(job_id__in=[job.id for job in failed_jobs]).count() == 2


def test_failed_execution_discard_all_in_batches():
  failed_jobs = [make_failed_job() for _ in range(3)]

  deleted = 0
  while FailedExecution.objects.exists():
    deleted += discard_failed_jobs(batch_size=2)

  assert deleted == 3
  assert Job.objects.filter(pk__in=[job.id for job in failed_jobs]).exists() is False
  assert FailedExecution.objects.count() == 0
