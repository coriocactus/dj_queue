from datetime import timedelta

import pytest
from django.tasks import task
from django.utils import timezone

from dj_queue.api import (
  QueueInfo,
  concurrency,
  discard_failed_jobs,
  retry_failed_jobs,
  schedule_failed_job_retry,
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


@task
@concurrency(
  key="account:{account_id}",
  limit=2,
  duration=60,
  on_conflict="discard",
)
def decorated_sync_account(account_id):
  return account_id


@concurrency(key=lambda account_id: f"account:{account_id}", limit=1)
@task
def wrapped_sync_account(account_id):
  return account_id


pytestmark = pytest.mark.django_db(transaction=True)


def test_concurrency_decorator_configures_task_function():
  assert decorated_sync_account.func.concurrency_key == "account:{account_id}"
  assert decorated_sync_account.func.concurrency_limit == 2
  assert decorated_sync_account.func.concurrency_duration == 60
  assert decorated_sync_account.func.on_conflict == "discard"


def test_concurrency_decorator_supports_wrapped_task():
  assert wrapped_sync_account.func.concurrency_key(42) == "account:42"
  assert wrapped_sync_account.func.concurrency_limit == 1
  assert hasattr(wrapped_sync_account.func, "concurrency_duration") is False
  assert wrapped_sync_account.func.on_conflict == "block"


def test_queue_info_size():
  make_ready_job(queue_name="emails")
  make_ready_job(queue_name="emails")
  make_ready_job(queue_name="other")

  assert QueueInfo("emails").size == 2


def test_queue_info_size_ignores_invalid_ready_state():
  make_ready_job(queue_name="emails")
  invalid_job = make_ready_job(queue_name="emails")
  Job.objects.filter(pk=invalid_job.pk).update(finished_at=timezone.now())

  assert QueueInfo("emails").size == 1


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


def test_queue_info_all_reuses_snapshot_for_public_reads(django_assert_num_queries):
  job = make_ready_job(queue_name="emails")
  ReadyExecution.objects.filter(job=job).update(
    latency_started_at=timezone.now() - timedelta(seconds=5)
  )

  queue = QueueInfo.all()[0]

  with django_assert_num_queries(0):
    assert queue.queue_name == "emails"
    assert queue.size == 1
    assert queue.paused is False
    assert queue.latency >= 0.0


def test_queue_info_mutations_invalidate_snapshot():
  make_ready_job(queue_name="emails")
  queue = QueueInfo.all()[0]

  queue.pause()

  assert queue.paused is True


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


def test_queue_info_resume_updates_ready_rows_without_materializing_ids(monkeypatch):
  job = make_ready_job(queue_name="emails")
  before_pause = timezone.now() - timedelta(seconds=40)
  ReadyExecution.objects.filter(job=job).update(
    created_at=before_pause,
    latency_started_at=before_pause,
  )
  QueueInfo("emails").pause()
  Pause.objects.filter(backend_alias="default", queue_name="emails").update(
    created_at=timezone.now() - timedelta(seconds=30)
  )

  def fail_values_list(self, *args, **kwargs):
    raise AssertionError("resume should not materialize ready-row ids")

  monkeypatch.setattr("django.db.models.query.QuerySet.values_list", fail_values_list)

  QueueInfo("emails").resume()

  assert QueueInfo("emails").latency < 20.0


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


def test_failed_execution_can_schedule_retry_at():
  retry_at = timezone.now() + timedelta(minutes=5)
  job = make_failed_job()

  scheduled = schedule_failed_job_retry(job.id, retry_at=retry_at)

  assert scheduled.id == job.id
  assert FailedExecution.objects.get(job=job).retry_at == retry_at
  assert ReadyExecution.objects.filter(job=job).exists() is False


def test_failed_execution_discard_all_in_batches():
  failed_jobs = [make_failed_job() for _ in range(3)]

  deleted = 0
  while FailedExecution.objects.exists():
    deleted += discard_failed_jobs(batch_size=2)

  assert deleted == 3
  assert Job.objects.filter(pk__in=[job.id for job in failed_jobs]).exists() is False
  assert FailedExecution.objects.count() == 0
