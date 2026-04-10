from datetime import timedelta

import pytest
from django.contrib.auth import get_user_model
from django.urls import reverse
from django.utils import timezone

from dj_queue.models import (
  FailedExecution,
  Job,
  Pause,
  Process,
  ReadyExecution,
  RecurringTask,
  Semaphore,
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
    backend_name=overrides.pop("backend_name", "default"),
    scheduled_at=overrides.pop("scheduled_at", None),
    concurrency_key=overrides.pop("concurrency_key", None),
    finished_at=overrides.pop("finished_at", None),
    return_value=overrides.pop("return_value", None),
    **overrides,
  )


def make_ready_job(**overrides):
  job = make_job(**overrides)
  ReadyExecution.objects.create(
    job=job,
    queue_name=job.queue_name,
    priority=job.priority,
  )
  return job


def make_failed_job(**overrides):
  job = make_job(**overrides)
  FailedExecution.objects.create(
    job=job,
    exception_class="builtins.ValueError",
    message="boom",
    traceback="traceback",
  )
  return job


@pytest.fixture
def admin_client(client):
  user = get_user_model().objects.create_superuser(
    username="admin",
    email="admin@example.com",
    password="password",
  )
  client.force_login(user)
  return client


def test_dashboard_admin_renders(admin_client):
  now = timezone.now()
  make_ready_job(queue_name="alpha")
  Pause.objects.create(queue_name="alpha")
  Process.objects.create(
    kind="Worker",
    pid=101,
    hostname="localhost",
    name="worker-1",
    metadata={"queues": ["alpha"]},
    last_heartbeat_at=now,
  )
  RecurringTask.objects.create(
    key="nightly",
    task_path="tests.tasks.echo",
    payload={"args": ["nightly"], "kwargs": {}},
    schedule="0 0 * * *",
    queue_name="alpha",
    priority=0,
    static=False,
  )
  Semaphore.objects.create(
    key="account:1",
    value=1,
    limit=2,
    expires_at=now + timedelta(minutes=5),
  )

  response = admin_client.get(reverse("admin:dj_queue_dashboard_changelist"))

  assert response.status_code == 200
  content = response.content.decode()
  assert "alpha" in content
  assert "worker-1" in content
  assert "nightly" in content
  assert "account:1" in content


def test_dashboard_backend_selection_changes_job_counts(admin_client, settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {},
    },
    "secondary": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {},
    },
  }
  make_ready_job(queue_name="alpha", backend_name="default")
  make_ready_job(queue_name="beta", backend_name="secondary")

  default_response = admin_client.get(reverse("admin:dj_queue_dashboard_changelist"))
  secondary_response = admin_client.get(
    reverse("admin:dj_queue_dashboard_changelist"),
    {"backend": "secondary"},
  )

  default_content = default_response.content.decode()
  secondary_content = secondary_response.content.decode()

  assert "alpha" in default_content
  assert "beta" not in default_content
  assert "beta" in secondary_content
  assert "alpha" not in secondary_content


def test_dashboard_overview_pages_large_sections(admin_client):
  for index in range(19):
    make_ready_job(queue_name=f"queue-{index:02d}")

  response = admin_client.get(reverse("admin:dj_queue_dashboard_changelist"))

  assert response.status_code == 200
  content = response.content.decode()
  assert "queue-00" in content
  assert "queue-17" in content
  assert "queue-18" not in content
  assert "queues_page=2" in content

  second_page = admin_client.get(
    reverse("admin:dj_queue_dashboard_changelist"),
    {"queues_page": 2},
  )

  assert second_page.status_code == 200
  assert "queue-18" in second_page.content.decode()


def test_dashboard_queue_pause_resume_and_clear_actions(admin_client, settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {},
    },
    "secondary": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {},
    },
  }
  job = make_ready_job(queue_name="alpha", backend_name="secondary")
  url = reverse("admin:dj_queue_dashboard_queue_action", args=["alpha"])

  response = admin_client.post(url, {"backend": "secondary", "action": "pause"})

  assert response.status_code == 302
  assert response["Location"].endswith("?backend=secondary")
  assert Pause.objects.filter(queue_name="alpha").exists() is True

  response = admin_client.post(url, {"backend": "secondary", "action": "resume"})

  assert response.status_code == 302
  assert Pause.objects.filter(queue_name="alpha").exists() is False

  response = admin_client.post(url, {"backend": "secondary", "action": "clear"})

  assert response.status_code == 302
  assert Job.objects.filter(pk=job.pk).exists() is False


def test_dashboard_queue_bulk_actions(admin_client):
  ready_job = make_ready_job(queue_name="alpha")
  failed_job = make_failed_job(queue_name="alpha")
  url = reverse("admin:dj_queue_dashboard_job_action", args=["alpha"])

  response = admin_client.post(
    url,
    {
      "backend": "default",
      "state": "failed",
      "action": "retry",
      "job_ids": [str(failed_job.pk)],
    },
  )

  assert response.status_code == 302
  assert FailedExecution.objects.filter(job_id=failed_job.pk).exists() is False
  assert Job.objects.filter(pk=failed_job.pk, ready_execution__isnull=False).exists() is True

  response = admin_client.post(
    url,
    {
      "backend": "default",
      "state": "ready",
      "action": "discard",
      "job_ids": [str(ready_job.pk)],
    },
  )

  assert response.status_code == 302
  assert Job.objects.filter(pk=ready_job.pk).exists() is False


def test_dashboard_links_to_raw_admin_tables(admin_client):
  response = admin_client.get(reverse("admin:dj_queue_dashboard_changelist"))

  assert response.status_code == 200
  content = response.content.decode()
  assert reverse("admin:dj_queue_job_changelist") in content
  assert reverse("admin:dj_queue_failedexecution_changelist") in content
  assert reverse("admin:dj_queue_process_changelist") in content
  assert reverse("admin:dj_queue_recurringtask_changelist") in content
  assert reverse("admin:dj_queue_pause_changelist") in content
  assert reverse("admin:dj_queue_semaphore_changelist") in content
