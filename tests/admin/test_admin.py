import pytest
from django.contrib.auth import get_user_model
from django.urls import reverse

from dj_queue.models import FailedExecution, Job, Process


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


def make_failed_job(**overrides):
  job = make_job(**overrides)
  failed = FailedExecution.objects.create(
    job=job,
    exception_class="builtins.ValueError",
    message="boom",
    traceback="traceback",
  )
  return job, failed


def make_process(**overrides):
  return Process.objects.create(
    kind=overrides.pop("kind", "Worker"),
    pid=overrides.pop("pid", 12345),
    hostname=overrides.pop("hostname", "localhost"),
    name=overrides.pop("name", "worker-1"),
    metadata=overrides.pop("metadata", {"threads": 2}),
    last_heartbeat_at=overrides.pop("last_heartbeat_at"),
    **overrides,
  )


@pytest.fixture
def admin_client(client):
  user = get_user_model().objects.create_superuser(
    username="admin",
    email="admin@example.com",
    password="password",
  )
  client.force_login(user)
  return client


def test_job_admin_changelist_renders(admin_client):
  make_job()

  response = admin_client.get(
    reverse("admin:dj_queue_job_changelist"),
    {"backend_name__exact": "default"},
  )

  assert response.status_code == 200


def test_failed_execution_admin_retry_action(admin_client):
  job, failed = make_failed_job()

  response = admin_client.post(
    reverse("admin:dj_queue_failedexecution_changelist"),
    {
      "action": "retry_jobs",
      "_selected_action": [str(failed.pk)],
    },
    follow=True,
  )

  assert response.status_code == 200
  assert FailedExecution.objects.filter(pk=failed.pk).exists() is False
  assert Job.objects.filter(pk=job.pk, ready_execution__isnull=False).exists() is True


def test_failed_execution_admin_changelist_filtered_by_backend(admin_client):
  make_failed_job()

  response = admin_client.get(
    reverse("admin:dj_queue_failedexecution_changelist"),
    {"job__backend_name__exact": "default"},
  )

  assert response.status_code == 200


def test_failed_execution_admin_discard_action(admin_client):
  job, failed = make_failed_job()

  response = admin_client.post(
    reverse("admin:dj_queue_failedexecution_changelist"),
    {
      "action": "discard_jobs",
      "_selected_action": [str(failed.pk)],
    },
    follow=True,
  )

  assert response.status_code == 200
  assert FailedExecution.objects.filter(pk=failed.pk).exists() is False
  assert Job.objects.filter(pk=job.pk).exists() is False


def test_process_admin_displays_metadata(admin_client):
  process = Process.objects.create(
    kind="Worker",
    pid=12345,
    hostname="localhost",
    name="worker-1",
    metadata={"threads": 2},
    last_heartbeat_at="2026-04-08T00:00:00Z",
  )

  response = admin_client.get(reverse("admin:dj_queue_process_changelist"))

  assert response.status_code == 200
  content = response.content.decode()
  assert process.name in content
  assert "threads" in content


def test_admin_index_hides_raw_dj_queue_models(admin_client):
  response = admin_client.get(reverse("admin:index"))

  assert response.status_code == 200
  dj_queue_app = next(
    app for app in response.context["available_apps"] if app["app_label"] == "dj_queue"
  )
  object_names = [model.get("object_name") for model in dj_queue_app["models"]]
  assert object_names == ["Dashboard"]
