from datetime import timedelta

import pytest
from django.contrib.auth import get_user_model
from django.urls import reverse
from django.utils.html import format_html
from django.utils import timezone

from dj_queue.models import (
  BlockedExecution,
  FailedExecution,
  Job,
  Process,
  ReadyExecution,
  RecurringExecution,
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
    {"backend": "default"},
  )

  assert response.status_code == 200
  assert "By backend" in response.content.decode()


def test_job_admin_status_filter(admin_client):
  finished_job = make_job(queue_name="finished", finished_at=timezone.now())
  ready_job = make_job(queue_name="ready")
  ReadyExecution.objects.create(
    job=ready_job, queue_name=ready_job.queue_name, priority=ready_job.priority
  )

  response = admin_client.get(
    reverse("admin:dj_queue_job_changelist"),
    {"status": "finished", "backend": "default"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert str(finished_job.id) in content
  assert str(ready_job.id) not in content


def test_job_admin_status_sort(admin_client):
  _finished_job = make_job(task_path="tests.tasks.finished", finished_at=timezone.now())
  ready_job = make_job(task_path="tests.tasks.ready")
  ReadyExecution.objects.create(
    job=ready_job, queue_name=ready_job.queue_name, priority=ready_job.priority
  )

  response = admin_client.get(
    reverse("admin:dj_queue_job_changelist"),
    {"backend": "default", "o": "-5"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert content.index("tests.tasks.ready") < content.index("tests.tasks.finished")


def test_job_admin_queue_name_links_to_matching_queue_state(admin_client):
  make_failed_job(queue_name="alpha")

  response = admin_client.get(
    reverse("admin:dj_queue_job_changelist"),
    {"backend": "default"},
  )

  assert response.status_code == 200
  assert (
    f"{reverse('admin:dj_queue_dashboard_queue', args=['alpha'])}?backend=default&amp;state=failed"
    in response.content.decode()
  )


def test_job_admin_recurring_task_filter(admin_client):
  task = RecurringTask.objects.create(
    key="nightly",
    task_path="tests.tasks.echo",
    payload={"args": [], "kwargs": {}},
    schedule="0 0 * * *",
    queue_name="default",
    priority=0,
    static=False,
  )
  recurring_job = make_job(task_path="tests.tasks.recurring")
  _other_job = make_job(task_path="tests.tasks.other")
  RecurringExecution.objects.create(job=recurring_job, task_key=task.key, run_at=timezone.now())

  response = admin_client.get(
    reverse("admin:dj_queue_job_changelist"),
    {
      "backend": "default",
      "recurring_task_key": "nightly",
    },
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert "tests.tasks.recurring" in content
  assert "tests.tasks.other" not in content


def test_job_admin_concurrency_key_filter(admin_client):
  matching_job = make_job(task_path="tests.tasks.matching", concurrency_key="acct:1")
  other_job = make_job(task_path="tests.tasks.other", concurrency_key="acct:2")

  response = admin_client.get(
    reverse("admin:dj_queue_job_changelist"),
    {
      "backend": "default",
      "concurrency_key": "acct:1",
    },
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert str(matching_job.id) in content
  assert str(other_job.id) not in content


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


def test_failed_execution_change_view_shows_retry_and_discard_actions(admin_client):
  _job, failed = make_failed_job()

  response = admin_client.get(
    reverse("admin:dj_queue_failedexecution_change", args=[failed.pk]),
    {"backend": "default"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert '<ul class="object-tools">' not in content
  assert '<div class="submit-row">' in content
  assert 'name="_djq_object_action" value="retry"' in content
  assert 'name="_djq_object_action" value="discard"' in content
  assert "Retry failed job" in content
  assert "Discard failed job" in content
  assert ">History<" not in content
  assert "Save and continue editing" not in content
  assert 'value="Save"' not in content


def test_failed_execution_change_view_retry_action(admin_client):
  job, failed = make_failed_job()

  response = admin_client.post(
    f"{reverse('admin:dj_queue_failedexecution_change', args=[failed.pk])}?backend=default",
    {"_djq_object_action": "retry"},
    follow=True,
  )

  assert response.status_code == 200
  assert FailedExecution.objects.filter(pk=failed.pk).exists() is False
  assert Job.objects.filter(pk=job.pk, ready_execution__isnull=False).exists() is True


def test_failed_execution_admin_changelist_filtered_by_backend(admin_client):
  make_failed_job()

  response = admin_client.get(
    reverse("admin:dj_queue_failedexecution_changelist"),
    {"backend": "default"},
  )

  assert response.status_code == 200


def test_failed_execution_admin_changelist_filtered_by_queue(admin_client):
  make_failed_job(queue_name="alpha")

  response = admin_client.get(
    reverse("admin:dj_queue_failedexecution_changelist"),
    {
      "job__queue_name": "alpha",
      "backend": "default",
    },
  )

  assert response.status_code == 200


def test_job_admin_backend_param_controls_shared_database_scope(admin_client, settings):
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
  default_job = make_job(task_path="tests.tasks.default", backend_name="default")
  secondary_job = make_job(task_path="tests.tasks.secondary", backend_name="secondary")

  response = admin_client.get(
    reverse("admin:dj_queue_job_changelist"),
    {"backend": "secondary", "backend_name": "default"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert str(secondary_job.id) in content
  assert str(default_job.id) not in content


def test_failed_execution_admin_backend_param_controls_shared_database_scope(
  admin_client,
  settings,
):
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
  default_job, _ = make_failed_job(task_path="tests.tasks.default", backend_name="default")
  secondary_job, _ = make_failed_job(task_path="tests.tasks.secondary", backend_name="secondary")

  response = admin_client.get(
    reverse("admin:dj_queue_failedexecution_changelist"),
    {"backend": "secondary", "job__backend_name": "default"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert str(secondary_job.id) in content
  assert str(default_job.id) not in content


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


def test_failed_execution_change_view_discard_action(admin_client):
  job, failed = make_failed_job()

  response = admin_client.post(
    f"{reverse('admin:dj_queue_failedexecution_change', args=[failed.pk])}?backend=default",
    {"_djq_object_action": "discard"},
    follow=True,
  )

  assert response.status_code == 200
  assert FailedExecution.objects.filter(pk=failed.pk).exists() is False
  assert Job.objects.filter(pk=job.pk).exists() is False


def test_job_change_view_shows_enqueue_retry_and_discard_actions_for_failed_jobs(admin_client):
  job, _failed = make_failed_job()

  response = admin_client.get(
    reverse("admin:dj_queue_job_change", args=[job.pk]),
    {"backend": "default"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert '<ul class="object-tools">' not in content
  assert '<div class="submit-row">' in content
  assert 'name="_djq_object_action" value="enqueue"' in content
  assert 'name="_djq_object_action" value="retry"' in content
  assert 'name="_djq_object_action" value="discard"' in content
  assert "Enqueue job" in content
  assert "Retry failed job" in content
  assert "Discard failed job" in content
  assert ">History<" not in content
  assert "Save and continue editing" not in content
  assert 'value="Save"' not in content


def test_job_change_view_shows_enqueue_action_for_non_failed_jobs(admin_client):
  job = make_job()

  response = admin_client.get(
    reverse("admin:dj_queue_job_change", args=[job.pk]),
    {"backend": "default"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert '<ul class="object-tools">' not in content
  assert '<div class="submit-row">' in content
  assert 'name="_djq_object_action" value="enqueue"' in content
  assert 'name="_djq_object_action" value="retry"' not in content
  assert 'name="_djq_object_action" value="discard"' not in content
  assert ">History<" not in content
  assert "Save and continue editing" not in content
  assert 'value="Save"' not in content


def test_process_change_view_hides_save_controls(admin_client):
  process = make_process(last_heartbeat_at=timezone.now())

  response = admin_client.get(
    reverse("admin:dj_queue_process_change", args=[process.pk]),
    {"backend": "default"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert '<ul class="object-tools">' not in content
  assert '<div class="submit-row">' not in content
  assert ">History<" not in content
  assert "Save and continue editing" not in content
  assert 'value="Save"' not in content


def test_job_change_view_retry_action(admin_client):
  job, failed = make_failed_job()

  response = admin_client.post(
    f"{reverse('admin:dj_queue_job_change', args=[job.pk])}?backend=default",
    {"_djq_object_action": "retry"},
    follow=True,
  )

  assert response.status_code == 200
  assert FailedExecution.objects.filter(pk=failed.pk).exists() is False
  assert Job.objects.filter(pk=job.pk, ready_execution__isnull=False).exists() is True


def test_job_change_view_enqueue_action(admin_client):
  job = make_job(queue_name="alpha", priority=7, args=["again"])
  change_url = f"{reverse('admin:dj_queue_job_change', args=[job.pk])}?backend=default"

  response = admin_client.post(
    change_url,
    {"_djq_object_action": "enqueue"},
    follow=True,
  )

  assert response.status_code == 200
  assert response.request["PATH_INFO"] == reverse("admin:dj_queue_job_change", args=[job.pk])
  assert Job.objects.filter(pk=job.pk).exists() is True

  new_job = Job.objects.exclude(pk=job.pk).get()
  assert new_job.task_path == job.task_path
  assert new_job.queue_name == job.queue_name
  assert new_job.priority == job.priority
  assert new_job.payload == job.payload
  assert new_job.backend_name == job.backend_name
  assert new_job.scheduled_at == job.scheduled_at
  assert ReadyExecution.objects.filter(job=new_job).exists() is True
  messages = list(response.context["messages"])
  assert len(messages) == 1
  assert messages[0].message == format_html(
    'Enqueued job <a href="{}">{}</a>.',
    f"{reverse('admin:dj_queue_job_change', args=[new_job.pk])}?backend=default",
    new_job.pk,
  )


def test_job_change_view_discard_action(admin_client):
  job, failed = make_failed_job()

  response = admin_client.post(
    f"{reverse('admin:dj_queue_job_change', args=[job.pk])}?backend=default",
    {"_djq_object_action": "discard"},
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


def test_process_admin_status_filter(admin_client):
  now = timezone.now()
  live = make_process(name="live-worker", last_heartbeat_at=now)
  stale = make_process(name="stale-worker", last_heartbeat_at=now - timedelta(minutes=10))

  response = admin_client.get(
    reverse("admin:dj_queue_process_changelist"),
    {"status": "live"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert live.name in content
  assert stale.name not in content


def test_process_admin_status_sort(admin_client):
  now = timezone.now()
  make_process(name="stale-worker", last_heartbeat_at=now - timedelta(minutes=10))
  make_process(name="live-worker", last_heartbeat_at=now)

  response = admin_client.get(
    reverse("admin:dj_queue_process_changelist"),
    {"o": "3"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert content.index("live-worker") < content.index("stale-worker")


def test_process_admin_hides_add_button(admin_client):
  make_process(last_heartbeat_at="2026-04-08T00:00:00Z")

  response = admin_client.get(reverse("admin:dj_queue_process_changelist"))

  assert response.status_code == 200
  assert response.context["has_add_permission"] is False
  assert "Add process" not in response.content.decode()


def test_recurring_task_admin_filters(admin_client):
  RecurringTask.objects.create(
    key="nightly",
    task_path="tests.tasks.echo",
    payload={"args": [], "kwargs": {}},
    schedule="0 0 * * *",
    queue_name="reports",
    priority=0,
    static=True,
  )
  RecurringTask.objects.create(
    key="hourly",
    task_path="tests.tasks.echo",
    payload={"args": [], "kwargs": {}},
    schedule="0 * * * *",
    queue_name="maintenance",
    priority=0,
    static=False,
  )

  response = admin_client.get(
    reverse("admin:dj_queue_recurringtask_changelist"),
    {"queue_name": "reports", "static__exact": "1"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert "nightly" in content
  assert "hourly" not in content


def test_backend_scoped_raw_admin_pages_show_backend_filter(admin_client):
  for url_name in (
    "admin:dj_queue_job_changelist",
    "admin:dj_queue_failedexecution_changelist",
    "admin:dj_queue_process_changelist",
    "admin:dj_queue_recurringtask_changelist",
    "admin:dj_queue_pause_changelist",
    "admin:dj_queue_semaphore_changelist",
  ):
    response = admin_client.get(reverse(url_name))

    assert response.status_code == 200
    content = response.content.decode()
    assert 'id="changelist-filter"' in content
    assert "By backend" in content


def test_semaphore_admin_blocked_waiters_sort(admin_client):
  high = Semaphore.objects.create(
    key="acct:high",
    value=1,
    limit=2,
    expires_at=timezone.now(),
  )
  low = Semaphore.objects.create(
    key="acct:low",
    value=1,
    limit=2,
    expires_at=timezone.now(),
  )
  for index in range(2):
    job = make_job(
      queue_name="blocked", concurrency_key=high.key, task_path=f"tests.tasks.high_{index}"
    )
    BlockedExecution.objects.create(
      job=job,
      queue_name=job.queue_name,
      priority=job.priority,
      concurrency_key=job.concurrency_key,
      expires_at=timezone.now(),
    )
  job = make_job(queue_name="blocked", concurrency_key=low.key, task_path="tests.tasks.low")
  BlockedExecution.objects.create(
    job=job,
    queue_name=job.queue_name,
    priority=job.priority,
    concurrency_key=job.concurrency_key,
    expires_at=timezone.now(),
  )

  response = admin_client.get(
    reverse("admin:dj_queue_semaphore_changelist"),
    {"o": "4"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert content.index(high.key) < content.index(low.key)


def test_admin_index_hides_raw_dj_queue_models(admin_client):
  response = admin_client.get(reverse("admin:index"))

  assert response.status_code == 200
  assert response.context["available_apps"][0]["app_label"] == "dj_queue"
  dj_queue_app = next(
    app for app in response.context["available_apps"] if app["app_label"] == "dj_queue"
  )
  assert dj_queue_app["name"] == "dj_queue"
  assert dj_queue_app["app_url"] == reverse("admin:dj_queue_dashboard_changelist")
  object_names = [model.get("object_name") for model in dj_queue_app["models"]]
  assert object_names == ["Dashboard"]


def test_dj_queue_app_index_redirects_to_dashboard(admin_client):
  response = admin_client.get(
    reverse("admin:app_list", kwargs={"app_label": "dj_queue"}),
    {"backend": "secondary"},
  )

  assert response.status_code == 302
  assert response["Location"] == (
    f"{reverse('admin:dj_queue_dashboard_changelist')}?backend=secondary"
  )


def test_job_admin_breadcrumb_links_to_dashboard(admin_client):
  make_job()

  response = admin_client.get(
    reverse("admin:dj_queue_job_changelist"),
    {"backend": "default"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert (
    f'› <a href="{reverse("admin:dj_queue_dashboard_changelist")}?backend=default">dj_queue</a>'
    in content
  )
  assert f'href="{reverse("admin:app_list", kwargs={"app_label": "dj_queue"})}"' not in content


def test_job_change_view_breadcrumb_links_preserve_backend(admin_client, settings):
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
  job = make_job(backend_name="secondary")

  response = admin_client.get(
    reverse("admin:dj_queue_job_change", args=[job.pk]),
    {"backend": "secondary"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert (
    f'› <a href="{reverse("admin:dj_queue_dashboard_changelist")}?backend=secondary">dj_queue</a>'
    in content
  )
  assert (
    f'› <a href="{reverse("admin:dj_queue_job_changelist")}?backend=secondary">Jobs</a>' in content
  )
