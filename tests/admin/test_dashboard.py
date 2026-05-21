from datetime import timedelta

import pytest
from django.contrib.auth import get_user_model
from django.http import Http404
from django.urls import reverse
from django.utils import timezone

from dj_queue import dashboard, db
from dj_queue.models import (
  BlockedExecution,
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


def make_ready_job(**overrides):
  job = make_job(**overrides)
  ReadyExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
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


def make_scheduled_job(**overrides):
  scheduled_at = overrides.pop("scheduled_at", timezone.now() + timedelta(minutes=5))
  job = make_job(scheduled_at=scheduled_at, **overrides)
  ScheduledExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
    scheduled_at=scheduled_at,
  )
  return job


def make_blocked_job(**overrides):
  overrides.setdefault("concurrency_key", "acct:1")
  job = make_job(**overrides)
  BlockedExecution.objects.create(
    job=job,
    backend_alias=job.backend_alias,
    queue_name=job.queue_name,
    priority=job.priority,
    concurrency_key=job.concurrency_key,
    expires_at=timezone.now() + timedelta(minutes=5),
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
  Pause.objects.create(backend_alias="default", queue_name="alpha")
  Process.objects.create(
    backend_alias="default",
    kind="Worker",
    pid=101,
    hostname="localhost",
    name="worker-1",
    metadata={"queues": ["alpha"]},
    last_heartbeat_at=now,
  )
  RecurringTask.objects.create(
    backend_alias="default",
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


def test_dashboard_backend_facts_show_effective_database_capabilities(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {},
    }
  }

  context = dashboard.dashboard_context(backend_alias="default")
  facts = {fact["label"]: fact["value"] for fact in context["backend_facts"]}
  capabilities = db.database_capabilities("default")

  assert facts["queue db"] == "default"
  assert facts["notify"] == ("on" if capabilities.supports_listen_notify else "unsupported")
  assert facts["skip locked"] == ("on" if capabilities.supports_skip_locked else "unsupported")


def test_dashboard_admin_title_includes_dj_queue(admin_client):
  response = admin_client.get(reverse("admin:dj_queue_dashboard_changelist"))

  assert response.status_code == 200
  content = response.content.decode()
  assert "<title>dj_queue | Django site admin</title>" in content
  assert "<h2>dashboard</h2>" not in content


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
  make_ready_job(queue_name="alpha", backend_alias="default")
  make_ready_job(queue_name="beta", backend_alias="secondary")

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


def test_dashboard_context_keeps_pause_and_recurring_rows_backend_scoped(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {},
    },
    "critical": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {"database_alias": "default"},
    },
  }
  make_failed_job(queue_name="alerts", backend_alias="critical")
  Pause.objects.create(backend_alias="critical", queue_name="alerts")
  make_ready_job(queue_name="alpha-demo", backend_alias="default")
  Pause.objects.create(backend_alias="default", queue_name="alpha-demo")
  RecurringTask.objects.create(
    backend_alias="default",
    key="alpha-nightly",
    task_path="tests.tasks.echo",
    payload={"args": ["alpha-nightly"], "kwargs": {}},
    schedule="0 0 * * *",
    queue_name="alpha-demo",
    priority=0,
    static=False,
  )

  context = dashboard.dashboard_context(backend_alias="critical")

  assert [row["name"] for row in context["queue_section"]["rows"]] == ["alerts"]
  assert context["summary_cards"][0]["label"] == "queues"
  assert context["summary_cards"][0]["value"] == 1
  assert context["summary_cards"][0]["detail"] == "1 paused"


def test_dashboard_does_not_render_shared_queue_section(admin_client, settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {},
    },
    "critical": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {"database_alias": "default"},
    },
  }
  make_failed_job(queue_name="alerts", backend_alias="critical")
  Pause.objects.create(backend_alias="critical", queue_name="alerts")
  Pause.objects.create(backend_alias="default", queue_name="alpha-demo")
  RecurringTask.objects.create(
    backend_alias="default",
    key="alpha-nightly",
    task_path="tests.tasks.echo",
    payload={"args": ["alpha-nightly"], "kwargs": {}},
    schedule="0 0 * * *",
    queue_name="alpha-demo",
    priority=0,
    static=False,
  )

  response = admin_client.get(
    reverse("admin:dj_queue_dashboard_changelist"),
    {"backend": "critical"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert 'id="shared-queue-summary"' not in content
  assert "<h2>shared queues</h2>" not in content


def test_dashboard_backend_selector_shows_backend_alias_only(admin_client, settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {},
    },
    "critical": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {"database_alias": "default"},
    },
  }

  response = admin_client.get(reverse("admin:dj_queue_dashboard_changelist"))

  assert response.status_code == 200
  content = response.content.decode()
  assert 'value="default" selected>default</option>' in content
  assert 'value="critical">critical</option>' in content


def test_dashboard_backend_switch_posts_to_base_dashboard_url(admin_client):
  response = admin_client.get(
    reverse("admin:dj_queue_dashboard_changelist"),
    {"queues_page": 2},
  )

  assert response.status_code == 200
  assert (
    f'<form method="get" action="{reverse("admin:dj_queue_dashboard_changelist")}">'
    in response.content.decode()
  )


def test_dashboard_processes_group_children_under_supervisor(admin_client):
  now = timezone.now()
  supervisor = Process.objects.create(
    kind="Supervisor",
    pid=101,
    hostname="localhost",
    name="supervisor-1",
    metadata={"mode": "async"},
    last_heartbeat_at=now,
  )
  Process.objects.create(
    kind="Dispatcher",
    pid=101,
    hostname="localhost",
    name="dispatcher-1",
    metadata={"polling_interval": 0.5},
    supervisor=supervisor,
    last_heartbeat_at=now,
  )

  response = admin_client.get(reverse("admin:dj_queue_dashboard_changelist"))

  assert response.status_code == 200
  content = response.content.decode()
  assert "supervisor-1" in content
  assert "dispatcher-1" in content
  assert "under supervisor-1" in content


def test_dashboard_process_groups_do_not_split_across_pages(admin_client, monkeypatch):
  monkeypatch.setitem(dashboard.OVERVIEW_PAGE_SIZES, "processes", 2)
  now = timezone.now()
  supervisor = Process.objects.create(
    kind="Supervisor",
    pid=101,
    hostname="localhost",
    name="alpha-supervisor",
    metadata={"mode": "async"},
    last_heartbeat_at=now,
  )
  Process.objects.create(
    kind="Dispatcher",
    pid=101,
    hostname="localhost",
    name="alpha-dispatcher",
    metadata={"polling_interval": 0.5},
    supervisor=supervisor,
    last_heartbeat_at=now,
  )
  Process.objects.create(
    kind="Worker",
    pid=101,
    hostname="localhost",
    name="alpha-worker",
    metadata={"queues": ["*"]},
    supervisor=supervisor,
    last_heartbeat_at=now,
  )
  Process.objects.create(
    kind="Supervisor",
    pid=202,
    hostname="localhost",
    name="zeta-supervisor",
    metadata={"mode": "async"},
    last_heartbeat_at=now,
  )

  first_page = admin_client.get(reverse("admin:dj_queue_dashboard_changelist"))
  second_page = admin_client.get(
    reverse("admin:dj_queue_dashboard_changelist"),
    {"processes_page": 2},
  )

  assert first_page.status_code == 200
  first_content = first_page.content.decode()
  second_content = second_page.content.decode()
  assert "alpha-supervisor" in first_content
  assert "alpha-dispatcher" in first_content
  assert "alpha-worker" in first_content
  assert "alpha-supervisor" not in second_content
  assert "alpha-dispatcher" not in second_content
  assert "alpha-worker" not in second_content
  assert "zeta-supervisor" in second_content


def test_dashboard_processes_sort_live_rows_first(admin_client):
  now = timezone.now()
  Process.objects.create(
    kind="Supervisor",
    pid=201,
    hostname="localhost",
    name="stale-supervisor",
    metadata={"mode": "async"},
    last_heartbeat_at=now - timedelta(minutes=10),
  )
  Process.objects.create(
    kind="Supervisor",
    pid=202,
    hostname="localhost",
    name="live-supervisor",
    metadata={"mode": "async"},
    last_heartbeat_at=now,
  )

  response = admin_client.get(reverse("admin:dj_queue_dashboard_changelist"))

  assert response.status_code == 200
  content = response.content.decode()
  assert content.index("live-supervisor") < content.index("stale-supervisor")


def test_dashboard_queue_section_supports_sorting(admin_client):
  make_ready_job(queue_name="alpha")
  make_ready_job(queue_name="alpha")
  make_ready_job(queue_name="beta")

  response = admin_client.get(
    reverse("admin:dj_queue_dashboard_changelist"),
    {"queues_sort": "-ready"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert content.index("alpha") < content.index("beta")
  assert "queues_sort=-ready" in content or "queues_sort=ready" in content


def test_dashboard_recurring_section_supports_sorting(admin_client):
  RecurringTask.objects.create(
    backend_alias="default",
    key="zeta",
    task_path="tests.tasks.echo",
    payload={"args": ["zeta"], "kwargs": {}},
    schedule="0 0 * * *",
    queue_name="default",
    priority=0,
    static=False,
  )
  RecurringTask.objects.create(
    backend_alias="default",
    key="alpha",
    task_path="tests.tasks.echo",
    payload={"args": ["alpha"], "kwargs": {}},
    schedule="0 0 * * *",
    queue_name="default",
    priority=0,
    static=False,
  )

  response = admin_client.get(
    reverse("admin:dj_queue_dashboard_changelist"),
    {"recurring_sort": "-key"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert content.index("zeta") < content.index("alpha")


def test_dashboard_recurring_key_links_to_raw_jobs(admin_client):
  task = RecurringTask.objects.create(
    backend_alias="default",
    key="nightly",
    task_path="tests.tasks.echo",
    payload={"args": [], "kwargs": {}},
    schedule="0 0 * * *",
    queue_name="default",
    priority=0,
    static=False,
  )
  job = make_job(queue_name="default")
  RecurringExecution.objects.create(
    backend_alias="default",
    job=job,
    task_key=task.key,
    run_at=timezone.now(),
  )

  response = admin_client.get(reverse("admin:dj_queue_dashboard_changelist"))

  assert response.status_code == 200
  content = response.content.decode()
  assert (
    f"{reverse('admin:dj_queue_job_changelist')}?backend=default&amp;recurring_task_key=nightly"
    in content
  )


def test_dashboard_recurring_timestamps_use_compact_format(admin_client):
  now = timezone.now().replace(microsecond=0)
  task = RecurringTask.objects.create(
    backend_alias="default",
    key="nightly",
    task_path="tests.tasks.echo",
    payload={"args": [], "kwargs": {}},
    schedule="0 0 * * *",
    queue_name="default",
    priority=0,
    static=False,
  )
  RecurringExecution.objects.create(
    backend_alias="default",
    job=None,
    task_key=task.key,
    run_at=now,
  )

  response = admin_client.get(reverse("admin:dj_queue_dashboard_changelist"))

  assert response.status_code == 200
  content = response.content.decode()
  assert timezone.localtime(now).strftime("%Y-%m-%d %H:%M:%S") in content
  expected_next_run = timezone.localtime(dashboard._next_run_at(task.schedule, now)).strftime(
    "%Y-%m-%d %H:%M:%S"
  )
  assert expected_next_run in content
  assert "a.m." not in content
  assert "p.m." not in content


def test_dashboard_semaphore_key_links_to_raw_jobs(admin_client):
  Semaphore.objects.create(
    key="acct:1",
    value=1,
    limit=2,
    expires_at=timezone.now() + timedelta(minutes=5),
  )
  make_job(queue_name="default", concurrency_key="acct:1")

  response = admin_client.get(reverse("admin:dj_queue_dashboard_changelist"))

  assert response.status_code == 200
  content = response.content.decode()
  assert (
    f"{reverse('admin:dj_queue_job_changelist')}?backend=default&amp;concurrency_key=acct%3A1"
    in content
  )


def test_dashboard_attention_card_links_to_failed_and_blocked_raw_jobs(admin_client):
  make_failed_job(queue_name="failed")
  make_blocked_job(queue_name="blocked")

  response = admin_client.get(reverse("admin:dj_queue_dashboard_changelist"))

  assert response.status_code == 200
  content = response.content.decode()
  assert f">{1} failed</a>" in content
  assert f"{reverse('admin:dj_queue_job_changelist')}?backend=default&amp;status=failed" in content
  assert f">{1} blocked</a>" in content
  assert (
    f"{reverse('admin:dj_queue_job_changelist')}?backend=default&amp;status=blocked" in content
  )


def test_dashboard_overview_pages_large_sections(admin_client):
  for index in range(19):
    make_ready_job(queue_name=f"queue-{index:02d}")

  response = admin_client.get(reverse("admin:dj_queue_dashboard_changelist"))

  assert response.status_code == 200
  content = response.content.decode()
  assert "queue-00" in content
  assert "queue-17" in content
  assert "queue-18" not in content
  assert "1-18 of 19 queues" in content
  assert 'aria-labelledby="pagination-queues"' in content
  assert 'aria-current="page">1</span>' in content
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
  job = make_ready_job(queue_name="alpha", backend_alias="secondary")
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
  scheduled_job = make_scheduled_job(queue_name="alpha")
  failed_job = make_failed_job(queue_name="alpha")
  finished_job = make_job(
    queue_name="alpha", finished_at=timezone.now(), return_value={"ok": True}
  )
  url = reverse("admin:dj_queue_dashboard_job_action", args=["alpha"])

  response = admin_client.post(
    url,
    {
      "backend": "default",
      "state": "failed",
      "action": "retry",
      "_selected_action": [str(failed_job.pk)],
    },
  )

  assert response.status_code == 302
  assert FailedExecution.objects.filter(job_id=failed_job.pk).exists() is False
  assert Job.objects.filter(pk=failed_job.pk, ready_execution__isnull=False).exists() is True

  response = admin_client.post(
    url,
    {
      "backend": "default",
      "state": "scheduled",
      "action": "discard",
      "_selected_action": [str(scheduled_job.pk)],
    },
  )

  assert response.status_code == 302
  assert Job.objects.filter(pk=scheduled_job.pk).exists() is False

  response = admin_client.post(
    url,
    {
      "backend": "default",
      "state": "finished",
      "action": "enqueue",
      "_selected_action": [str(finished_job.pk)],
    },
  )

  assert response.status_code == 302
  cloned_job = Job.objects.exclude(pk__in=[ready_job.pk, failed_job.pk, finished_job.pk]).get()
  assert cloned_job.task_path == finished_job.task_path
  assert cloned_job.priority == finished_job.priority
  assert cloned_job.payload == finished_job.payload
  assert cloned_job.backend_alias == finished_job.backend_alias
  assert ReadyExecution.objects.filter(job=cloned_job).exists() is True

  response = admin_client.post(
    url,
    {
      "backend": "default",
      "state": "ready",
      "action": "discard",
      "_selected_action": [str(ready_job.pk)],
    },
  )

  assert response.status_code == 302
  assert Job.objects.filter(pk=ready_job.pk).exists() is False


@pytest.mark.parametrize(
  ("state", "action", "factory", "state_model"),
  (
    ("ready", "discard", make_ready_job, ReadyExecution),
    ("scheduled", "discard", make_scheduled_job, ScheduledExecution),
    ("blocked", "discard", make_blocked_job, BlockedExecution),
    ("failed", "retry", make_failed_job, FailedExecution),
    ("failed", "discard", make_failed_job, FailedExecution),
  ),
)
def test_dashboard_queue_bulk_actions_are_queue_scoped(
  admin_client,
  state,
  action,
  factory,
  state_model,
):
  foreign_job = factory(queue_name="beta")
  url = reverse("admin:dj_queue_dashboard_job_action", args=["alpha"])

  response = admin_client.post(
    url,
    {
      "backend": "default",
      "state": state,
      "action": action,
      "_selected_action": [str(foreign_job.pk)],
    },
  )

  assert response.status_code == 302
  assert Job.objects.filter(pk=foreign_job.pk).exists() is True
  assert state_model.objects.filter(job=foreign_job).exists() is True


def test_dashboard_queue_bulk_actions_require_explicit_selection(admin_client):
  ready_job = make_ready_job(queue_name="alpha")
  url = reverse("admin:dj_queue_dashboard_job_action", args=["alpha"])

  response = admin_client.post(
    f"{url}?backend=default&state=ready",
    {
      "backend": "default",
      "state": "ready",
      "action": "",
      "_selected_action": [str(ready_job.pk)],
    },
    follow=True,
  )

  assert response.status_code == 200
  messages = list(response.context["messages"])
  assert [message.message for message in messages] == ["No action selected."]
  assert Job.objects.filter(pk=ready_job.pk).exists() is True


def test_dashboard_queue_view_uses_django_changelist_structure(admin_client):
  job = make_ready_job(queue_name="alpha")
  Process.objects.create(
    backend_alias="default",
    kind="Worker",
    pid=201,
    hostname="localhost",
    name="worker-1",
    metadata={"queues": ["alpha"]},
    last_heartbeat_at=timezone.now(),
  )

  response = admin_client.get(
    reverse("admin:dj_queue_dashboard_queue", args=["alpha"]),
    {"backend": "default", "state": "ready"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert '<ul class="object-tools">' not in content
  assert 'id="toolbar"' in content
  assert 'class="queue-toolbar-actions"' in content
  assert 'class="queue-button-pause"' in content
  assert 'class="toplinks queue-state-tabs"' in content
  assert 'class="queue-state-tab queue-state-tab-current"' in content
  assert "Backend:</strong> default" in content
  assert "Database:</strong> default" in content
  assert "Workers:</strong> 1" in content
  assert "Latency:</strong> " in content
  assert "Paused:</strong> no" in content
  assert '<option value="" selected>---------</option>' in content
  assert 'aria-current="page"' in content
  dashboard_url = f"{reverse('admin:dj_queue_dashboard_changelist')}?backend=default"
  queue_section_url = f"{dashboard_url}#queue-summary"
  assert content.count(f'<a href="{dashboard_url}">dj_queue</a>') == 2
  assert content.count(f'<a href="{queue_section_url}">queues</a>') == 2
  assert "<h1>" in content
  assert "› alpha" in content
  assert f'href="{reverse("admin:dj_queue_job_change", args=[job.pk])}?backend=default"' in content


def test_dashboard_queue_view_includes_django_admin_action_assets(admin_client):
  make_ready_job(queue_name="alpha")

  response = admin_client.get(
    reverse("admin:dj_queue_dashboard_queue", args=["alpha"]),
    {"backend": "default", "state": "ready"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert reverse("admin:jsi18n") in content
  assert 'src="/static/admin/js/actions.js"' in content


def test_dashboard_queue_view_uses_django_admin_action_field_names(admin_client):
  for _ in range(3):
    make_ready_job(queue_name="alpha")

  response = admin_client.get(
    reverse("admin:dj_queue_dashboard_queue", args=["alpha"]),
    {"backend": "default", "state": "ready"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert 'name="_selected_action"' in content
  assert 'data-actions-icnt="3"' in content
  assert 'id="action-toggle"' in content
  assert 'class="select-across"' in content


def test_dashboard_queue_view_exposes_scheduled_and_finished_actions(admin_client):
  make_scheduled_job(queue_name="alpha")
  make_job(queue_name="alpha", finished_at=timezone.now(), return_value={"ok": True})

  scheduled = admin_client.get(
    reverse("admin:dj_queue_dashboard_queue", args=["alpha"]),
    {"backend": "default", "state": "scheduled"},
  )
  finished = admin_client.get(
    reverse("admin:dj_queue_dashboard_queue", args=["alpha"]),
    {"backend": "default", "state": "finished"},
  )

  assert scheduled.status_code == 200
  assert '<option value="discard">discard selected</option>' in scheduled.content.decode()
  assert finished.status_code == 200
  assert '<option value="enqueue">enqueue selected again</option>' in finished.content.decode()


def test_dashboard_queue_controls_use_distinct_pause_resume_styles(admin_client):
  make_ready_job(queue_name="alpha")
  Pause.objects.create(backend_alias="default", queue_name="paused")

  overview = admin_client.get(reverse("admin:dj_queue_dashboard_changelist"))
  paused_queue = admin_client.get(
    reverse("admin:dj_queue_dashboard_queue", args=["paused"]),
    {"backend": "default", "state": "ready"},
  )

  assert overview.status_code == 200
  overview_content = overview.content.decode()
  assert 'class="button djq-button-pause"' in overview_content
  assert 'class="button djq-button-resume"' in overview_content

  assert paused_queue.status_code == 200
  assert 'class="queue-button-resume"' in paused_queue.content.decode()


def test_dashboard_paused_queue_hides_latency(admin_client):
  make_ready_job(queue_name="alpha")
  paused_job = make_ready_job(queue_name="paused")
  Pause.objects.create(backend_alias="default", queue_name="paused")

  rows = dashboard.dashboard_context(backend_alias="default")["queue_section"]["rows"]
  alpha_row = next(row for row in rows if row["name"] == "alpha")
  paused_row = next(row for row in rows if row["name"] == "paused")

  assert alpha_row["latency_seconds"] is not None
  assert paused_row["paused"] is True
  assert paused_row["latency_seconds"] is None
  assert paused_row["ready_count"] == 1

  response = admin_client.get(reverse("admin:dj_queue_dashboard_changelist"))

  assert response.status_code == 200
  content = response.content.decode()
  assert f">{paused_job.queue_name}</a>" in content


def test_dashboard_resumed_queue_latency_excludes_paused_time(admin_client):
  job = make_ready_job(queue_name="paused")
  ReadyExecution.objects.filter(job=job).update(
    created_at=timezone.now() - timedelta(seconds=40),
    latency_started_at=timezone.now() - timedelta(seconds=40),
  )
  Pause.objects.create(backend_alias="default", queue_name="paused")
  Pause.objects.filter(backend_alias="default", queue_name="paused").update(
    created_at=timezone.now() - timedelta(seconds=30)
  )

  dashboard.apply_queue_action(backend_alias="default", queue_name="paused", action="resume")

  queue_row = dashboard.queue_page_context(
    backend_alias="default",
    queue_name="paused",
    state="ready",
    page_number=1,
  )["queue_latency_seconds"]
  assert queue_row is not None
  assert queue_row < 15.0


def test_dashboard_queue_view_title_includes_dj_queue(admin_client):
  response = admin_client.get(
    reverse("admin:dj_queue_dashboard_queue", args=["alpha"]),
    {"backend": "default", "state": "finished"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert "<title>alpha | dj_queue | Django site admin</title>" in content
  assert "<h2>alpha</h2>" not in content


def test_dashboard_queue_view_raw_links_are_queue_scoped(admin_client):
  make_ready_job(queue_name="alpha")
  make_failed_job(queue_name="alpha")
  Pause.objects.create(backend_alias="default", queue_name="alpha")

  response = admin_client.get(
    reverse("admin:dj_queue_dashboard_queue", args=["alpha"]),
    {"backend": "default", "state": "ready"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert (
    f"{reverse('admin:dj_queue_job_changelist')}?backend=default&amp;queue_name=alpha&amp;status=ready"
    in content
  )
  assert (
    f"{reverse('admin:dj_queue_failedexecution_changelist')}?backend=default&amp;job__queue_name=alpha"
    in content
  )
  assert "Raw jobs" in content
  assert "Failed executions" in content
  assert "pauses" not in content


def test_dashboard_queue_view_hides_missing_raw_links(admin_client):
  make_ready_job(queue_name="alpha")

  response = admin_client.get(
    reverse("admin:dj_queue_dashboard_queue", args=["alpha"]),
    {"backend": "default", "state": "ready"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert "Raw jobs" in content
  assert "Failed executions" not in content
  assert "pauses" not in content


def test_dashboard_queue_view_zero_results_has_no_fake_page_number(admin_client):
  Pause.objects.create(backend_alias="default", queue_name="alpha")

  response = admin_client.get(
    reverse("admin:dj_queue_dashboard_queue", args=["alpha"]),
    {"backend": "default", "state": "finished"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert "0 jobs" in content
  assert "1 0 jobs" not in content


def test_dashboard_queue_view_supports_sorting(admin_client):
  make_ready_job(queue_name="alpha", task_path="tests.tasks.zeta")
  make_ready_job(queue_name="alpha", task_path="tests.tasks.alpha")

  response = admin_client.get(
    reverse("admin:dj_queue_dashboard_queue", args=["alpha"]),
    {"backend": "default", "state": "ready", "sort": "task"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert content.index("tests.tasks.alpha") < content.index("tests.tasks.zeta")
  assert "sorted ascending" in content
  assert '<div class="sortoptions">' in content
  assert "sort=-task" in content
  assert "sort=-task#result_list" not in content


def test_dashboard_queue_view_supports_multi_column_sorting(admin_client):
  low = make_ready_job(queue_name="alpha", task_path="tests.tasks.alpha", priority=0)
  high = make_ready_job(queue_name="alpha", task_path="tests.tasks.alpha", priority=2)
  make_ready_job(queue_name="alpha", task_path="tests.tasks.beta", priority=1)

  response = admin_client.get(
    reverse("admin:dj_queue_dashboard_queue", args=["alpha"]),
    {"backend": "default", "state": "ready", "sort": "task.-priority"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert content.index(str(high.id)) < content.index(str(low.id))
  assert 'title="Sorting priority: 1">1</span>' in content
  assert 'title="Sorting priority: 2">2</span>' in content
  assert "sort=-task.-priority" in content
  assert "sort=-task.-priority#result_list" not in content


def test_dashboard_queue_view_supports_sorting_finished_json_values(admin_client):
  alpha = make_job(queue_name="alpha", finished_at=timezone.now(), return_value={"a": 1})
  beta = make_job(queue_name="alpha", finished_at=timezone.now(), return_value={"b": 1})

  response = admin_client.get(
    reverse("admin:dj_queue_dashboard_queue", args=["alpha"]),
    {"backend": "default", "state": "finished", "sort": "return_value"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert content.index(str(alpha.id)) < content.index(str(beta.id))


def test_dashboard_queue_pagination_omits_default_sort(admin_client, monkeypatch):
  monkeypatch.setattr(dashboard, "PAGE_SIZE", 1)
  for index in range(3):
    make_ready_job(queue_name="alpha", task_path=f"tests.tasks.job_{index:02d}")

  response = admin_client.get(
    reverse("admin:dj_queue_dashboard_queue", args=["alpha"]),
    {"backend": "default", "state": "ready"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert "page=2#result_list" in content
  assert "page=2&amp;sort" not in content


def test_dashboard_queue_pagination_uses_elided_ranges(admin_client, monkeypatch):
  monkeypatch.setattr(dashboard, "PAGE_SIZE", 1)
  for index in range(12):
    make_ready_job(queue_name="alpha", task_path=f"tests.tasks.job_{index:02d}")

  response = admin_client.get(
    reverse("admin:dj_queue_dashboard_queue", args=["alpha"]),
    {"backend": "default", "state": "ready"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert "…" in content
  assert "page=12#result_list" in content


def test_dashboard_overview_pagination_omits_default_sort(admin_client, monkeypatch):
  monkeypatch.setitem(dashboard.OVERVIEW_PAGE_SIZES, "queues", 1)
  for index in range(3):
    make_ready_job(queue_name=f"queue-{index:02d}")

  response = admin_client.get(reverse("admin:dj_queue_dashboard_changelist"))

  assert response.status_code == 200
  content = response.content.decode()
  assert "queues_page=2" in content
  assert "queues_page=2&amp;queues_sort" not in content


def test_dashboard_overview_pagination_uses_elided_ranges(admin_client, monkeypatch):
  monkeypatch.setitem(dashboard.OVERVIEW_PAGE_SIZES, "queues", 1)
  for index in range(12):
    make_ready_job(queue_name=f"queue-{index:02d}")

  response = admin_client.get(reverse("admin:dj_queue_dashboard_changelist"))

  assert response.status_code == 200
  content = response.content.decode()
  assert "…" in content
  assert "queues_page=12" in content


def test_dashboard_result_count_text_format(admin_client):
  for i in range(5):
    make_ready_job(queue_name=f"queue-{i:02d}")

  response = admin_client.get(reverse("admin:dj_queue_dashboard_changelist"))

  content = response.content.decode()
  assert "1-5 of 5 queues" in content


def test_dashboard_result_count_text_single(admin_client):
  make_ready_job(queue_name="only")

  content = admin_client.get(
    reverse("admin:dj_queue_dashboard_changelist"),
  ).content.decode()

  assert "1-1 of 1 queue" in content


def test_dashboard_paginator_structure(admin_client):
  for i in range(19):
    make_ready_job(queue_name=f"queue-{i:02d}")

  content = admin_client.get(
    reverse("admin:dj_queue_dashboard_changelist"),
  ).content.decode()

  # django-standard nav with aria-labelledby and hidden h2
  assert '<nav class="paginator" aria-labelledby="pagination-queues">' in content
  assert '<h2 id="pagination-queues" class="visually-hidden">Pagination queues</h2>' in content

  # current page is a span, not an anchor with empty href
  assert '<span aria-current="page">1</span>' in content
  assert 'href="" aria-current' not in content

  # next page is a real link
  assert "queues_page=2" in content


def test_dashboard_paginator_page2_result_count(admin_client):
  for i in range(19):
    make_ready_job(queue_name=f"queue-{i:02d}")

  content = admin_client.get(
    reverse("admin:dj_queue_dashboard_changelist"),
    {"queues_page": 2},
  ).content.decode()

  assert "19-19 of 19 queues" in content


def test_dashboard_sort_no_indicators_on_default_load(admin_client):
  make_ready_job(queue_name="alpha")

  content = admin_client.get(
    reverse("admin:dj_queue_dashboard_changelist"),
  ).content.decode()

  # no sorted class on any th
  assert "sorted ascending" not in content
  assert "sorted descending" not in content
  # no sort option controls rendered
  assert "sortremove" not in content
  assert "sortpriority" not in content


def test_dashboard_sort_single_column_ascending(admin_client):
  make_ready_job(queue_name="beta")
  make_ready_job(queue_name="alpha")

  content = admin_client.get(
    reverse("admin:dj_queue_dashboard_changelist"),
    {"queues_sort": "name"},
  ).content.decode()

  assert "sorted ascending" in content
  assert content.index("alpha") < content.index("beta")
  # sort controls rendered
  assert "sortremove" in content


def test_dashboard_sort_single_column_descending(admin_client):
  make_ready_job(queue_name="alpha")
  make_ready_job(queue_name="beta")

  content = admin_client.get(
    reverse("admin:dj_queue_dashboard_changelist"),
    {"queues_sort": "-name"},
  ).content.decode()

  assert "sorted descending" in content
  assert content.index("beta") < content.index("alpha")


def test_dashboard_sort_multi_column_dot_separated(admin_client):
  make_ready_job(queue_name="alpha")
  make_ready_job(queue_name="alpha")
  make_ready_job(queue_name="beta")

  content = admin_client.get(
    reverse("admin:dj_queue_dashboard_changelist"),
    {"queues_sort": "-ready.name"},
  ).content.decode()

  # alpha has 2 ready, beta has 1, so alpha first with -ready
  assert content.index("alpha") < content.index("beta")
  # both columns show sorted class
  assert "sorted descending" in content
  assert "sorted ascending" in content
  # priority numbers displayed for multi-sort
  assert "sortpriority" in content


def test_dashboard_sort_multi_column_priority_numbers(admin_client):
  make_ready_job(queue_name="alpha")
  make_ready_job(queue_name="beta")

  content = admin_client.get(
    reverse("admin:dj_queue_dashboard_changelist"),
    {"queues_sort": "name.-ready"},
  ).content.decode()

  assert 'title="Sorting priority: 1">1</span>' in content
  assert 'title="Sorting priority: 2">2</span>' in content


def test_dashboard_sort_single_column_no_priority_numbers(admin_client):
  make_ready_job(queue_name="alpha")

  content = admin_client.get(
    reverse("admin:dj_queue_dashboard_changelist"),
    {"queues_sort": "name"},
  ).content.decode()

  assert "sortpriority" not in content


def test_dashboard_sort_primary_click_brings_to_front(admin_client):
  make_ready_job(queue_name="alpha")

  content = admin_client.get(
    reverse("admin:dj_queue_dashboard_changelist"),
    {"queues_sort": "name.-ready"},
  ).content.decode()

  # clicking "claimed" (unsorted) should produce claimed field at front
  assert "queues_sort=-claimed.name.-ready" in content


def test_dashboard_sort_toggle_keeps_position(admin_client):
  make_ready_job(queue_name="alpha")

  content = admin_client.get(
    reverse("admin:dj_queue_dashboard_changelist"),
    {"queues_sort": "name.-ready"},
  ).content.decode()

  # toggle link for "name" (currently asc) should flip to desc in place
  assert "queues_sort=-name.-ready" in content


def test_dashboard_sort_remove_from_chain(admin_client):
  make_ready_job(queue_name="alpha")

  content = admin_client.get(
    reverse("admin:dj_queue_dashboard_changelist"),
    {"queues_sort": "name.-ready"},
  ).content.decode()

  # remove link for "name" leaves only -ready
  assert 'href="?queues_sort=-ready#queue-summary"' in content
  # remove link for "ready" leaves only name
  assert 'href="?queues_sort=name#queue-summary"' in content


def test_dashboard_sort_links_preserve_section_anchors(admin_client):
  make_ready_job(queue_name="alpha")

  content = admin_client.get(
    reverse("admin:dj_queue_dashboard_changelist"),
    {"queues_sort": "name"},
  ).content.decode()

  assert "#queue-summary" in content


def test_dashboard_sort_invalid_field_falls_back_to_default(admin_client):
  make_ready_job(queue_name="alpha")
  make_ready_job(queue_name="beta")

  response = admin_client.get(
    reverse("admin:dj_queue_dashboard_changelist"),
    {"queues_sort": "nonexistent"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  # falls back to default sort, no crash, no sorted indicators
  assert "sorted ascending" not in content
  assert "sorted descending" not in content


def test_dashboard_sort_preserves_sort_in_page_links(admin_client):
  for i in range(19):
    make_ready_job(queue_name=f"queue-{i:02d}")

  content = admin_client.get(
    reverse("admin:dj_queue_dashboard_changelist"),
    {"queues_sort": "-ready"},
  ).content.decode()

  # page 2 link should preserve the sort parameter
  assert "queues_sort=-ready" in content
  assert "queues_page=2" in content


def test_dashboard_inherits_django_admin_css(admin_client):
  make_ready_job(queue_name="alpha")

  content = admin_client.get(
    reverse("admin:dj_queue_dashboard_changelist"),
  ).content.decode()

  # changelists.css provides paginator and table chrome
  assert "admin/css/changelists.css" in content
  # base_site.html provides base.css with .sorted, .sortable, .paginator styles
  assert "admin/css/base.css" in content


def test_dashboard_header_uses_django_th_structure(admin_client):
  make_ready_job(queue_name="alpha")

  content = admin_client.get(
    reverse("admin:dj_queue_dashboard_changelist"),
    {"queues_sort": "name"},
  ).content.decode()

  # headers use <div class="text"><a> structure matching django admin
  assert '<div class="text"><a role="button"' in content
  # sorted headers include sortoptions with toggle and sortremove
  assert '<div class="sortoptions">' in content
  assert 'class="sortremove"' in content
  assert 'class="toggle ascending"' in content


def test_dashboard_sort_all_sections(admin_client):
  now = timezone.now()
  make_ready_job(queue_name="alpha")
  Process.objects.create(
    backend_alias="default",
    kind="Worker",
    pid=1,
    hostname="localhost",
    name="w-1",
    metadata={},
    last_heartbeat_at=now,
  )
  RecurringTask.objects.create(
    backend_alias="default",
    key="nightly",
    task_path="tests.tasks.echo",
    payload={"args": [], "kwargs": {}},
    schedule="0 0 * * *",
    queue_name="default",
    priority=0,
    static=False,
  )
  Semaphore.objects.create(
    key="acct:1",
    value=1,
    limit=2,
    expires_at=now + timedelta(minutes=5),
  )

  response = admin_client.get(
    reverse("admin:dj_queue_dashboard_changelist"),
    {
      "queues_sort": "name",
      "processes_sort": "-status",
      "recurring_sort": "key",
      "semaphores_sort": "-available",
    },
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert "queues_sort=name" in content
  assert "processes_sort=-status" in content
  assert "recurring_sort=key" in content
  assert "semaphores_sort=-available" in content


def test_dashboard_sort_deduplicates_fields(admin_client):
  make_ready_job(queue_name="alpha")

  content = admin_client.get(
    reverse("admin:dj_queue_dashboard_changelist"),
    {"queues_sort": "name.name.-name"},
  ).content.decode()

  assert content.count("sorted ascending") == 1
  assert "sorted descending" not in content


def test_dashboard_page1_link_navigates_instead_of_anchor_only(admin_client, monkeypatch):
  monkeypatch.setitem(dashboard.OVERVIEW_PAGE_SIZES, "queues", 2)
  for i in range(5):
    make_ready_job(queue_name=f"q-{i:02d}")

  content = admin_client.get(
    reverse("admin:dj_queue_dashboard_changelist"),
    {"queues_page": 2},
  ).content.decode()

  # page 1 link must include "?" to trigger navigation, not just "#queue-summary"
  assert 'href="?#queue-summary"' in content


def test_dashboard_links_to_raw_admin_tables(admin_client, settings):
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

  response = admin_client.get(
    reverse("admin:dj_queue_dashboard_changelist"),
    {"backend": "secondary"},
  )

  assert response.status_code == 200
  content = response.content.decode()
  assert f'href="{reverse("admin:dj_queue_job_changelist")}?backend=secondary"' in content
  assert ">Jobs</a>" in content
  assert (
    f'href="{reverse("admin:dj_queue_failedexecution_changelist")}?backend=secondary"' in content
  )
  assert ">Failed executions</a>" in content
  assert f'href="{reverse("admin:dj_queue_process_changelist")}?backend=secondary"' in content
  assert ">Processes</a>" in content
  assert (
    f'href="{reverse("admin:dj_queue_recurringtask_changelist")}?backend=secondary"' in content
  )
  assert ">Recurring tasks</a>" in content
  assert f'href="{reverse("admin:dj_queue_pause_changelist")}?backend=secondary"' in content
  assert ">Pauses</a>" in content
  assert f'href="{reverse("admin:dj_queue_semaphore_changelist")}?backend=secondary"' in content
  assert ">Semaphores</a>" in content


def test_dashboard_backend_aliases_ignore_non_dj_queue_backends(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {},
    },
    "other": {
      "BACKEND": "other.backend.Backend",
      "QUEUES": [],
      "OPTIONS": {},
    },
  }

  assert dashboard.configured_backend_aliases() == ("default",)
  assert dashboard.resolve_backend_alias(None) == "default"


def test_dashboard_backend_resolution_handles_no_dj_queue_backends(settings):
  settings.TASKS = {
    "other": {
      "BACKEND": "other.backend.Backend",
      "QUEUES": [],
      "OPTIONS": {},
    },
  }

  with pytest.raises(Http404, match="no dj_queue backends"):
    dashboard.resolve_backend_alias(None)


def test_dashboard_rejects_non_dj_queue_backend_alias(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {},
    },
    "other": {
      "BACKEND": "other.backend.Backend",
      "QUEUES": [],
      "OPTIONS": {},
    },
  }

  with pytest.raises(Http404, match="unknown dj_queue backend"):
    dashboard.resolve_backend_alias("other")
