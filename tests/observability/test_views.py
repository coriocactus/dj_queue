import sys
import types

import pytest
from django.urls import include, path, reverse
from django.utils import timezone

from dj_queue.models import Job, Pause, Process, ReadyExecution, RecurringTask, Semaphore


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
  ReadyExecution.objects.create(job=job, queue_name=job.queue_name, priority=job.priority)
  return job


@pytest.fixture(autouse=True)
def observability_urls(settings):
  module = types.ModuleType("tests.observability_urls")
  module.urlpatterns = [path("dj_queue/", include("dj_queue.urls"))]
  sys.modules[module.__name__] = module
  settings.ROOT_URLCONF = module.__name__
  yield
  sys.modules.pop(module.__name__, None)


def test_stats_endpoint_returns_all_backend_snapshots(client, settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {"database_alias": "default"},
    },
    "critical": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {"database_alias": "default"},
    },
  }
  now = timezone.now()
  make_ready_job(queue_name="alpha", backend_alias="default")
  Pause.objects.create(backend_alias="critical", queue_name="shared")
  RecurringTask.objects.create(
    backend_alias="critical",
    key="nightly",
    task_path="tests.tasks.echo",
    payload={"args": [], "kwargs": {}},
    schedule="0 0 * * *",
    queue_name="shared",
    priority=0,
    static=False,
  )
  Semaphore.objects.create(
    key="account:1",
    value=1,
    limit=2,
    expires_at=now,
  )
  Process.objects.create(
    backend_alias="default",
    kind="Worker",
    pid=101,
    hostname="localhost",
    name="worker-1",
    metadata={"queues": ["alpha"]},
    last_heartbeat_at=now,
  )

  response = client.get(reverse("dj_queue:stats"))

  assert response.status_code == 200
  payload = response.json()
  assert [backend["backend_alias"] for backend in payload["backends"]] == ["default", "critical"]
  default = payload["backends"][0]
  assert default["runner_metrics"]["live"] == 1
  assert default["queues"][0]["name"] == "alpha"
  critical = payload["backends"][1]
  assert [queue["name"] for queue in critical["queues"]] == ["shared"]
  assert critical["recurring"][0]["key"] == "nightly"
  assert critical["semaphores"][0]["key"] == "account:1"


def test_metrics_endpoint_renders_prometheus_text(client, settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {"database_alias": "default"},
    }
  }
  now = timezone.now()
  make_ready_job(queue_name="alpha", backend_alias="default")
  Process.objects.create(
    backend_alias="default",
    kind="Worker",
    pid=101,
    hostname="localhost",
    name="worker-1",
    metadata={"queues": ["alpha"]},
    last_heartbeat_at=now,
  )

  response = client.get(reverse("dj_queue:metrics"))

  assert response.status_code == 200
  assert response["Content-Type"].startswith("text/plain; version=0.0.4")
  content = response.content.decode()
  assert "dj_queue_queue_jobs" in content
  assert 'backend="default"' in content
  assert 'queue="alpha"' in content
  assert 'state="ready"' in content


def test_observability_endpoints_allow_open_access_when_token_unset(client, settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {"database_alias": "default"},
    }
  }

  stats = client.get(reverse("dj_queue:stats"))
  metrics = client.get(reverse("dj_queue:metrics"))

  assert stats.status_code == 200
  assert metrics.status_code == 200


def test_observability_endpoints_require_bearer_token_when_configured(client, settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {"database_alias": "default"},
    }
  }
  settings.DJ_QUEUE_OBSERVABILITY_TOKEN = "secret-token"

  stats = client.get(reverse("dj_queue:stats"))
  metrics = client.get(reverse("dj_queue:metrics"))

  assert stats.status_code == 401
  assert stats["WWW-Authenticate"] == "Bearer"
  assert metrics.status_code == 401
  assert metrics["WWW-Authenticate"] == "Bearer"


def test_observability_endpoints_accept_matching_bearer_token(client, settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {"database_alias": "default"},
    }
  }
  settings.DJ_QUEUE_OBSERVABILITY_TOKEN = "secret-token"

  stats = client.get(
    reverse("dj_queue:stats"),
    headers={"Authorization": "Bearer secret-token"},
  )
  metrics = client.get(
    reverse("dj_queue:metrics"),
    headers={"Authorization": "Bearer secret-token"},
  )

  assert stats.status_code == 200
  assert metrics.status_code == 200
