from datetime import timedelta

import pytest
from django.http import HttpResponse
from django.test import RequestFactory
from django.utils import timezone
from django.utils.module_loading import import_string

from bin import dev_admin
from dj_queue.models import Job, Process


pytestmark = pytest.mark.django_db(transaction=True)


def test_seed_demo_data_uses_importable_demo_task_paths():
  dev_admin.seed_demo_data()

  task_paths = set(
    Job.objects.filter(task_path__startswith="demo.tasks.").values_list("task_path", flat=True)
  )

  assert task_paths
  for task_path in task_paths:
    task = import_string(task_path)
    assert task.module_path == task_path


def test_seeded_process_heartbeat_middleware_refreshes_dashboard_rows():
  dev_admin.seed_demo_data()
  stale_at = timezone.now() - timedelta(minutes=10)
  Process.objects.filter(name__in=dev_admin.SEEDED_PROCESS_NAMES).update(last_heartbeat_at=stale_at)
  request_started_at = timezone.now()

  middleware = dev_admin.SeededProcessHeartbeatMiddleware(lambda request: HttpResponse("ok"))
  response = middleware(RequestFactory().get("/admin/"))

  assert response.status_code == 200
  refreshed = list(
    Process.objects.filter(name__in=dev_admin.SEEDED_PROCESS_NAMES).values_list(
      "last_heartbeat_at", flat=True
    )
  )
  assert len(refreshed) == len(dev_admin.SEEDED_PROCESS_NAMES)
  assert all(last_heartbeat_at >= request_started_at for last_heartbeat_at in refreshed)
