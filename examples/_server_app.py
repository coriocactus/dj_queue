import os

from examples._example import ensure_project_on_path

ensure_project_on_path()

import django
from django.conf import settings

DB_PATH = os.environ.get("DJ_QUEUE_EXAMPLE_SERVER_DB")
if not DB_PATH:
  raise RuntimeError("DJ_QUEUE_EXAMPLE_SERVER_DB is required")

if not settings.configured:
  settings.configure(
    SECRET_KEY="examples",
    DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
    USE_TZ=True,
    ALLOWED_HOSTS=["*"],
    ROOT_URLCONF=__name__,
    DATABASES={
      "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": DB_PATH,
      }
    },
    INSTALLED_APPS=["dj_queue"],
    DATABASE_ROUTERS=["dj_queue.routers.DjQueueRouter"],
    TASKS={
      "default": {
        "BACKEND": "dj_queue.backend.DjQueueBackend",
        "QUEUES": [],
        "OPTIONS": {
          "mode": "async",
          "workers": [{"queues": "*", "threads": 1, "processes": 1, "polling_interval": 0.01}],
          "dispatchers": [],
          "scheduler": {"dynamic_tasks_enabled": False, "polling_interval": 5},
          "recurring": {},
          "process_heartbeat_interval": 1,
          "process_alive_threshold": 5,
          "preserve_finished_jobs": True,
          "clear_finished_jobs_after": None,
        },
      }
    },
  )

django.setup()

from django.core.asgi import get_asgi_application
from django.core.management import call_command
from django.core.wsgi import get_wsgi_application
from django.db import connections
from django.http import JsonResponse
from django.tasks import task
from django.urls import path

from dj_queue.contrib.asgi import DjQueueLifespan
from dj_queue.models import Job, Pause, Process, RecurringTask, Semaphore


def prepare_database():
  call_command("migrate", "dj_queue", interactive=False, verbosity=0)
  for model in (Job, Pause, Process, RecurringTask, Semaphore):
    model.objects.all().delete()
  connections.close_all()


def _status_name(status):
  return str(getattr(status, "value", status)).lower()


@task
def echo(value):
  return value


def health(_request):
  return JsonResponse({"ok": True})


def enqueue_job(request):
  value = request.GET.get("value", "embedded")
  task_result = echo.enqueue(value)
  return JsonResponse(
    {
      "job_id": str(task_result.id),
      "status": _status_name(task_result.status),
    }
  )


def job_result(_request, job_id):
  task_result = echo.get_backend().get_result(job_id)
  status = _status_name(task_result.status)
  return JsonResponse(
    {
      "job_id": str(job_id),
      "status": status,
      "return_value": task_result.return_value if status == "successful" else None,
      "worker_processes": Process.objects.filter(kind="Worker").count(),
    }
  )


urlpatterns = [
  path("health", health),
  path("enqueue", enqueue_job),
  path("result/<uuid:job_id>", job_result),
]

wsgi_application = get_wsgi_application()
asgi_application = DjQueueLifespan(get_asgi_application(), forward_wrapped_lifespan=False)
