#!/usr/bin/env -S uv run --with uvicorn

import argparse
from datetime import timedelta
import os
from pathlib import Path
import sys
from uuid import UUID

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
  sys.path.insert(0, str(PROJECT_ROOT))


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def parse_args(argv):
  parser = argparse.ArgumentParser(
    description="Run a one-process dj_queue admin development server.",
  )
  parser.add_argument("--host", default=os.environ.get("DJ_QUEUE_DEV_ADMIN_HOST", "127.0.0.1"))
  parser.add_argument(
    "--port",
    type=int,
    default=int(os.environ.get("DJ_QUEUE_DEV_ADMIN_PORT", "17777")),
  )
  parser.add_argument(
    "--db",
    default=os.environ.get(
      "DJ_QUEUE_DEV_ADMIN_DB",
      str(PROJECT_ROOT / ".dev" / "dj_queue_admin.sqlite3"),
    ),
  )
  parser.add_argument(
    "--username",
    default=os.environ.get("DJ_QUEUE_DEV_ADMIN_USER", "admin"),
  )
  parser.add_argument(
    "--password",
    default=os.environ.get("DJ_QUEUE_DEV_ADMIN_PASSWORD", "password"),
  )
  parser.add_argument(
    "--no-seed",
    action="store_true",
    help="Keep existing dj_queue rows instead of reseeding demo data on startup.",
  )
  return parser.parse_args(argv)


ARGS = parse_args(sys.argv[1:])
DB_PATH = Path(ARGS.db).expanduser().resolve()
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

from django.conf import settings  # noqa: E402

if not settings.configured:
  settings.configure(
    SECRET_KEY="dev-admin",
    DEBUG=True,
    ALLOWED_HOSTS=["*"],
    USE_TZ=True,
    DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
    ROOT_URLCONF=__name__,
    STATIC_URL="/static/",
    DATABASES={
      "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": str(DB_PATH),
      }
    },
    INSTALLED_APPS=[
      "django.contrib.admin",
      "django.contrib.auth",
      "django.contrib.contenttypes",
      "django.contrib.sessions",
      "django.contrib.messages",
      "django.contrib.staticfiles",
      "dj_queue",
    ],
    MIDDLEWARE=[
      "django.middleware.common.CommonMiddleware",
      "django.contrib.sessions.middleware.SessionMiddleware",
      "django.middleware.csrf.CsrfViewMiddleware",
      "django.contrib.auth.middleware.AuthenticationMiddleware",
      "django.contrib.messages.middleware.MessageMiddleware",
      "django.middleware.clickjacking.XFrameOptionsMiddleware",
    ],
    TEMPLATES=[
      {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
          "context_processors": [
            "django.template.context_processors.request",
            "django.contrib.auth.context_processors.auth",
            "django.contrib.messages.context_processors.messages",
          ]
        },
      }
    ],
    DATABASE_ROUTERS=["dj_queue.routers.DjQueueRouter"],
    SESSION_ENGINE="django.contrib.sessions.backends.db",
    TASKS={
      "default": {
        "BACKEND": "dj_queue.backend.DjQueueBackend",
        "QUEUES": [],
        "OPTIONS": {
          "mode": "async",
          "workers": [
            {
              "queues": "*",
              "threads": 1,
              "processes": 1,
              "polling_interval": 0.05,
            }
          ],
          "dispatchers": [
            {
              "batch_size": 100,
              "polling_interval": 0.5,
              "concurrency_maintenance": True,
              "concurrency_maintenance_interval": 60,
            }
          ],
          "scheduler": {
            "dynamic_tasks_enabled": False,
            "polling_interval": 5,
          },
          "recurring": {},
          "preserve_finished_jobs": True,
          "clear_finished_jobs_after": None,
          "process_heartbeat_interval": 1,
          "process_alive_threshold": 5,
          "listen_notify": False,
        },
      }
    },
  )


# ---------------------------------------------------------------------------
# Django Setup
# ---------------------------------------------------------------------------


import django  # noqa: E402

django.setup()

from django.contrib import admin  # noqa: E402
from django.contrib.auth import get_user_model  # noqa: E402
from django.contrib.staticfiles.handlers import ASGIStaticFilesHandler  # noqa: E402
from django.core.asgi import get_asgi_application  # noqa: E402
from django.core.management import call_command  # noqa: E402
from django.http import HttpResponse, JsonResponse  # noqa: E402
from django.urls import path  # noqa: E402
from django.utils import timezone  # noqa: E402

import uvicorn  # noqa: E402

from dj_queue.contrib.asgi import DjQueueLifespan  # noqa: E402
from dj_queue.models import (  # noqa: E402
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
from tests.tasks import add  # noqa: E402


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------


def bootstrap():
  call_command("migrate", interactive=False, verbosity=0)
  ensure_superuser(ARGS.username, ARGS.password)
  if not ARGS.no_seed:
    seed_demo_data()


def ensure_superuser(username, password):
  user_model = get_user_model()
  user, _created = user_model.objects.get_or_create(
    username=username,
    defaults={
      "email": f"{username}@example.com",
      "is_staff": True,
      "is_superuser": True,
    },
  )
  if not user.is_staff or not user.is_superuser:
    user.is_staff = True
    user.is_superuser = True
  user.set_password(password)
  user.save(update_fields=["email", "is_staff", "is_superuser", "password"])
  return user


# ---------------------------------------------------------------------------
# Demo Data
# ---------------------------------------------------------------------------


def make_job(**overrides):
  payload = {"args": list(overrides.pop("args", [])), "kwargs": dict(overrides.pop("kwargs", {}))}
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


def seed_demo_data():
  now = timezone.now()

  Job.objects.all().delete()
  Process.objects.exclude(kind="Supervisor").delete()
  Process.objects.filter(kind="Supervisor").delete()
  RecurringExecution.objects.all().delete()
  RecurringTask.objects.all().delete()
  Pause.objects.all().delete()
  Semaphore.objects.all().delete()

  Pause.objects.create(queue_name="paused-demo")
  Semaphore.objects.create(
    key="account:demo",
    value=1,
    limit=2,
    expires_at=now + timedelta(minutes=10),
  )
  RecurringTask.objects.create(
    key="demo-nightly",
    task_path="tests.tasks.echo",
    payload={"args": ["nightly"], "kwargs": {}},
    schedule="0 0 1 1 *",
    queue_name="maintenance",
    priority=-5,
    description="demo recurring task",
    static=False,
  )

  ready_job = make_job(queue_name="paused-demo", priority=10, args=["ready-demo"])
  ReadyExecution.objects.create(
    job=ready_job,
    queue_name=ready_job.queue_name,
    priority=ready_job.priority,
  )

  scheduled_job = make_job(
    queue_name="scheduled-demo",
    priority=5,
    scheduled_at=now + timedelta(hours=1),
    args=["scheduled-demo"],
  )
  ScheduledExecution.objects.create(
    job=scheduled_job,
    queue_name=scheduled_job.queue_name,
    priority=scheduled_job.priority,
    scheduled_at=scheduled_job.scheduled_at,
  )

  blocked_job = make_job(
    queue_name="blocked-demo",
    priority=1,
    concurrency_key="account:demo",
    args=["blocked-demo"],
  )
  BlockedExecution.objects.create(
    job=blocked_job,
    queue_name=blocked_job.queue_name,
    priority=blocked_job.priority,
    concurrency_key=blocked_job.concurrency_key,
    expires_at=now + timedelta(minutes=15),
  )

  failed_job = make_job(queue_name="failed-demo", args=["boom"])
  FailedExecution.objects.create(
    job=failed_job,
    exception_class="builtins.ValueError",
    message="boom",
    traceback="traceback",
  )

  make_job(
    task_path="tests.tasks.add",
    queue_name="finished-demo",
    args=[2, 3],
    finished_at=now,
    return_value=5,
  )


# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------


def index(_request):
  return HttpResponse(
    """
    <html>
      <body style=\"font-family: sans-serif; max-width: 56rem; margin: 3rem auto; line-height: 1.5;\">
        <h1>dj_queue admin dev</h1>
        <p><a href=\"/admin/\">Open Django admin</a></p>
        <p><a href=\"/enqueue/\">Enqueue a live demo job</a></p>
        <p>default login: <code>admin</code> / <code>password</code></p>
      </body>
    </html>
    """
  )


def enqueue_job(_request):
  task_result = add.enqueue(2, 3)
  return JsonResponse(
    {
      "job_id": str(task_result.id),
      "status": str(getattr(task_result.status, "value", task_result.status)).lower(),
    }
  )


def result_job(_request, job_id: UUID):
  task_result = add.get_backend().get_result(job_id)
  status = str(getattr(task_result.status, "value", task_result.status)).lower()
  return JsonResponse(
    {
      "job_id": str(job_id),
      "status": status,
      "return_value": task_result.return_value if status == "successful" else None,
      "process_count": Process.objects.count(),
    }
  )


# ---------------------------------------------------------------------------
# Routing And ASGI
# ---------------------------------------------------------------------------


urlpatterns = [
  path("", index),
  path("admin/", admin.site.urls),
  path("enqueue/", enqueue_job),
  path("result/<uuid:job_id>/", result_job),
]


application = DjQueueLifespan(ASGIStaticFilesHandler(get_asgi_application()))


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main():
  bootstrap()
  print(f"admin db: {DB_PATH}")
  print(f"admin url: http://{ARGS.host}:{ARGS.port}/admin/")
  print(f"login: {ARGS.username} / {ARGS.password}")
  uvicorn.run(application, host=ARGS.host, port=ARGS.port, lifespan="on", log_level="info")


if __name__ == "__main__":
  main()
