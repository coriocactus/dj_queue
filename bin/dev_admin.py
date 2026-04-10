#!/usr/bin/env -S uv run --with uvicorn --with watchfiles

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
    default=os.environ.get("DJ_QUEUE_DEV_ADMIN_DB"),
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
    default=_env_flag("DJ_QUEUE_DEV_ADMIN_NO_SEED", False),
    help="Keep existing dj_queue rows instead of reseeding demo data on startup.",
  )
  parser.add_argument(
    "--reload",
    dest="reload",
    action="store_true",
    default=_env_flag("DJ_QUEUE_DEV_ADMIN_RELOAD", True),
    help="Reload on Python or template changes while developing the dashboard.",
  )
  parser.add_argument(
    "--no-reload",
    dest="reload",
    action="store_false",
    help="Disable code reload.",
  )
  args = parser.parse_args(argv)
  if args.db is None:
    args.db = str(PROJECT_ROOT / ".dev" / f"dj_queue_admin_{args.port}.sqlite3")
  return args


def _env_flag(name, default):
  raw = os.environ.get(name)
  if raw is None:
    return default
  return raw.strip().lower() in {"1", "true", "yes", "on"}


ARGS = parse_args(sys.argv[1:] if __name__ == "__main__" else [])
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
              "queues": ["interactive", "maintenance", "reports"],
              "threads": 1,
              "processes": 1,
              "polling_interval": 0.05,
            },
          ],
          "dispatchers": [],
          "scheduler": {
            "dynamic_tasks_enabled": False,
            "polling_interval": 5,
          },
          "recurring": {},
          "preserve_finished_jobs": True,
          "clear_finished_jobs_after": None,
          "process_heartbeat_interval": 1,
          "process_alive_threshold": 120,
          "listen_notify": False,
        },
      },
      "critical": {
        "BACKEND": "dj_queue.backend.DjQueueBackend",
        "QUEUES": ["critical-paused", "alerts", "critical-review"],
        "OPTIONS": {
          "database_alias": "default",
          "mode": "async",
          "workers": [],
          "dispatchers": [],
          "scheduler": {
            "dynamic_tasks_enabled": True,
            "polling_interval": 10,
          },
          "recurring": {},
          "preserve_finished_jobs": True,
          "clear_finished_jobs_after": None,
          "process_heartbeat_interval": 1,
          "process_alive_threshold": 120,
          "listen_notify": False,
        },
      },
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
from tests.tasks import add, echo, fail, sleep_for  # noqa: E402


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------


def bootstrap():
  call_command("migrate", interactive=False, verbosity=0)
  ensure_superuser(ARGS.username, ARGS.password)
  if not ARGS.no_seed:
    seed_demo_data()


def export_runtime_env():
  os.environ["DJ_QUEUE_DEV_ADMIN_HOST"] = ARGS.host
  os.environ["DJ_QUEUE_DEV_ADMIN_PORT"] = str(ARGS.port)
  os.environ["DJ_QUEUE_DEV_ADMIN_DB"] = str(DB_PATH)
  os.environ["DJ_QUEUE_DEV_ADMIN_USER"] = ARGS.username
  os.environ["DJ_QUEUE_DEV_ADMIN_PASSWORD"] = ARGS.password
  os.environ["DJ_QUEUE_DEV_ADMIN_RELOAD"] = "1" if ARGS.reload else "0"
  if ARGS.no_seed:
    os.environ["DJ_QUEUE_DEV_ADMIN_NO_SEED"] = "1"
  else:
    os.environ.pop("DJ_QUEUE_DEV_ADMIN_NO_SEED", None)


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
  Pause.objects.create(queue_name="critical-paused")
  Pause.objects.create(queue_name="bulk-paused")

  semaphore_specs = (
    ("account:demo", 1, 2, 10),
    ("account:reporting", 0, 1, 20),
    ("mailer:burst", 2, 3, 5),
    ("tenant:alpha", 3, 4, 8),
    ("tenant:beta", 2, 4, 12),
    ("tenant:gamma", 1, 4, 16),
    ("tenant:delta", 0, 2, 18),
    ("tenant:epsilon", 1, 2, 22),
    ("tenant:zeta", 4, 5, 7),
    ("tenant:eta", 2, 3, 11),
    ("tenant:theta", 1, 3, 14),
    ("tenant:iota", 0, 2, 17),
    ("tenant:kappa", 2, 2, 24),
  )
  for key, value, limit, expiry_minutes in semaphore_specs:
    Semaphore.objects.create(
      key=key,
      value=value,
      limit=limit,
      expires_at=now + timedelta(minutes=expiry_minutes),
    )

  recurring_specs = (
    ("demo-nightly", "maintenance", "0 0 1 1 *", -5, "demo recurring task", False, 540),
    ("demo-hourly-report", "reports", "0 * * * *", 5, "hourly report build", True, 40),
    (
      "critical-audit",
      "critical-review",
      "*/15 * * * *",
      10,
      "critical backend audit sweep",
      False,
      5,
    ),
    ("cleanup-stale-sessions", "maintenance", "15 * * * *", 0, "cleanup stale sessions", True, 75),
    (
      "refresh-dashboard-caches",
      "interactive",
      "*/10 * * * *",
      0,
      "refresh dashboard caches",
      False,
      9,
    ),
    ("ship-daily-digest", "alerts", "30 7 * * *", 3, "ship daily digest", True, 700),
    ("fetch-billing-events", "reports", "*/20 * * * *", 4, "fetch billing events", False, 12),
    ("rebuild-search-index", "maintenance", "0 2 * * *", 8, "rebuild search index", True, 900),
    (
      "critical-sla-check",
      "critical-review",
      "*/5 * * * *",
      12,
      "check critical sla windows",
      False,
      3,
    ),
    ("expire-demo-tokens", "interactive", "45 * * * *", 1, "expire demo tokens", False, 22),
    ("sync-crm", "reports", "0 */2 * * *", 2, "sync crm", True, 110),
    ("notify-stuck-jobs", "alerts", "*/30 * * * *", 6, "notify stuck jobs", False, 25),
    ("trim-finished-jobs", "maintenance", "0 3 * * *", -1, "trim finished jobs", True, 820),
  )
  for key, queue_name, schedule, priority, description, static, minutes_ago in recurring_specs:
    RecurringTask.objects.create(
      key=key,
      task_path="tests.tasks.echo",
      payload={"args": [key], "kwargs": {}},
      schedule=schedule,
      queue_name=queue_name,
      priority=priority,
      description=description,
      static=static,
    )
    RecurringExecution.objects.create(task_key=key, run_at=now - timedelta(minutes=minutes_ago))

  legacy_supervisor = Process.objects.create(
    kind="Supervisor",
    pid=88001,
    hostname="legacy-host.local",
    name="legacy-supervisor-1",
    metadata={
      "mode": "async",
      "worker_count": 2,
      "dispatcher_count": 1,
      "has_scheduler": True,
    },
    last_heartbeat_at=now - timedelta(seconds=18),
  )
  Process.objects.create(
    kind="Dispatcher",
    pid=88011,
    hostname="legacy-host.local",
    name="legacy-dispatcher-1",
    metadata={"batch_size": 50, "polling_interval": 1.0},
    supervisor=legacy_supervisor,
    last_heartbeat_at=now - timedelta(seconds=17),
  )
  Process.objects.create(
    kind="Scheduler",
    pid=88012,
    hostname="legacy-host.local",
    name="legacy-scheduler-1",
    metadata={"dynamic_tasks_enabled": True, "polling_interval": 5},
    supervisor=legacy_supervisor,
    last_heartbeat_at=now - timedelta(seconds=17),
  )
  Process.objects.create(
    kind="Worker",
    pid=88021,
    hostname="legacy-host.local",
    name="legacy-worker-1",
    metadata={"queues": ["interactive", "reports"], "threads": 1},
    supervisor=legacy_supervisor,
    last_heartbeat_at=now - timedelta(seconds=16),
  )
  Process.objects.create(
    kind="Worker",
    pid=88022,
    hostname="legacy-host.local",
    name="legacy-worker-2",
    metadata={"queues": ["maintenance"], "threads": 1},
    supervisor=legacy_supervisor,
    last_heartbeat_at=now - timedelta(seconds=15),
  )

  current_supervisor = Process.objects.create(
    kind="Supervisor",
    pid=99001,
    hostname="dashboard-host.local",
    name="dashboard-supervisor-1",
    metadata={
      "mode": "async",
      "worker_count": 4,
      "dispatcher_count": 1,
      "has_scheduler": True,
    },
    last_heartbeat_at=now - timedelta(seconds=2),
  )
  Process.objects.create(
    kind="Dispatcher",
    pid=99011,
    hostname="dashboard-host.local",
    name="dashboard-dispatcher-1",
    metadata={"batch_size": 100, "polling_interval": 0.5},
    supervisor=current_supervisor,
    last_heartbeat_at=now - timedelta(seconds=1),
  )
  Process.objects.create(
    kind="Scheduler",
    pid=99012,
    hostname="dashboard-host.local",
    name="dashboard-scheduler-1",
    metadata={"dynamic_tasks_enabled": True, "polling_interval": 10},
    supervisor=current_supervisor,
    last_heartbeat_at=now - timedelta(seconds=1),
  )
  for index, queue_selector in enumerate(
    (
      ["interactive", "reports"],
      ["maintenance"],
      ["alerts", "critical-review"],
      ["bulk-*"],
    ),
    start=1,
  ):
    Process.objects.create(
      kind="Worker",
      pid=99020 + index,
      hostname="dashboard-host.local",
      name=f"dashboard-worker-{index}",
      metadata={"queues": queue_selector, "threads": 1},
      supervisor=current_supervisor,
      last_heartbeat_at=now - timedelta(seconds=index),
    )

  for index in range(3):
    ready_job = make_job(
      queue_name="paused-demo",
      priority=10 - index,
      args=[f"paused-{index}"],
    )
    ReadyExecution.objects.create(
      job=ready_job,
      queue_name=ready_job.queue_name,
      priority=ready_job.priority,
    )

  for index in range(5):
    ready_job = make_job(
      queue_name="backfill-import",
      priority=index % 3,
      args=[f"backfill-{index}"],
    )
    ReadyExecution.objects.create(
      job=ready_job,
      queue_name=ready_job.queue_name,
      priority=ready_job.priority,
    )

  for index in range(14):
    queue_name = f"bulk-queue-{index + 1:02d}"
    ready_job = make_job(
      queue_name=queue_name,
      priority=index % 5,
      args=[queue_name],
    )
    ReadyExecution.objects.create(
      job=ready_job,
      queue_name=ready_job.queue_name,
      priority=ready_job.priority,
    )

  critical_ready = make_job(
    backend_name="critical",
    queue_name="critical-paused",
    priority=15,
    args=["critical-ready"],
  )
  ReadyExecution.objects.create(
    job=critical_ready,
    queue_name=critical_ready.queue_name,
    priority=critical_ready.priority,
  )

  for offset, name in ((1, "scheduled-demo"), (6, "reports-later")):
    scheduled_job = make_job(
      queue_name=name,
      priority=5,
      scheduled_at=now + timedelta(hours=offset),
      args=[name],
    )
    ScheduledExecution.objects.create(
      job=scheduled_job,
      queue_name=scheduled_job.queue_name,
      priority=scheduled_job.priority,
      scheduled_at=scheduled_job.scheduled_at,
    )

  blocked_specs = (
    ("blocked-demo", "demo", "account:demo", 1, 15),
    ("report-waiters", "reporting", "account:reporting", 4, 25),
  )
  for queue_name, account_id, concurrency_key, priority, expiry_minutes in blocked_specs:
    blocked_job = make_job(
      task_path="tests.tasks.limited",
      queue_name=queue_name,
      priority=priority,
      concurrency_key=concurrency_key,
      args=[account_id, queue_name],
    )
    BlockedExecution.objects.create(
      job=blocked_job,
      queue_name=blocked_job.queue_name,
      priority=blocked_job.priority,
      concurrency_key=blocked_job.concurrency_key,
      expires_at=now + timedelta(minutes=expiry_minutes),
    )

  failed_specs = (
    ("failed-demo", "default", "boom"),
    ("alerts", "critical", "smtp timeout"),
  )
  for queue_name, backend_name, message in failed_specs:
    failed_job = make_job(queue_name=queue_name, backend_name=backend_name, args=[message])
    FailedExecution.objects.create(
      job=failed_job,
      exception_class="builtins.ValueError",
      message=message,
      traceback="traceback",
    )

  make_job(
    task_path="tests.tasks.add",
    queue_name="finished-demo",
    args=[2, 3],
    finished_at=now,
    return_value=5,
  )
  make_job(
    task_path="tests.tasks.echo",
    queue_name="reports",
    args=["finished-report"],
    finished_at=now - timedelta(minutes=3),
    return_value="finished-report",
  )
  make_job(
    task_path="tests.tasks.echo",
    backend_name="critical",
    queue_name="critical-review",
    args=["finished-critical"],
    finished_at=now - timedelta(minutes=15),
    return_value="finished-critical",
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
        <p><a href=\"/enqueue-burst/\">Enqueue a burst of demo jobs</a></p>
        <p><a href=\"/seed/\">Reset seeded dashboard data</a></p>
        <p>default login: <code>admin</code> / <code>password</code></p>
      </body>
    </html>
    """
  )


def enqueue_job(_request):
  task_result = add.using(queue_name="interactive").enqueue(2, 3)
  return JsonResponse(
    {
      "job_id": str(task_result.id),
      "status": str(getattr(task_result.status, "value", task_result.status)).lower(),
    }
  )


def enqueue_burst(_request):
  results = [
    echo.using(queue_name="interactive").enqueue("demo-live-a"),
    echo.using(queue_name="reports").enqueue("demo-live-b"),
    fail.using(queue_name="interactive").enqueue("burst-failure"),
    sleep_for.using(queue_name="interactive").enqueue(2),
  ]
  return JsonResponse({"job_ids": [str(result.id) for result in results]})


def reset_seed(_request):
  seed_demo_data()
  return JsonResponse({"seeded": True, "job_count": Job.objects.count()})


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
  path("enqueue-burst/", enqueue_burst),
  path("seed/", reset_seed),
  path("result/<uuid:job_id>/", result_job),
]


application = DjQueueLifespan(ASGIStaticFilesHandler(get_asgi_application()))


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main():
  export_runtime_env()
  bootstrap()
  print(f"admin db: {DB_PATH}")
  print(f"admin url: http://{ARGS.host}:{ARGS.port}/admin/")
  print(f"login: {ARGS.username} / {ARGS.password}")
  if ARGS.reload:
    uvicorn.run(
      "bin.dev_admin:application",
      host=ARGS.host,
      port=ARGS.port,
      lifespan="on",
      log_level="info",
      reload=True,
      reload_dirs=[
        str(PROJECT_ROOT / "bin"),
        str(PROJECT_ROOT / "dj_queue"),
        str(PROJECT_ROOT / "tests"),
      ],
      reload_includes=["*.py", "*.html"],
    )
    return
  uvicorn.run(application, host=ARGS.host, port=ARGS.port, lifespan="on", log_level="info")


if __name__ == "__main__":
  main()
