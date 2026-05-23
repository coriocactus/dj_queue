#!/usr/bin/env -S uv run --with uvicorn --with watchfiles

import argparse
import os
import sys
import types
from datetime import timedelta
from pathlib import Path
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
    "--auto-login",
    dest="auto_login",
    action="store_true",
    default=_env_flag("DJ_QUEUE_DEV_ADMIN_AUTO_LOGIN", True),
    help="Auto-sign in to Django admin with the dev superuser.",
  )
  parser.add_argument(
    "--login-required",
    dest="auto_login",
    action="store_false",
    help="Require the normal Django admin login screen.",
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

DEMO_STATIC_RECURRING_SPECS = (
  ("demo-hourly-report", "reports", "0 * * * *", 5, "hourly report build", 40),
  ("cleanup-stale-sessions", "maintenance", "15 * * * *", 0, "cleanup stale sessions", 75),
  ("ship-daily-digest", "alerts", "30 7 * * *", 3, "ship daily digest", 700),
  ("rebuild-search-index", "maintenance", "0 2 * * *", 8, "rebuild search index", 900),
  ("sync-crm", "reports", "0 */2 * * *", 2, "sync crm", 110),
  ("trim-finished-jobs", "maintenance", "0 3 * * *", -1, "trim finished jobs", 820),
)

DEMO_STATIC_RECURRING = {
  key: {
    "task_path": "tests.tasks.echo",
    "schedule": schedule,
    "queue_name": queue_name,
    "priority": priority,
    "description": description,
    "args": [key],
  }
  for key, queue_name, schedule, priority, description, _minutes_ago in DEMO_STATIC_RECURRING_SPECS
}

from django.conf import settings  # noqa: E402

if not settings.configured:
  settings.configure(
    SECRET_KEY="dev-admin",
    DEBUG=True,
    ALLOWED_HOSTS=["*"],
    USE_TZ=True,
    TIME_ZONE="Europe/London",
    DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
    ROOT_URLCONF=__name__,
    STATIC_URL="/static/",
    DATABASES={
      "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": str(DB_PATH),
        "OPTIONS": {
          "timeout": 5,
          "transaction_mode": "IMMEDIATE",
        },
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
      "bin.admin.SeededProcessHeartbeatMiddleware",
      *(["bin.admin.AutoLoginMiddleware"] if ARGS.auto_login else []),
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
        "OPTIONS": {},
      },
      "demo": {
        "BACKEND": "dj_queue.backend.DjQueueBackend",
        "QUEUES": [],
        "OPTIONS": {
          "mode": "async",
          "workers": [
            {
              "queues": ["*"],
              "threads": 1,
              "processes": 1,
              "polling_interval": 0.2,
            },
          ],
          "dispatchers": [{"batch_size": 100, "polling_interval": 1}],
          "scheduler": {
            "dynamic_tasks_enabled": True,
            "polling_interval": 5,
          },
          "recurring": DEMO_STATIC_RECURRING,
          "preserve_finished_jobs": True,
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

from django.db.backends.signals import connection_created  # noqa: E402


def _enable_wal(sender, connection, **kwargs):
  if connection.vendor == "sqlite":
    with connection.cursor() as cursor:
      cursor.execute("PRAGMA journal_mode=WAL;")
      cursor.execute("PRAGMA synchronous=NORMAL;")
      cursor.execute("PRAGMA cache_size=-20000;")
      cursor.execute("PRAGMA temp_store=MEMORY;")
      cursor.execute("PRAGMA mmap_size=2147483648;")


connection_created.connect(_enable_wal)

from django.contrib import admin  # noqa: E402
from django.contrib.auth import get_user_model, login  # noqa: E402
from django.contrib.staticfiles.handlers import ASGIStaticFilesHandler  # noqa: E402
from django.core.asgi import get_asgi_application  # noqa: E402
from django.core.management import call_command  # noqa: E402
from django.http import HttpResponse, JsonResponse  # noqa: E402
from django.tasks import task  # noqa: E402
from django.urls import include, path  # noqa: E402
from django.utils import timezone  # noqa: E402

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


SEEDED_PROCESS_NAMES = (
  "dashboard-supervisor",
  "dashboard-dispatcher",
  "dashboard-scheduler",
  "dashboard-worker-alpha",
)


def _demo_result(name, account_id=None):
  return {"task": name, "account_id": account_id}


def _register_demo_task(
  module, name, *, concurrency_key=None, concurrency_limit=None, concurrency_duration=None
):
  def implementation(account_id):
    return _demo_result(name, account_id)

  implementation.__name__ = name
  implementation.__qualname__ = name
  implementation.__module__ = module.__name__
  demo_task = task(implementation)
  if concurrency_key is not None:
    demo_task.func.concurrency_key = concurrency_key
  if concurrency_limit is not None:
    demo_task.func.concurrency_limit = concurrency_limit
  if concurrency_duration is not None:
    demo_task.func.concurrency_duration = concurrency_duration
  setattr(module, name, demo_task)


def _install_demo_tasks_module():
  demo_package = types.ModuleType("demo")
  demo_package.__path__ = []
  tasks_module = types.ModuleType("demo.tasks")
  tasks_module.__package__ = "demo"

  for name in (
    "refresh_customer_cache",
    "generate_statement_pdf",
    "build_account_snapshot",
    "send_digest",
    "push_billing_webhook",
    "fetch_billing_events",
    "trim_finished_exports",
  ):
    _register_demo_task(tasks_module, name)

  _register_demo_task(
    tasks_module,
    "sync_account",
    concurrency_key="account:{account_id}",
    concurrency_limit=2,
    concurrency_duration=360,
  )

  demo_package.tasks = tasks_module
  sys.modules["demo"] = demo_package
  sys.modules["demo.tasks"] = tasks_module


_install_demo_tasks_module()


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------


class AutoLoginMiddleware:
  def __init__(self, get_response):
    self.get_response = get_response

  def __call__(self, request):
    if not request.user.is_authenticated:
      user = get_user_model().objects.get(username=ARGS.username)
      login(request, user, backend="django.contrib.auth.backends.ModelBackend")
    return self.get_response(request)


class SeededProcessHeartbeatMiddleware:
  def __init__(self, get_response):
    self.get_response = get_response

  def __call__(self, request):
    refresh_seeded_process_heartbeats()
    return self.get_response(request)


def bootstrap():
  call_command("migrate", interactive=False, verbosity=0)
  ensure_superuser(ARGS.username, ARGS.password)
  if not ARGS.no_seed:
    seed_demo_data()
    assert_seed_demo_data_includes_demo_static_recurring_config()
    assert_seed_demo_data_uses_importable_demo_task_paths()
    assert_seed_demo_data_keeps_alpha_queue_ready_until_manual_unpause()
    assert_seeded_process_heartbeat_middleware_refreshes_dashboard_rows()


def export_runtime_env():
  os.environ["DJ_QUEUE_DEV_ADMIN_HOST"] = ARGS.host
  os.environ["DJ_QUEUE_DEV_ADMIN_PORT"] = str(ARGS.port)
  os.environ["DJ_QUEUE_DEV_ADMIN_DB"] = str(DB_PATH)
  os.environ["DJ_QUEUE_DEV_ADMIN_USER"] = ARGS.username
  os.environ["DJ_QUEUE_DEV_ADMIN_PASSWORD"] = ARGS.password
  os.environ["DJ_QUEUE_DEV_ADMIN_AUTO_LOGIN"] = "1" if ARGS.auto_login else "0"
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


def refresh_seeded_process_heartbeats(*, now=None):
  if now is None:
    now = timezone.now()
  Process.objects.filter(name__in=SEEDED_PROCESS_NAMES).update(last_heartbeat_at=now)


# ---------------------------------------------------------------------------
# Harness Assertions
# ---------------------------------------------------------------------------


def assert_seed_demo_data_uses_importable_demo_task_paths():
  from django.utils.module_loading import import_string

  task_paths = set(
    Job.objects.filter(task_path__startswith="demo.tasks.").values_list("task_path", flat=True)
  )

  assert task_paths
  for task_path in task_paths:
    task = import_string(task_path)
    assert task.module_path == task_path


def assert_seeded_process_heartbeat_middleware_refreshes_dashboard_rows():
  from django.http import HttpResponse
  from django.test import RequestFactory

  stale_at = timezone.now() - timedelta(minutes=10)
  Process.objects.filter(backend_alias="demo", name__in=SEEDED_PROCESS_NAMES).update(
    last_heartbeat_at=stale_at
  )

  request_started_at = timezone.now()
  middleware = SeededProcessHeartbeatMiddleware(lambda request: HttpResponse("ok"))
  response = middleware(RequestFactory().get("/admin/"))
  assert response.status_code == 200

  refreshed = list(
    Process.objects.filter(backend_alias="demo", name__in=SEEDED_PROCESS_NAMES).values_list(
      "last_heartbeat_at", flat=True
    )
  )
  assert len(refreshed) == len(SEEDED_PROCESS_NAMES)
  assert all(last_heartbeat_at >= request_started_at for last_heartbeat_at in refreshed)


def assert_seed_demo_data_includes_demo_static_recurring_config():
  configured_keys = set(DEMO_STATIC_RECURRING)
  seeded_keys = set(
    RecurringTask.objects.filter(
      backend_alias="demo",
      static=True,
      key__in=configured_keys,
    ).values_list("key", flat=True)
  )

  assert configured_keys
  assert seeded_keys == configured_keys


def assert_seed_demo_data_keeps_alpha_queue_ready_until_manual_unpause():
  alpha_ready_jobs = list(
    ReadyExecution.objects.filter(queue_name="alpha-demo")
    .select_related("job")
    .order_by("-priority", "job_id")
  )

  assert [row.job.task_path for row in alpha_ready_jobs] == [
    "tests.tasks.sleep_for",
    "demo.tasks.refresh_customer_cache",
    "demo.tasks.generate_statement_pdf",
  ]
  assert alpha_ready_jobs[0].job.payload == {"args": [90], "kwargs": {}}
  assert Job.objects.filter(queue_name="alpha-demo", claimed_execution__isnull=False).count() == 0


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
    backend_alias=overrides.pop("backend_alias", "demo"),
    scheduled_at=overrides.pop("scheduled_at", None),
    concurrency_key=overrides.pop("concurrency_key", None),
    finished_at=overrides.pop("finished_at", None),
    return_value=overrides.pop("return_value", None),
    **overrides,
  )


def seed_demo_data():
  now = timezone.now()
  critical_queue_names = set(settings.TASKS["critical"]["QUEUES"])
  seeded_backends = {"demo", "critical"}

  def backend_alias_for_queue(queue_name):
    if queue_name in critical_queue_names:
      return "critical"
    return "demo"

  Job.objects.all().delete()
  Process.objects.filter(backend_alias__in=seeded_backends).delete()
  RecurringExecution.objects.all().delete()
  RecurringTask.objects.all().delete()
  Pause.objects.all().delete()
  Semaphore.objects.all().delete()

  for queue_name in (
    "paused-demo",
    "alpha-demo",
    "critical-paused",
    "bulk-paused",
    "backfill-import",
  ):
    Pause.objects.create(
      backend_alias=backend_alias_for_queue(queue_name),
      queue_name=queue_name,
    )

  dashboard_supervisor = Process.objects.create(
    backend_alias="demo",
    kind="Supervisor",
    pid=9101,
    hostname="dashboard.local",
    name="dashboard-supervisor",
    metadata={"mode": "async"},
    last_heartbeat_at=now,
  )
  Process.objects.create(
    backend_alias="demo",
    kind="Dispatcher",
    pid=9102,
    hostname="dashboard.local",
    name="dashboard-dispatcher",
    metadata={"polling_interval": 1},
    supervisor=dashboard_supervisor,
    last_heartbeat_at=now,
  )
  Process.objects.create(
    backend_alias="demo",
    kind="Scheduler",
    pid=9103,
    hostname="dashboard.local",
    name="dashboard-scheduler",
    metadata={"polling_interval": 5},
    supervisor=dashboard_supervisor,
    last_heartbeat_at=now,
  )
  Process.objects.create(
    backend_alias="demo",
    kind="Worker",
    pid=9104,
    hostname="dashboard.local",
    name="dashboard-worker-alpha",
    metadata={"queues": ["alpha-demo"], "threads": 1},
    supervisor=dashboard_supervisor,
    last_heartbeat_at=now,
  )

  semaphore_specs = (
    ("account:alpha-demo", 1, 2, 360),
    ("account:demo", 1, 2, 480),
    ("account:reporting", 0, 1, 600),
    ("mailer:burst", 2, 3, 360),
    ("tenant:alpha", 3, 4, 420),
    ("tenant:beta", 2, 4, 540),
    ("tenant:gamma", 1, 4, 660),
    ("tenant:delta", 0, 2, 720),
    ("tenant:epsilon", 1, 2, 510),
    ("tenant:zeta", 4, 5, 390),
    ("tenant:eta", 2, 3, 450),
    ("tenant:theta", 1, 3, 570),
    ("tenant:iota", 0, 2, 630),
    ("tenant:kappa", 2, 2, 480),
  )
  for key, value, limit, expiry_minutes in semaphore_specs:
    Semaphore.objects.create(
      key=key,
      value=value,
      limit=limit,
      expires_at=now + timedelta(minutes=expiry_minutes),
    )

  recurring_specs = (
    ("demo", "demo-nightly", "maintenance", "0 0 1 1 *", -5, "demo recurring task", False, 540),
    (
      "critical",
      "critical-audit",
      "critical-review",
      "*/15 * * * *",
      10,
      "critical backend audit sweep",
      False,
      5,
    ),
    (
      "demo",
      "refresh-dashboard-caches",
      "interactive",
      "*/10 * * * *",
      0,
      "refresh dashboard caches",
      False,
      9,
    ),
    (
      "demo",
      "fetch-billing-events",
      "reports",
      "*/20 * * * *",
      4,
      "fetch billing events",
      False,
      12,
    ),
    (
      "critical",
      "critical-sla-check",
      "critical-review",
      "*/5 * * * *",
      12,
      "check critical sla windows",
      False,
      3,
    ),
    (
      "demo",
      "expire-demo-tokens",
      "interactive",
      "45 * * * *",
      1,
      "expire demo tokens",
      False,
      22,
    ),
    ("demo", "notify-stuck-jobs", "alerts", "*/30 * * * *", 6, "notify stuck jobs", False, 25),
    *(
      ("demo", key, queue_name, schedule, priority, description, True, minutes_ago)
      for key, queue_name, schedule, priority, description, minutes_ago in DEMO_STATIC_RECURRING_SPECS
    ),
  )
  for (
    backend_alias,
    key,
    queue_name,
    schedule,
    priority,
    description,
    static,
    minutes_ago,
  ) in recurring_specs:
    RecurringTask.objects.update_or_create(
      backend_alias=backend_alias,
      key=key,
      defaults={
        "task_path": "tests.tasks.echo",
        "payload": {"args": [key], "kwargs": {}},
        "schedule": schedule,
        "queue_name": queue_name,
        "priority": priority,
        "description": description,
        "static": static,
      },
    )
    RecurringExecution.objects.create(
      backend_alias=backend_alias,
      task_key=key,
      run_at=now - timedelta(minutes=minutes_ago),
    )

  for index in range(3):
    ready_job = make_job(
      queue_name="paused-demo",
      priority=10 - index,
      args=[f"paused-{index}"],
    )
    ReadyExecution.objects.create(
      job=ready_job,
      backend_alias=ready_job.backend_alias,
      queue_name=ready_job.queue_name,
      priority=ready_job.priority,
    )

  for priority, task_path in (
    (25, "tests.tasks.sleep_for"),
    (20, "demo.tasks.refresh_customer_cache"),
    (5, "demo.tasks.generate_statement_pdf"),
  ):
    ready_job = make_job(
      queue_name="alpha-demo",
      priority=priority,
      task_path=task_path,
      args=[90] if task_path == "tests.tasks.sleep_for" else ["acct_42"],
    )
    ReadyExecution.objects.create(
      job=ready_job,
      backend_alias=ready_job.backend_alias,
      queue_name=ready_job.queue_name,
      priority=ready_job.priority,
    )

  scheduled_job = make_job(
    queue_name="alpha-demo",
    priority=8,
    task_path="demo.tasks.send_digest",
    scheduled_at=now + timedelta(minutes=45),
    args=["acct_42"],
  )
  ScheduledExecution.objects.create(
    job=scheduled_job,
    backend_alias=scheduled_job.backend_alias,
    queue_name=scheduled_job.queue_name,
    priority=scheduled_job.priority,
    scheduled_at=scheduled_job.scheduled_at,
  )

  blocked_job = make_job(
    task_path="demo.tasks.sync_account",
    queue_name="alpha-demo",
    priority=9,
    concurrency_key="account:alpha-demo",
    args=["acct_42"],
  )
  BlockedExecution.objects.create(
    job=blocked_job,
    backend_alias=blocked_job.backend_alias,
    queue_name=blocked_job.queue_name,
    priority=blocked_job.priority,
    concurrency_key=blocked_job.concurrency_key,
    expires_at=now + timedelta(minutes=30),
  )

  for task_path, exception_class, message in (
    ("demo.tasks.push_billing_webhook", "builtins.TimeoutError", "provider timed out"),
    ("demo.tasks.fetch_billing_events", "builtins.ConnectionError", "upstream returned 502"),
  ):
    failed_job = make_job(
      queue_name="alpha-demo",
      priority=4,
      task_path=task_path,
      args=["acct_42"],
    )
    FailedExecution.objects.create(
      job=failed_job,
      exception_class=exception_class,
      message=message,
      traceback="traceback",
    )

  make_job(
    task_path="demo.tasks.trim_finished_exports",
    queue_name="alpha-demo",
    args=["acct_42"],
    finished_at=now - timedelta(minutes=12),
    return_value={"status": "ok", "rows": 3},
  )

  for index in range(5):
    ready_job = make_job(
      queue_name="backfill-import",
      priority=index % 3,
      args=[f"backfill-{index}"],
    )
    ReadyExecution.objects.create(
      job=ready_job,
      backend_alias=ready_job.backend_alias,
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
      backend_alias=ready_job.backend_alias,
      queue_name=ready_job.queue_name,
      priority=ready_job.priority,
    )
    Pause.objects.create(backend_alias="demo", queue_name=queue_name)

  critical_ready = make_job(
    backend_alias="critical",
    queue_name="critical-paused",
    priority=15,
    args=["critical-ready"],
  )
  ReadyExecution.objects.create(
    job=critical_ready,
    backend_alias=critical_ready.backend_alias,
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
      backend_alias=scheduled_job.backend_alias,
      queue_name=scheduled_job.queue_name,
      priority=scheduled_job.priority,
      scheduled_at=scheduled_job.scheduled_at,
    )

  blocked_specs = (
    ("blocked-demo", "demo", "account:demo", 1, 480),
    ("report-waiters", "reporting", "account:reporting", 4, 600),
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
      backend_alias=blocked_job.backend_alias,
      queue_name=blocked_job.queue_name,
      priority=blocked_job.priority,
      concurrency_key=blocked_job.concurrency_key,
      expires_at=now + timedelta(minutes=expiry_minutes),
    )

  failed_specs = (
    ("failed-demo", "demo", "boom"),
    ("alerts", "critical", "smtp timeout"),
  )
  for queue_name, backend_alias, message in failed_specs:
    failed_job = make_job(queue_name=queue_name, backend_alias=backend_alias, args=[message])
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
    backend_alias="critical",
    queue_name="critical-review",
    args=["finished-critical"],
    finished_at=now - timedelta(minutes=15),
    return_value="finished-critical",
  )


# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------


def index(_request):
  access_copy = ""
  if not ARGS.auto_login:
    access_copy = f"default login: <code>{ARGS.username}</code> / <code>{ARGS.password}</code>"

  return HttpResponse(
    f"""
    <html>
      <body style=\"font-family: sans-serif; max-width: 56rem; margin: 3rem auto; line-height: 1.5;\">
        <h1>dj_queue admin dev</h1>
        <p><a href=\"/admin/\">Open Django admin</a></p>
        <p><a href=\"/dj_queue/stats.json\">Open queue statistics JSON</a></p>
        <p><a href=\"/dj_queue/metrics\">Open Prometheus metrics</a></p>
        <p><a href=\"/enqueue/\">Enqueue a live demo job</a></p>
        <p><a href=\"/enqueue-burst/\">Enqueue a burst of demo jobs</a></p>
        <p><a href=\"/seed/\">Reset seeded dashboard data</a></p>
        <p>seeded operator data lives primarily under backend <code>demo</code></p>
        <p>{access_copy}</p>
      </body>
    </html>
    """
  )


def enqueue_job(_request):
  task_result = add.using(backend="demo", queue_name="interactive").enqueue(2, 3)
  return JsonResponse(
    {
      "job_id": str(task_result.id),
      "status": str(getattr(task_result.status, "value", task_result.status)).lower(),
    }
  )


def enqueue_burst(_request):
  results = [
    echo.using(backend="demo", queue_name="interactive").enqueue("demo-live-a"),
    echo.using(backend="demo", queue_name="reports").enqueue("demo-live-b"),
    fail.using(backend="demo", queue_name="interactive").enqueue("burst-failure"),
    sleep_for.using(backend="demo", queue_name="interactive").enqueue(2),
  ]
  return JsonResponse({"job_ids": [str(result.id) for result in results]})


def reset_seed(_request):
  seed_demo_data()
  assert_seed_demo_data_includes_demo_static_recurring_config()
  assert_seed_demo_data_uses_importable_demo_task_paths()
  assert_seed_demo_data_keeps_alpha_queue_ready_until_manual_unpause()
  assert_seeded_process_heartbeat_middleware_refreshes_dashboard_rows()
  return JsonResponse({"seeded": True, "job_count": Job.objects.count()})


def result_job(_request, job_id: UUID):
  task_result = add.using(backend="demo").get_backend().get_result(job_id)
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
  path("dj_queue/", include("dj_queue.urls")),
  path("enqueue/", enqueue_job),
  path("enqueue-burst/", enqueue_burst),
  path("seed/", reset_seed),
  path("result/<uuid:job_id>/", result_job),
]


application = DjQueueLifespan(
  ASGIStaticFilesHandler(get_asgi_application()),
  backend_alias="demo",
  forward_wrapped_lifespan=False,
)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main():
  import uvicorn

  export_runtime_env()
  bootstrap()
  print(f"admin db: {DB_PATH}")
  print(f"admin url: http://{ARGS.host}:{ARGS.port}/admin/")
  if not ARGS.auto_login:
    print(f"login: {ARGS.username} / {ARGS.password}")
  if ARGS.reload:
    uvicorn.run(
      "bin.admin:application",
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
