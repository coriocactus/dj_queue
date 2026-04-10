"""shared django configuration for single-database examples.

importing this module configures django, runs migrations, and clears queue state
"""

import importlib
import os
import sys

from _example import ensure_project_on_path

ensure_project_on_path()

import django  # noqa: E402
from django.conf import settings  # noqa: E402
from django.db import connections  # noqa: E402

DB_BACKEND = os.environ.get("DB_BACKEND", "sqlite")
MYSQL_FAMILY = {"mysql", "mariadb"}

DEFAULT_DB_USER = "dj_queue"
DEFAULT_DB_PASSWORD = "dj_queue"
if DB_BACKEND in MYSQL_FAMILY:
  DEFAULT_DB_USER = "root"
  DEFAULT_DB_PASSWORD = "root"

if DB_BACKEND in MYSQL_FAMILY:
  pymysql = importlib.import_module("pymysql")
  pymysql.version_info = (2, 2, 1, "final", 0)
  pymysql.__version__ = "2.2.1"
  pymysql.install_as_MySQLdb()

_DATABASES = {
  "sqlite": {
    "ENGINE": "django.db.backends.sqlite3",
    "NAME": ":memory:",
  },
  "postgres": {
    "ENGINE": "django.db.backends.postgresql",
    "NAME": os.environ.get("DB_NAME", "dj_queue_test"),
    "USER": os.environ.get("DB_USER", DEFAULT_DB_USER),
    "PASSWORD": os.environ.get("DB_PASSWORD", DEFAULT_DB_PASSWORD),
    "HOST": os.environ.get("DB_HOST", "127.0.0.1"),
    "PORT": os.environ.get("DB_PORT", "17432"),
  },
  "mysql": {
    "ENGINE": "django.db.backends.mysql",
    "NAME": os.environ.get("DB_NAME", "dj_queue_test"),
    "USER": os.environ.get("DB_USER", DEFAULT_DB_USER),
    "PASSWORD": os.environ.get("DB_PASSWORD", DEFAULT_DB_PASSWORD),
    "HOST": os.environ.get("DB_HOST", "127.0.0.1"),
    "PORT": os.environ.get("DB_PORT", "17312"),
  },
  "mariadb": {
    "ENGINE": "django.db.backends.mysql",
    "NAME": os.environ.get("DB_NAME", "dj_queue_test"),
    "USER": os.environ.get("DB_USER", DEFAULT_DB_USER),
    "PASSWORD": os.environ.get("DB_PASSWORD", DEFAULT_DB_PASSWORD),
    "HOST": os.environ.get("DB_HOST", "127.0.0.1"),
    "PORT": os.environ.get("DB_PORT", "17306"),
  },
}

if DB_BACKEND not in _DATABASES:
  print(f"unknown DB_BACKEND: {DB_BACKEND!r} (expected: sqlite, postgres, mysql, mariadb)")
  sys.exit(1)

settings.configure(
  SECRET_KEY="examples",
  DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
  USE_TZ=True,
  DATABASES={"default": _DATABASES[DB_BACKEND]},
  INSTALLED_APPS=["dj_queue"],
  DATABASE_ROUTERS=["dj_queue.routers.DjQueueRouter"],
  TASKS={
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {},
    },
  },
)

django.setup()

from django.core.management import call_command  # noqa: E402

call_command("migrate", "--run-syncdb", verbosity=0)

# clear any leftover state so each example starts clean
from dj_queue.models import Job, Pause, RecurringTask, Semaphore  # noqa: E402

for model in (Job, Pause, RecurringTask, Semaphore):
  model.objects.all().delete()

connections.close_all()
