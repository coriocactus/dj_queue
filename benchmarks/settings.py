import importlib
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
SECRET_KEY = "benchmark"
USE_TZ = True
ROOT_URLCONF = "benchmarks.urls"
STATIC_URL = "/static/"

BENCHMARK_BACKEND = os.environ.get("BENCHMARK_BACKEND", "sqlite")
MYSQL_FAMILY = {"mysql", "mariadb"}


def _positive_int_env(name, default):
  value = os.environ.get(name)
  if value in (None, ""):
    return default
  number = int(value)
  if number <= 0:
    raise ValueError(f"{name} must be a positive integer")
  return number


def _bool_env(name, default):
  value = os.environ.get(name)
  if value in (None, ""):
    return default
  if value.lower() in {"1", "true", "yes", "on"}:
    return True
  if value.lower() in {"0", "false", "no", "off"}:
    return False
  raise ValueError(f"{name} must be a boolean value")


if BENCHMARK_BACKEND in MYSQL_FAMILY:
  pymysql = importlib.import_module("pymysql")
  pymysql.version_info = (2, 2, 1, "final", 0)
  pymysql.__version__ = "2.2.1"
  pymysql.install_as_MySQLdb()

_SQLITE_NAME = os.environ.get(
  "BENCHMARK_DB_NAME",
  str(BASE_DIR / "benchmark-results" / "dj_queue_benchmark.sqlite3"),
)
_DATABASES = {
  "sqlite": {
    "ENGINE": "django.db.backends.sqlite3",
    "NAME": _SQLITE_NAME,
  },
  "postgres": {
    "ENGINE": "django.db.backends.postgresql",
    "NAME": os.environ.get("BENCHMARK_DB_NAME", "dj_queue_benchmark"),
    "USER": os.environ.get("BENCHMARK_DB_USER", "dj_queue"),
    "PASSWORD": os.environ.get("BENCHMARK_DB_PASSWORD", "dj_queue"),
    "HOST": os.environ.get("BENCHMARK_DB_HOST", "127.0.0.1"),
    "PORT": os.environ.get("BENCHMARK_DB_PORT", "17432"),
  },
  "mysql": {
    "ENGINE": "django.db.backends.mysql",
    "NAME": os.environ.get("BENCHMARK_DB_NAME", "dj_queue_benchmark"),
    "USER": os.environ.get("BENCHMARK_DB_USER", "root"),
    "PASSWORD": os.environ.get("BENCHMARK_DB_PASSWORD", "root"),
    "HOST": os.environ.get("BENCHMARK_DB_HOST", "127.0.0.1"),
    "PORT": os.environ.get("BENCHMARK_DB_PORT", "17312"),
  },
  "mariadb": {
    "ENGINE": "django.db.backends.mysql",
    "NAME": os.environ.get("BENCHMARK_DB_NAME", "dj_queue_benchmark"),
    "USER": os.environ.get("BENCHMARK_DB_USER", "root"),
    "PASSWORD": os.environ.get("BENCHMARK_DB_PASSWORD", "root"),
    "HOST": os.environ.get("BENCHMARK_DB_HOST", "127.0.0.1"),
    "PORT": os.environ.get("BENCHMARK_DB_PORT", "17306"),
  },
}

DATABASES = {"default": _DATABASES[BENCHMARK_BACKEND]}
DEFAULT_WORKER_COUNT = 1 if BENCHMARK_BACKEND == "sqlite" else 4
DEFAULT_WORKER_THREADS = 1 if BENCHMARK_BACKEND == "sqlite" else 8
WORKER_COUNT = _positive_int_env("BENCHMARK_WORKER_COUNT", DEFAULT_WORKER_COUNT)
WORKER_THREADS = _positive_int_env("BENCHMARK_WORKER_THREADS", DEFAULT_WORKER_THREADS)
PRESERVE_FINISHED_JOBS = _bool_env("BENCHMARK_PRESERVE_FINISHED_JOBS", True)
INSTALLED_APPS = [
  "django.contrib.contenttypes",
  "dj_queue",
]
MIDDLEWARE = []
TEMPLATES = []
DATABASE_ROUTERS = ["dj_queue.routers.DjQueueRouter"]

TASKS = {
  "default": {
    "BACKEND": "dj_queue.backend.DjQueueBackend",
    "QUEUES": [],
    "OPTIONS": {
      "mode": "async",
      "workers": [
        {"queues": "*", "threads": WORKER_THREADS, "processes": 1, "polling_interval": 0.01}
        for _index in range(WORKER_COUNT)
      ],
      "dispatchers": [],
      "scheduler": None,
      "recurring": {},
      "process_heartbeat_interval": 0,
      "process_alive_threshold": 60,
      "shutdown_timeout": 60,
      "preserve_finished_jobs": PRESERVE_FINISHED_JOBS,
      "clear_finished_jobs_after": None,
      "clear_failed_jobs_after": None,
      "clear_recurring_executions_after": None,
      "listen_notify": False,
      "silence_polling": True,
    },
  }
}
