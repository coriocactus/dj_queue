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
WORKER_COUNT = 1 if BENCHMARK_BACKEND == "sqlite" else 4
WORKER_THREADS = 1 if BENCHMARK_BACKEND == "sqlite" else 8
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
      "preserve_finished_jobs": True,
      "clear_finished_jobs_after": None,
      "clear_failed_jobs_after": None,
      "clear_recurring_executions_after": None,
      "listen_notify": False,
      "silence_polling": True,
    },
  }
}
