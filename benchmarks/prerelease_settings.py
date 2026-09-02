import importlib
import os

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
SECRET_KEY = "prerelease-load"
USE_TZ = True

BACKEND = os.environ["PRERELEASE_BACKEND"]
MYSQL_FAMILY = {"mysql", "mariadb"}

if BACKEND in MYSQL_FAMILY:
  pymysql = importlib.import_module("pymysql")
  pymysql.version_info = (2, 2, 1, "final", 0)
  pymysql.__version__ = "2.2.1"
  pymysql.install_as_MySQLdb()

DATABASES = {
  "default": {
    "sqlite": {
      "ENGINE": "django.db.backends.sqlite3",
      "NAME": os.environ["PRERELEASE_DB_NAME"],
    },
    "postgres": {
      "ENGINE": "django.db.backends.postgresql",
      "NAME": os.environ["PRERELEASE_DB_NAME"],
      "USER": os.environ.get("PRERELEASE_DB_USER", "dj_queue"),
      "PASSWORD": os.environ.get("PRERELEASE_DB_PASSWORD", "dj_queue"),
      "HOST": os.environ.get("PRERELEASE_DB_HOST", "127.0.0.1"),
      "PORT": os.environ.get("PRERELEASE_DB_PORT", "5432"),
    },
    "mysql": {
      "ENGINE": "django.db.backends.mysql",
      "NAME": os.environ["PRERELEASE_DB_NAME"],
      "USER": os.environ.get("PRERELEASE_DB_USER", "root"),
      "PASSWORD": os.environ.get("PRERELEASE_DB_PASSWORD", "root"),
      "HOST": os.environ.get("PRERELEASE_DB_HOST", "127.0.0.1"),
      "PORT": os.environ.get("PRERELEASE_DB_PORT", "3306"),
    },
    "mariadb": {
      "ENGINE": "django.db.backends.mysql",
      "NAME": os.environ["PRERELEASE_DB_NAME"],
      "USER": os.environ.get("PRERELEASE_DB_USER", "root"),
      "PASSWORD": os.environ.get("PRERELEASE_DB_PASSWORD", "root"),
      "HOST": os.environ.get("PRERELEASE_DB_HOST", "127.0.0.1"),
      "PORT": os.environ.get("PRERELEASE_DB_PORT", "3306"),
    },
  }[BACKEND]
}

INSTALLED_APPS = [
  "django.contrib.contenttypes",
  "dj_queue",
]
MIDDLEWARE = []
TEMPLATES = []
DATABASE_ROUTERS = ["dj_queue.routers.DjQueueRouter"]

WORKER_COUNT = int(os.environ.get("PRERELEASE_WORKERS", "2"))
WORKER_THREADS = int(os.environ.get("PRERELEASE_THREADS", "4"))
RUNTIME_LABEL = os.environ.get("PRERELEASE_RUNTIME_LABEL", "unknown")

TASKS = {
  "default": {
    "BACKEND": "dj_queue.backend.DjQueueBackend",
    "QUEUES": [],
    "OPTIONS": {
      "mode": "async",
      "workers": [
        {
          "queues": "*",
          "threads": WORKER_THREADS,
          "processes": 1,
          "polling_interval": 0.01,
        }
        for _index in range(WORKER_COUNT)
      ],
      "dispatchers": [
        {
          "batch_size": 500,
          "polling_interval": 0.05,
          "concurrency_maintenance": True,
          "concurrency_maintenance_interval": 5,
        }
      ],
      "scheduler": {
        "dynamic_tasks_enabled": True,
        "polling_interval": 0.05,
      },
      "recurring": {},
      "process_heartbeat_interval": 1,
      "process_alive_threshold": 10,
      "shutdown_timeout": 60,
      "preserve_finished_jobs": True,
      "clear_finished_jobs_after": None,
      "clear_failed_jobs_after": None,
      "clear_recurring_executions_after": None,
      "default_concurrency_duration": 60,
      "listen_notify": False,
      "silence_polling": True,
    },
  }
}
