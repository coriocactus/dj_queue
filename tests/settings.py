import importlib
import os

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
USE_TZ = True

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

DATABASES = {"default": _DATABASES[DB_BACKEND]}
INSTALLED_APPS = ["dj_queue"]
DATABASE_ROUTERS = ["dj_queue.routers.DjQueueRouter"]

TASKS = {
  "default": {
    "BACKEND": "dj_queue.backend.DjQueueBackend",
    "QUEUES": [],
    "OPTIONS": {},
  },
}
