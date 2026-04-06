import os

DB_BACKEND = os.environ.get("DB_BACKEND", "sqlite")

_DATABASES = {
    "sqlite": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    },
    "postgres": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.environ.get("DB_NAME", "dj_queue_test"),
        "USER": os.environ.get("DB_USER", "dj_queue"),
        "PASSWORD": os.environ.get("DB_PASSWORD", "dj_queue"),
        "HOST": os.environ.get("DB_HOST", "127.0.0.1"),
        "PORT": os.environ.get("DB_PORT", "5432"),
    },
    "mysql": {
        "ENGINE": "django.db.backends.mysql",
        "NAME": os.environ.get("DB_NAME", "dj_queue_test"),
        "USER": os.environ.get("DB_USER", "dj_queue"),
        "PASSWORD": os.environ.get("DB_PASSWORD", "dj_queue"),
        "HOST": os.environ.get("DB_HOST", "127.0.0.1"),
        "PORT": os.environ.get("DB_PORT", "3306"),
    },
    "mariadb": {
        "ENGINE": "django.db.backends.mysql",
        "NAME": os.environ.get("DB_NAME", "dj_queue_test"),
        "USER": os.environ.get("DB_USER", "dj_queue"),
        "PASSWORD": os.environ.get("DB_PASSWORD", "dj_queue"),
        "HOST": os.environ.get("DB_HOST", "127.0.0.1"),
        "PORT": os.environ.get("DB_PORT", "3307"),
    },
}

DATABASES = {"default": _DATABASES[DB_BACKEND]}

INSTALLED_APPS = [
    "dj_queue",
]

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

USE_TZ = True
