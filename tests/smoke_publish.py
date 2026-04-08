import importlib
import pkgutil

import django
import dj_queue
from django.apps import apps
from django.conf import settings
from django.core.management import get_commands


if not settings.configured:
  settings.configure(
    SECRET_KEY="smoke",
    USE_TZ=True,
    DATABASES={"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}},
    INSTALLED_APPS=[
      "django.contrib.admin",
      "django.contrib.auth",
      "django.contrib.contenttypes",
      "django.contrib.sessions",
      "django.contrib.messages",
      "dj_queue",
    ],
    TASKS={
      "default": {
        "BACKEND": "dj_queue.backend.DjQueueBackend",
        "QUEUES": [],
        "OPTIONS": {},
      }
    },
  )

django.setup()

for module_name in sorted(
  ["dj_queue", *[module.name for module in pkgutil.walk_packages(dj_queue.__path__, "dj_queue.")]]
):
  importlib.import_module(module_name)

assert apps.is_installed("dj_queue")
assert "dj_queue" in get_commands()
