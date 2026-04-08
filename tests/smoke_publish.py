import importlib

import django
from django.apps import apps
from django.conf import settings
from django.core.management import get_commands


if not settings.configured:
  settings.configure(
    SECRET_KEY="smoke",
    USE_TZ=True,
    DATABASES={"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}},
    INSTALLED_APPS=["dj_queue"],
    TASKS={
      "default": {
        "BACKEND": "dj_queue.backend.DjQueueBackend",
        "QUEUES": [],
        "OPTIONS": {},
      }
    },
  )

django.setup()

for module_name in (
  "dj_queue",
  "dj_queue.api",
  "dj_queue.backend",
  "dj_queue.config",
  "dj_queue.db",
  "dj_queue.models",
  "dj_queue.operations",
  "dj_queue.runtime",
  "dj_queue.contrib",
):
  importlib.import_module(module_name)

assert apps.is_installed("dj_queue")
assert "dj_queue" in get_commands()
