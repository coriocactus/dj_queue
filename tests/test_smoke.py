import importlib

from django.apps import apps


def test_app_installed():
  assert apps.is_installed("dj_queue")


def test_phase_zero_modules_import():
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
    assert importlib.import_module(module_name)
