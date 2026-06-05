import os

import pytest
from django.db import connections

from dj_queue import db as dj_queue_db

DB_BACKEND = os.environ.get("DB_BACKEND", "sqlite")


@pytest.fixture(autouse=True)
def _reset_capability_cache():
  dj_queue_db._CAPABILITIES_CACHE.clear()
  yield
  dj_queue_db._CAPABILITIES_CACHE.clear()


def pytest_collection_modifyitems(items):
  for item in items:
    for marker in ("postgres", "mysql", "mariadb"):
      if marker in item.keywords and DB_BACKEND != marker:
        item.add_marker(pytest.mark.skip(reason=f"requires DB_BACKEND={marker}"))


def _reset_connections():
  dj_queue_db._CAPABILITIES_CACHE.clear()
  aliases = list(connections)
  connections.close_all()
  for alias in aliases:
    if hasattr(connections._connections, alias):
      delattr(connections._connections, alias)
  connections.__dict__.pop("settings", None)
  connections._settings = None


@pytest.fixture
def queue_test_settings(settings):
  original_databases = settings.DATABASES
  original_tasks = settings.TASKS
  databases_changed = False
  tasks_changed = False

  def apply(*, databases=None, tasks=None):
    nonlocal databases_changed, tasks_changed

    if databases is not None:
      settings.DATABASES = databases
      databases_changed = True
    if tasks is not None:
      settings.TASKS = tasks
      tasks_changed = True
    _reset_connections()

  try:
    yield apply
  finally:
    if databases_changed:
      settings.DATABASES = original_databases
    if tasks_changed:
      settings.TASKS = original_tasks
    if databases_changed or tasks_changed:
      _reset_connections()
