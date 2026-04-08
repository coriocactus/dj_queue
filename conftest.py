import os

import pytest
from django.db import connections

DB_BACKEND = os.environ.get("DB_BACKEND", "sqlite")


def pytest_collection_modifyitems(items):
  for item in items:
    for marker in ("postgres", "mysql", "mariadb"):
      if marker in item.keywords and DB_BACKEND != marker:
        item.add_marker(pytest.mark.skip(reason=f"requires DB_BACKEND={marker}"))


def _reset_connections():
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
