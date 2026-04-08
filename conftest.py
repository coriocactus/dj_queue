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

  def apply(*, databases=None, tasks=None):
    if databases is not None:
      settings.DATABASES = databases
    if tasks is not None:
      settings.TASKS = tasks
    _reset_connections()

  try:
    yield apply
  finally:
    settings.DATABASES = original_databases
    settings.TASKS = original_tasks
    _reset_connections()
