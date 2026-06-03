from importlib import import_module

from dj_queue.db import database_capabilities


def backend_sql(alias):
  backend_family = database_capabilities(alias).backend_family
  if backend_family == "postgresql":
    return import_module("dj_queue.sql.postgres")
  if backend_family in {"mysql", "mariadb"}:
    return import_module("dj_queue.sql.mysql")
  return import_module("dj_queue.sql.sqlite")
