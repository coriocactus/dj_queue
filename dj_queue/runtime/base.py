from contextlib import contextmanager

from django.db import close_old_connections


@contextmanager
def app_executor():
  close_old_connections()
  try:
    yield
  finally:
    close_old_connections()
