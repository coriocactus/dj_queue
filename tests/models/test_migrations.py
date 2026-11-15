import pytest
from django.db import connection


@pytest.mark.django_db
@pytest.mark.parametrize(
  ("table", "column"),
  [
    ("dj_queue_semaphores", "active_count"),
    ("dj_queue_processes", "supervisor_identity"),
  ],
)
def test_migrate_keeps_columns_that_n_minus_1_processes_use(table, column):
  with connection.cursor() as cursor:
    description = connection.introspection.get_table_description(cursor, table)

  assert column in {info.name for info in description}
