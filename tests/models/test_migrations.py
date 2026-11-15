from datetime import timedelta
from uuid import uuid4

import pytest
from django.db import connection
from django.db.migrations.executor import MigrationExecutor
from django.utils import timezone

from dj_queue.models import RecurringExecution
from tests.factories import make_job


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


@pytest.mark.django_db(transaction=True)
def test_migrate_resolves_legacy_recurring_reservations():
  MigrationExecutor(connection).migrate([("dj_queue", "0021_remove_bridge_fields_from_state")])
  run_at = timezone.now().replace(microsecond=0)
  job = make_job()
  intended_job_id = uuid4()
  with_job = RecurringExecution.objects.create(task_key="legacy", run_at=run_at, job=job)
  RecurringExecution.objects.create(task_key="legacy", run_at=run_at - timedelta(minutes=1))
  reserved = RecurringExecution.objects.create(
    task_key="legacy", run_at=run_at - timedelta(minutes=2), intended_job_id=intended_job_id
  )

  executor = MigrationExecutor(connection)
  executor.migrate(executor.loader.graph.leaf_nodes("dj_queue"))

  assert set(RecurringExecution.objects.values_list("id", "intended_job_id", "job_id")) == {
    (with_job.id, job.id, job.id),
    (reserved.id, intended_job_id, None),
  }
