from datetime import timedelta
from uuid import uuid4

import pytest
from django.db import connection
from django.db.migrations.executor import MigrationExecutor
from django.utils import timezone

from dj_queue.models import Process, RecurringExecution, Semaphore
from tests.factories import make_job


def columns(table):
  with connection.cursor() as cursor:
    return {info.name for info in connection.introspection.get_table_description(cursor, table)}


@pytest.mark.django_db
def test_migrate_drops_bridge_columns():
  assert "active_count" not in columns("dj_queue_semaphores")
  assert "supervisor_identity" not in columns("dj_queue_processes")


@pytest.mark.django_db(transaction=True)
def test_migrate_drops_bridge_columns_by_table_rebuild(monkeypatch):
  if not hasattr(connection.features, "can_alter_table_drop_column"):
    pytest.skip("only Django 6.0 on SQLite before 3.35.5 rebuilds the table")
  MigrationExecutor(connection).migrate(
    [("dj_queue", "0022_resolve_legacy_recurring_reservations")]
  )
  assert "active_count" in columns("dj_queue_semaphores")
  assert "supervisor_identity" in columns("dj_queue_processes")
  Semaphore.objects.create(key="kept", value=1, limit=2, expires_at=timezone.now())
  Process.objects.create(
    kind="Worker", pid=1, hostname="host", name="worker-1", last_heartbeat_at=timezone.now()
  )
  monkeypatch.setattr(connection.features, "can_alter_table_drop_column", False)

  executor = MigrationExecutor(connection)
  executor.migrate(executor.loader.graph.leaf_nodes("dj_queue"))

  assert "active_count" not in columns("dj_queue_semaphores")
  assert "supervisor_identity" not in columns("dj_queue_processes")
  assert Semaphore.objects.values_list("key", "value", "limit").get() == ("kept", 1, 2)
  assert Process.objects.values_list("kind", "name", "supervisor").get() == (
    "Worker",
    "worker-1",
    None,
  )


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
