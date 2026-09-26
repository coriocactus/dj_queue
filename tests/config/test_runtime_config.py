from datetime import timedelta

import pytest
from django.core.management import call_command
from django.test import override_settings
from django.utils import timezone

from dj_queue.config import load_backend_config
from dj_queue.management.commands.dj_queue import build_supervisor
from dj_queue.models import Job, Pause, Process, ReadyExecution, ScheduledExecution, Semaphore
from dj_queue.operations.jobs import enqueue_job
from dj_queue.operations.queues import pause_queue, resume_queue
from tests.config.test_queue_db_runtime import (
  _dj_queue_tables,
  _queue_tasks,
  _sqlite_databases,
  wait_until,
)
from tests.tasks import limited

pytestmark = pytest.mark.filterwarnings(
  r"ignore:Overriding setting DATABASES can lead to unexpected behavior\.:UserWarning"
)


def test_cli_configuration_reaches_runtime_threads_and_operations(
  tmp_path, django_db_blocker, queue_test_settings
):
  queue_test_settings(
    databases=_sqlite_databases(tmp_path), tasks=_queue_tasks(database_alias="default")
  )
  path = tmp_path / "queue.toml"
  path.write_text("""mode = "async"
database_alias = "queue"
preserve_finished_jobs = false
listen_notify = false
process_heartbeat_interval = 0.02
[[workers]]
threads = 1
polling_interval = 0.01
[[dispatchers]]
polling_interval = 0.01
""")
  supervisor = build_supervisor(backend_alias="default", cli_overrides={"config": str(path)})
  supervisor.standalone = False

  with django_db_blocker.unblock():
    with override_settings(DATABASE_ROUTERS=[]):
      call_command("migrate", "dj_queue", database="queue", verbosity=0)
    now = timezone.now()
    for value in ("first", "second"):
      job = Job.objects.using("queue").create(
        task_path=limited.module_path,
        backend_alias="default",
        queue_name="default",
        priority=0,
        payload={"args": [1], "kwargs": {"value": value}},
        scheduled_at=now,
        concurrency_key="account:1",
        concurrency_limit=1,
        concurrency_duration=60,
        concurrency_on_conflict="block",
      )
      ScheduledExecution.objects.using("queue").create(
        job=job,
        backend_alias="default",
        queue_name="default",
        priority=0,
        scheduled_at=now,
      )
    try:
      supervisor.start()
      wait_until(lambda: not Job.objects.using("queue").exists(), timeout=5)
      wait_until(
        lambda: (
          Process.objects.using("queue")
          .filter(last_heartbeat_at__gt=now + timedelta(seconds=0.02))
          .exists()
        )
      )
      assert Semaphore.objects.using("queue").get(key="account:1").value == 1
      assert _dj_queue_tables("default") == set()
    finally:
      supervisor.stop()
    assert not Process.objects.using("queue").exists()


def test_explicit_config_does_not_change_other_callers(
  tmp_path, django_db_blocker, queue_test_settings
):
  queue_test_settings(
    databases=_sqlite_databases(tmp_path), tasks=_queue_tasks(database_alias="default")
  )
  path = tmp_path / "queue.toml"
  path.write_text('database_alias = "queue"\n')
  config = load_backend_config(cli_overrides={"config": str(path)})
  with django_db_blocker.unblock():
    with override_settings(DATABASE_ROUTERS=[]):
      for alias in ("default", "queue"):
        call_command("migrate", "dj_queue", database=alias, verbosity=0)
    explicit = enqueue_job(limited, [1], {}, config=config)
    ordinary = enqueue_job(limited, [2], {})
    assert ReadyExecution.objects.using("queue").filter(job_id=explicit.pk).exists()
    assert ReadyExecution.objects.using("default").filter(job_id=ordinary.pk).exists()
    assert load_backend_config().database_alias == "default"

    pause_queue("default")
    pause_queue("default", config=config)
    assert resume_queue("default", config=config) is True
    assert not Pause.objects.using("queue").exists()
    assert Pause.objects.using("default").exists()
