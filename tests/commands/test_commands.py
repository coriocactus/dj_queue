from datetime import timedelta
from io import StringIO
from types import SimpleNamespace
from uuid import uuid4

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import connections
from django.utils import timezone

from dj_queue.config import load_backend_config
from dj_queue.models import (
  ClaimedExecution,
  FailedExecution,
  Job,
  Process,
  ReadyExecution,
  RecurringExecution,
  Semaphore,
)
from dj_queue.runtime.base import DJ_QUEUE_VERSION, ROLLOUT_PROTOCOL_VERSION

pytestmark = pytest.mark.django_db(transaction=True)


def make_finished_job(**overrides):
  finished_at = overrides.pop("finished_at", timezone.now() - timedelta(days=2))
  return Job.objects.create(
    task_path=overrides.pop("task_path", "tests.tasks.echo"),
    queue_name=overrides.pop("queue_name", "default"),
    priority=overrides.pop("priority", 0),
    payload=overrides.pop("payload", {"args": [], "kwargs": {}}),
    backend_alias=overrides.pop("backend_alias", "default"),
    scheduled_at=overrides.pop("scheduled_at", None),
    concurrency_key=overrides.pop("concurrency_key", None),
    finished_at=finished_at,
    return_value=overrides.pop("return_value", "ok"),
    **overrides,
  )


def make_process(**overrides):
  metadata = {
    "dj_queue_version": DJ_QUEUE_VERSION,
    "rollout_protocol": ROLLOUT_PROTOCOL_VERSION,
  }
  return Process.objects.create(
    backend_alias=overrides.pop("backend_alias", "default"),
    kind=overrides.pop("kind", "Worker"),
    pid=overrides.pop("pid", 12345),
    hostname=overrides.pop("hostname", "localhost"),
    name=overrides.pop("name", "worker-1"),
    metadata=overrides.pop("metadata", metadata),
    supervisor=overrides.pop("supervisor", None),
    last_heartbeat_at=overrides.pop("last_heartbeat_at", timezone.now()),
    **overrides,
  )


def test_dj_queue_command_starts_default_runtime(monkeypatch):
  started = []

  class StubSupervisor:
    def run(self):
      started.append("run")

  def build_supervisor(*, backend_alias, cli_overrides):
    started.append((backend_alias, cli_overrides))
    return StubSupervisor()

  monkeypatch.setattr("dj_queue.management.commands.dj_queue.build_supervisor", build_supervisor)

  call_command("dj_queue")

  assert started == [
    (
      "default",
      {
        "config": None,
        "mode": None,
        "only_work": False,
        "only_dispatch": False,
        "skip_recurring": False,
      },
    ),
    "run",
  ]


def test_dj_queue_command_mode_async(monkeypatch):
  seen = []

  class StubSupervisor:
    def run(self):
      seen.append("run")

  def build_supervisor(*, backend_alias, cli_overrides):
    seen.append((backend_alias, cli_overrides))
    return StubSupervisor()

  monkeypatch.setattr("dj_queue.management.commands.dj_queue.build_supervisor", build_supervisor)

  call_command("dj_queue", "--mode", "async")

  assert seen == [
    (
      "default",
      {
        "config": None,
        "mode": "async",
        "only_work": False,
        "only_dispatch": False,
        "skip_recurring": False,
      },
    ),
    "run",
  ]


def test_dj_queue_command_waits_for_migrations_before_runtime(monkeypatch):
  events = []

  class StubSupervisor:
    def run(self):
      events.append("run")

  def wait_for_migrations(database_alias, *, timeout, stdout):
    events.append(("wait", database_alias, timeout))

  def build_supervisor(*, backend_alias, cli_overrides):
    events.append(("build", backend_alias, cli_overrides))
    return StubSupervisor()

  monkeypatch.setattr(
    "dj_queue.management.commands.dj_queue.wait_for_dj_queue_migrations",
    wait_for_migrations,
  )
  monkeypatch.setattr("dj_queue.management.commands.dj_queue.build_supervisor", build_supervisor)

  call_command("dj_queue", "--migration-wait-timeout", "2.5")

  assert events == [
    ("wait", "default", 2.5),
    (
      "build",
      "default",
      {
        "config": None,
        "mode": None,
        "only_work": False,
        "only_dispatch": False,
        "skip_recurring": False,
      },
    ),
    "run",
  ]


def test_wait_for_dj_queue_migrations_rechecks_until_applied(monkeypatch):
  from dj_queue.management.commands import dj_queue as command

  pending_migration = SimpleNamespace(app_label="dj_queue", name="0010_pending")
  plans = [[(pending_migration, False)], []]
  sleeps = []
  output = StringIO()

  def pending_migrations(database_alias):
    assert database_alias == "default"
    return plans.pop(0)

  monkeypatch.setattr(command, "pending_dj_queue_migrations", pending_migrations)
  monkeypatch.setattr(command.time, "sleep", sleeps.append)

  command.wait_for_dj_queue_migrations("default", timeout=5, interval=0.25, stdout=output)

  assert sleeps == [0.25]
  assert "waiting for dj_queue migrations" in output.getvalue()


def test_wait_for_dj_queue_migrations_times_out(monkeypatch):
  from dj_queue.management.commands import dj_queue as command

  pending_migration = SimpleNamespace(app_label="dj_queue", name="0010_pending")
  monkeypatch.setattr(
    command,
    "pending_dj_queue_migrations",
    lambda _database_alias: [(pending_migration, False)],
  )

  with pytest.raises(CommandError, match="dj_queue migrations are pending"):
    command.wait_for_dj_queue_migrations("default", timeout=0, interval=0)


def test_only_work_mode_starts_no_dispatcher_or_scheduler(monkeypatch):
  captured = []

  class StubSupervisor:
    def run(self):
      return None

  def build_supervisor(*, backend_alias, cli_overrides):
    captured.append(load_backend_config(backend_alias, cli_overrides=cli_overrides))
    return StubSupervisor()

  monkeypatch.setattr("dj_queue.management.commands.dj_queue.build_supervisor", build_supervisor)

  call_command("dj_queue", "--only-work")

  config = captured[0]
  assert config.only_work is True
  assert config.workers
  assert config.dispatchers == ()
  assert config.scheduler is None


def test_only_dispatch_mode_starts_no_worker_or_scheduler(monkeypatch):
  captured = []

  class StubSupervisor:
    def run(self):
      return None

  def build_supervisor(*, backend_alias, cli_overrides):
    captured.append(load_backend_config(backend_alias, cli_overrides=cli_overrides))
    return StubSupervisor()

  monkeypatch.setattr("dj_queue.management.commands.dj_queue.build_supervisor", build_supervisor)

  call_command("dj_queue", "--only-dispatch")

  config = captured[0]
  assert config.only_dispatch is True
  assert config.workers == ()
  assert config.dispatchers
  assert config.scheduler is None


def test_skip_recurring_flag_suppresses_scheduler_even_when_configured(monkeypatch, settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {
        "recurring": {
          "static-task": {
            "task_path": "tests.tasks.echo",
            "schedule": "* * * * *",
          }
        }
      },
    }
  }
  captured = []

  class StubSupervisor:
    def run(self):
      return None

  def build_supervisor(*, backend_alias, cli_overrides):
    captured.append(load_backend_config(backend_alias, cli_overrides=cli_overrides))
    return StubSupervisor()

  monkeypatch.setattr("dj_queue.management.commands.dj_queue.build_supervisor", build_supervisor)

  call_command("dj_queue", "--skip-recurring")

  config = captured[0]
  assert config.skip_recurring is True
  assert config.scheduler is None


def test_dj_queue_command_uses_selected_backend_toml_overlay(monkeypatch, settings, tmp_path):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {
        "mode": "fork",
      },
    },
    "secondary": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {
        "mode": "fork",
      },
    },
  }
  config_path = tmp_path / "dj_queue.toml"
  config_path.write_text(
    (
      "[backends.default]\n"
      'mode = "async"\n'
      "[backends.secondary]\n"
      'mode = "fork"\n'
      'database_alias = "queue_secondary"'
    ),
    encoding="utf-8",
  )
  captured = []

  class StubSupervisor:
    def run(self):
      return None

  def build_supervisor(*, backend_alias, cli_overrides):
    captured.append(load_backend_config(backend_alias, cli_overrides=cli_overrides))
    return StubSupervisor()

  monkeypatch.setattr(
    "dj_queue.management.commands.dj_queue.wait_for_dj_queue_migrations",
    lambda _database_alias, *, timeout, stdout: None,
  )
  monkeypatch.setattr("dj_queue.management.commands.dj_queue.build_supervisor", build_supervisor)

  call_command("dj_queue", "--backend", "secondary", "--config", str(config_path))

  config = captured[0]
  assert config.backend_alias == "secondary"
  assert config.mode == "fork"
  assert config.database_alias == "queue_secondary"


def test_dj_queue_command_rejects_other_backend_alias(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "other.backend.Backend",
      "QUEUES": [],
      "OPTIONS": {},
    }
  }

  with pytest.raises(CommandError, match="not configured for DjQueueBackend"):
    call_command("dj_queue")


def test_dj_queue_prune_command_deletes_old_finished_jobs(capsys):
  old_job = make_finished_job(finished_at=timezone.now() - timedelta(days=3))
  recent_job = make_finished_job(finished_at=timezone.now() - timedelta(hours=1))

  call_command("dj_queue_prune", "--older-than", "86400")

  captured = capsys.readouterr()
  assert "deleted 1 finished jobs" in captured.out
  assert Job.objects.filter(pk=old_job.pk).exists() is False
  assert Job.objects.filter(pk=recent_job.pk).exists() is True


@pytest.mark.parametrize(
  ("option", "value", "message"),
  (
    ("--older-than", "-1", "--older-than must be non-negative"),
    ("--failed-older-than", "-1", "--failed-older-than must be non-negative"),
    ("--recurring-older-than", "-1", "--recurring-older-than must be non-negative"),
    ("--batch-size", "0", "--batch-size must be positive"),
  ),
)
def test_prune_command_rejects_invalid_limits(option, value, message):
  with pytest.raises(CommandError, match=message):
    call_command("dj_queue_prune", option, value)


def test_prune_command_reports_when_batch_limit_is_reached(capsys):
  make_finished_job()
  make_finished_job()

  call_command("dj_queue_prune", "--older-than", "0", "--batch-size", "1")

  captured = capsys.readouterr()
  assert "deleted 1 finished jobs" in captured.out
  assert "batch limit reached; run dj_queue_prune again to check for more" in captured.out
  assert Job.objects.count() == 1


def test_prune_command_task_path_filter_limits_deletions(capsys):
  kept_job = make_finished_job(task_path="tests.tasks.other")
  pruned_job = make_finished_job(task_path="tests.tasks.echo")

  call_command("dj_queue_prune", "--older-than", "86400", "--task-path", "tests.tasks.echo")

  captured = capsys.readouterr()
  assert "deleted 1 finished jobs" in captured.out
  assert Job.objects.filter(pk=pruned_job.pk).exists() is False
  assert Job.objects.filter(pk=kept_job.pk).exists() is True


def test_prune_command_default_older_than_uses_backend_config(settings, capsys):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {
        "clear_finished_jobs_after": 3600,
      },
    }
  }
  old_job = make_finished_job(finished_at=timezone.now() - timedelta(hours=2))
  recent_job = make_finished_job(finished_at=timezone.now() - timedelta(minutes=30))

  call_command("dj_queue_prune")

  captured = capsys.readouterr()
  assert "deleted 1 finished jobs" in captured.out
  assert Job.objects.filter(pk=old_job.pk).exists() is False
  assert Job.objects.filter(pk=recent_job.pk).exists() is True


def test_prune_command_can_delete_failed_and_recurring_rows(capsys):
  old_failed_job = Job.objects.create(
    task_path="tests.tasks.echo",
    queue_name="default",
    priority=0,
    payload={"args": [], "kwargs": {}},
    backend_alias="default",
  )
  recent_failed_job = Job.objects.create(
    task_path="tests.tasks.echo",
    queue_name="default",
    priority=0,
    payload={"args": [], "kwargs": {}},
    backend_alias="default",
  )
  old_failed = FailedExecution.objects.create(
    job=old_failed_job,
    exception_class="ValueError",
    message="old",
    traceback="old",
  )
  recent_failed = FailedExecution.objects.create(
    job=recent_failed_job,
    exception_class="ValueError",
    message="recent",
    traceback="recent",
  )
  FailedExecution.objects.filter(pk=old_failed.pk).update(
    created_at=timezone.now() - timedelta(days=3)
  )
  FailedExecution.objects.filter(pk=recent_failed.pk).update(
    created_at=timezone.now() - timedelta(hours=1)
  )

  old_recurring = RecurringExecution.objects.create(
    backend_alias="default",
    task_key="nightly",
    run_at=timezone.now() - timedelta(days=3),
  )
  recent_recurring = RecurringExecution.objects.create(
    backend_alias="default",
    task_key="nightly",
    run_at=timezone.now() - timedelta(hours=1),
  )

  call_command(
    "dj_queue_prune",
    "--failed-older-than",
    "86400",
    "--recurring-older-than",
    "86400",
  )

  captured = capsys.readouterr()
  assert "deleted 0 finished jobs, 1 failed jobs, and 1 recurring executions" in captured.out
  assert Job.objects.filter(pk=old_failed_job.pk).exists() is False
  assert Job.objects.filter(pk=recent_failed_job.pk).exists() is True
  assert RecurringExecution.objects.filter(pk=old_recurring.pk).exists() is False
  assert RecurringExecution.objects.filter(pk=recent_recurring.pk).exists() is True


def test_prune_command_uses_backend_config_for_failed_and_recurring_windows(settings, capsys):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {
        "clear_finished_jobs_after": None,
        "clear_failed_jobs_after": 3600,
        "clear_recurring_executions_after": 3600,
      },
    }
  }
  old_failed_job = Job.objects.create(
    task_path="tests.tasks.echo",
    queue_name="default",
    priority=0,
    payload={"args": [], "kwargs": {}},
    backend_alias="default",
  )
  old_failed = FailedExecution.objects.create(
    job=old_failed_job,
    exception_class="ValueError",
    message="old",
    traceback="old",
  )
  FailedExecution.objects.filter(pk=old_failed.pk).update(
    created_at=timezone.now() - timedelta(hours=2)
  )
  old_recurring = RecurringExecution.objects.create(
    backend_alias="default",
    task_key="nightly",
    run_at=timezone.now() - timedelta(hours=2),
  )

  call_command("dj_queue_prune")

  captured = capsys.readouterr()
  assert "deleted 0 finished jobs, 1 failed jobs, and 1 recurring executions" in captured.out
  assert Job.objects.filter(pk=old_failed_job.pk).exists() is False
  assert RecurringExecution.objects.filter(pk=old_recurring.pk).exists() is False


def test_dj_queue_health_reports_live_and_dead_states():
  make_process(
    backend_alias="default",
    last_heartbeat_at=timezone.now(),
  )

  healthy_stdout = StringIO()
  call_command("dj_queue_health", stdout=healthy_stdout)
  assert healthy_stdout.getvalue().strip() == "healthy"

  deep_stdout = StringIO()
  call_command("dj_queue_health", "--deep", stdout=deep_stdout)
  assert deep_stdout.getvalue().strip() == "healthy"

  Process.objects.all().update(last_heartbeat_at=timezone.now() - timedelta(hours=1))

  unhealthy_stderr = StringIO()
  with pytest.raises(SystemExit, match="1"):
    call_command("dj_queue_health", stderr=unhealthy_stderr)
  assert unhealthy_stderr.getvalue().strip() == "unhealthy: no live dj_queue processes"


def test_health_command_can_require_one_process_version(capsys):
  make_process(
    metadata={
      "dj_queue_version": "0.12.0",
      "rollout_protocol": ROLLOUT_PROTOCOL_VERSION,
    }
  )

  with pytest.raises(SystemExit, match="1"):
    call_command("dj_queue_health", "--require-version", DJ_QUEUE_VERSION)

  assert (
    f"1 live processes do not run required dj_queue version {DJ_QUEUE_VERSION}"
    in capsys.readouterr().err
  )


def test_health_command_rejects_non_positive_max_age():
  with pytest.raises(CommandError, match="--max-age must be positive"):
    call_command("dj_queue_health", "--max-age", "0")


@pytest.mark.parametrize(
  "command_name",
  ("dj_queue_health", "dj_queue_prune", "dj_queue_postgres_autovacuum"),
)
def test_operational_commands_report_configuration_errors_as_command_errors(
  settings,
  command_name,
):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {"unknown_option": True},
    }
  }

  with pytest.raises(CommandError, match="unknown_option"):
    call_command(command_name)


def test_postgres_autovacuum_command_prints_reviewable_sql(monkeypatch):
  monkeypatch.setattr(
    "dj_queue.management.commands.dj_queue_postgres_autovacuum.database_capabilities",
    lambda alias: SimpleNamespace(backend_family="postgresql"),
  )

  output = StringIO()
  call_command("dj_queue_postgres_autovacuum", stdout=output)

  text = output.getvalue()
  quote_name = connections[load_backend_config("default").database_alias].ops.quote_name
  assert "review before applying" in text
  assert f"ALTER TABLE {quote_name('dj_queue_jobs')} SET (" in text
  assert f"ALTER TABLE {quote_name('dj_queue_ready_executions')} SET (" in text
  assert "autovacuum_vacuum_scale_factor = 0.01" in text
  assert "autovacuum_vacuum_threshold = 50" in text
  assert "dj_queue_processes" not in text


def test_postgres_autovacuum_command_rejects_non_postgres(monkeypatch):
  monkeypatch.setattr(
    "dj_queue.management.commands.dj_queue_postgres_autovacuum.database_capabilities",
    lambda alias: SimpleNamespace(backend_family="sqlite"),
  )

  with pytest.raises(CommandError, match="requires PostgreSQL"):
    call_command("dj_queue_postgres_autovacuum")


def test_health_command_max_age_override_changes_freshness_threshold():
  make_process(last_heartbeat_at=timezone.now() - timedelta(seconds=20))

  with pytest.raises(SystemExit, match="1"):
    call_command("dj_queue_health", "--max-age", "10")


def test_health_command_stays_backend_scoped_on_shared_queue_db(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {"database_alias": "default"},
    },
    "secondary": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {"database_alias": "default"},
    },
  }
  make_process(
    backend_alias="secondary",
    last_heartbeat_at=timezone.now(),
  )

  with pytest.raises(SystemExit, match="1"):
    call_command("dj_queue_health", "--backend", "default")


def test_health_command_deep_reports_invalid_execution_state(capsys):
  make_process(last_heartbeat_at=timezone.now())
  job = make_finished_job()
  ReadyExecution.objects.create(
    job=job,
    backend_alias="default",
    queue_name="default",
    priority=0,
  )

  with pytest.raises(SystemExit, match="1"):
    call_command("dj_queue_health", "--deep")

  assert "jobs have invalid execution state" in capsys.readouterr().err


def test_health_command_deep_reports_state_backend_mismatch(capsys):
  make_process(last_heartbeat_at=timezone.now())
  job = Job.objects.create(
    task_path="tests.tasks.echo",
    queue_name="default",
    priority=0,
    payload={"args": [], "kwargs": {}},
    backend_alias="default",
  )
  ReadyExecution.objects.create(
    job=job,
    backend_alias="secondary",
    queue_name="default",
    priority=0,
  )

  with pytest.raises(SystemExit, match="1"):
    call_command("dj_queue_health", "--deep")

  assert "ready execution rows have mismatched backend ownership" in capsys.readouterr().err


def test_health_command_deep_reports_runtime_integrity_problems(capsys):
  make_process(name="live-worker", last_heartbeat_at=timezone.now())
  stale_process = make_process(
    name="stale-worker",
    last_heartbeat_at=timezone.now() - timedelta(hours=1),
  )
  job = Job.objects.create(
    task_path="tests.tasks.echo",
    queue_name="default",
    priority=0,
    payload={"args": [], "kwargs": {}},
    backend_alias="default",
  )
  ClaimedExecution.objects.create(job=job, process=stale_process)
  RecurringExecution.objects.create(
    backend_alias="default",
    task_key="nightly",
    run_at=timezone.now(),
    job=None,
    intended_job_id=uuid4(),
  )
  Semaphore.objects.create(
    key="account:1",
    value=-1,
    limit=1,
    expires_at=timezone.now() + timedelta(minutes=1),
  )

  with pytest.raises(SystemExit, match="1"):
    call_command("dj_queue_health", "--deep")

  stderr = capsys.readouterr().err
  assert "claimed execution rows have missing or stale processes" in stderr
  assert "recurring execution reservations have no job" in stderr
  assert "semaphores have impossible slot counts" in stderr


def test_procline_is_best_effort_when_dependency_missing(monkeypatch):
  imported = []

  class FakeImportlib:
    @staticmethod
    def import_module(name):
      imported.append(name)
      raise ModuleNotFoundError(name)

  from dj_queue.runtime import procline

  monkeypatch.setattr(procline, "importlib", FakeImportlib)

  assert procline.set_process_title("dj-queue worker-1") is False
  assert imported == ["setproctitle"]
