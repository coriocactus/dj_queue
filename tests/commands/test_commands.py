from datetime import timedelta
from io import StringIO

import pytest
from django.core.management import call_command
from django.utils import timezone

from dj_queue.config import load_backend_config
from dj_queue.models import FailedExecution, Job, Process, RecurringExecution


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
  return Process.objects.create(
    backend_alias=overrides.pop("backend_alias", "default"),
    kind=overrides.pop("kind", "Worker"),
    pid=overrides.pop("pid", 12345),
    hostname=overrides.pop("hostname", "localhost"),
    name=overrides.pop("name", "worker-1"),
    metadata=overrides.pop("metadata", {}),
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


def test_dj_queue_prune_command_deletes_old_finished_jobs(capsys):
  old_job = make_finished_job(finished_at=timezone.now() - timedelta(days=3))
  recent_job = make_finished_job(finished_at=timezone.now() - timedelta(hours=1))

  call_command("dj_queue_prune", "--older-than", "86400")

  captured = capsys.readouterr()
  assert "deleted 1 finished jobs" in captured.out
  assert Job.objects.filter(pk=old_job.pk).exists() is False
  assert Job.objects.filter(pk=recent_job.pk).exists() is True


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

  Process.objects.all().update(last_heartbeat_at=timezone.now() - timedelta(hours=1))

  with pytest.raises(SystemExit, match="1"):
    call_command("dj_queue_health")


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


def test_procline_is_best_effort_when_dependency_missing(monkeypatch):
  imported = []

  class FakeImportlib:
    @staticmethod
    def import_module(name):
      imported.append(name)
      raise ModuleNotFoundError(name)

  import dj_queue.runtime.procline as procline

  monkeypatch.setattr(procline, "importlib", FakeImportlib)

  assert procline.set_process_title("dj-queue worker-1") is False
  assert imported == ["setproctitle"]
