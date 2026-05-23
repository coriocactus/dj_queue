import math
import time

from django.core.exceptions import ImproperlyConfigured
from django.core.management.base import BaseCommand
from django.core.management.base import CommandError
from django.db import connections
from django.db.migrations.executor import MigrationExecutor

from dj_queue.config import load_backend_config
from dj_queue.runtime.supervisor import AsyncSupervisor, ForkSupervisor

MIGRATION_WAIT_TIMEOUT = 60
MIGRATION_WAIT_INTERVAL = 0.5


def build_supervisor(*, backend_alias, cli_overrides):
  mode = load_backend_config(backend_alias, cli_overrides=cli_overrides).mode
  supervisor_class = AsyncSupervisor if mode == "async" else ForkSupervisor
  return supervisor_class.from_backend_config(
    backend_alias=backend_alias,
    cli_overrides=cli_overrides,
  )


def wait_for_dj_queue_migrations(
  database_alias,
  *,
  timeout=MIGRATION_WAIT_TIMEOUT,
  interval=MIGRATION_WAIT_INTERVAL,
  stdout=None,
):
  if not math.isfinite(timeout) or timeout < 0:
    raise CommandError("dj_queue migration wait timeout must be non-negative")

  deadline = time.monotonic() + timeout
  announced = False
  while True:
    pending = pending_dj_queue_migrations(database_alias)
    if not pending:
      return

    migration_names = ", ".join(
      f"{migration.app_label}.{migration.name}" for migration, _backwards in pending
    )
    if stdout is not None and not announced:
      stdout.write(
        f"waiting for dj_queue migrations on database {database_alias!r}: {migration_names}"
      )
      announced = True

    if time.monotonic() >= deadline:
      raise CommandError(
        f"dj_queue migrations are pending on database {database_alias!r}: {migration_names}; "
        f"run manage.py migrate dj_queue --database {database_alias} before starting dj_queue"
      )
    time.sleep(min(interval, max(deadline - time.monotonic(), 0)))


def pending_dj_queue_migrations(database_alias):
  executor = MigrationExecutor(connections[database_alias])
  targets = [node for node in executor.loader.graph.leaf_nodes() if node[0] == "dj_queue"]
  return [
    (migration, backwards)
    for migration, backwards in executor.migration_plan(targets)
    if migration.app_label == "dj_queue"
  ]


class Command(BaseCommand):
  help = "Start the dj_queue supervisor"

  def add_arguments(self, parser):
    parser.add_argument("--backend", default="default")
    parser.add_argument("--config")
    parser.add_argument("--mode", choices=("fork", "async"))
    parser.add_argument("--only-work", action="store_true")
    parser.add_argument("--only-dispatch", action="store_true")
    parser.add_argument("--skip-recurring", action="store_true")
    parser.add_argument("--migration-wait-timeout", type=float, default=MIGRATION_WAIT_TIMEOUT)

  def handle(self, *args, **options):
    cli_overrides = {
      "config": options["config"],
      "mode": options["mode"],
      "only_work": options["only_work"],
      "only_dispatch": options["only_dispatch"],
      "skip_recurring": options["skip_recurring"],
    }
    try:
      config = load_backend_config(
        options["backend"],
        cli_overrides=cli_overrides,
      )
      wait_for_dj_queue_migrations(
        config.database_alias,
        timeout=options["migration_wait_timeout"],
        stdout=self.stdout,
      )
      supervisor = build_supervisor(
        backend_alias=options["backend"],
        cli_overrides=cli_overrides,
      )
    except ImproperlyConfigured as exc:
      raise CommandError(str(exc)) from exc
    supervisor.run()
