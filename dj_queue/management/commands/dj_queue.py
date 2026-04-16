from django.core.exceptions import ImproperlyConfigured
from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from dj_queue.config import load_backend_config
from dj_queue.runtime.supervisor import AsyncSupervisor, ForkSupervisor


def build_supervisor(*, backend_alias, cli_overrides):
  mode = load_backend_config(backend_alias, cli_overrides=cli_overrides).mode
  supervisor_class = AsyncSupervisor if mode == "async" else ForkSupervisor
  return supervisor_class.from_backend_config(
    backend_alias=backend_alias,
    cli_overrides=cli_overrides,
  )


class Command(BaseCommand):
  help = "Start the dj_queue supervisor"

  def add_arguments(self, parser):
    parser.add_argument("--backend", default="default")
    parser.add_argument("--config")
    parser.add_argument("--mode", choices=("fork", "async"))
    parser.add_argument("--only-work", action="store_true")
    parser.add_argument("--only-dispatch", action="store_true")
    parser.add_argument("--skip-recurring", action="store_true")

  def handle(self, *args, **options):
    cli_overrides = {
      "config": options["config"],
      "mode": options["mode"],
      "only_work": options["only_work"],
      "only_dispatch": options["only_dispatch"],
      "skip_recurring": options["skip_recurring"],
    }
    try:
      supervisor = build_supervisor(
        backend_alias=options["backend"],
        cli_overrides=cli_overrides,
      )
    except ImproperlyConfigured as exc:
      raise CommandError(str(exc)) from exc
    supervisor.run()
