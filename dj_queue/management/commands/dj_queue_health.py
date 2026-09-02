from django.core.exceptions import ImproperlyConfigured
from django.core.management.base import BaseCommand, CommandError

from dj_queue import observability


class Command(BaseCommand):
  help = "Check dj_queue process health"

  def add_arguments(self, parser):
    parser.add_argument(
      "--backend",
      default="default",
      help="TASKS backend alias (default: default)",
    )
    parser.add_argument(
      "--max-age",
      type=int,
      help="maximum process heartbeat age in seconds (default: process_alive_threshold)",
    )
    parser.add_argument(
      "--deep",
      action="store_true",
      help="also check persisted queue invariants",
    )
    parser.add_argument(
      "--require-version",
      help="require all live queue processes to run this exact dj_queue version",
    )

  def handle(self, *args, **options):
    backend_alias = options["backend"]
    max_age = options["max_age"]
    required_version = options["require_version"]
    if max_age is not None and max_age <= 0:
      raise CommandError("--max-age must be positive")

    try:
      healthy = observability.has_live_processes(backend_alias=backend_alias, max_age=max_age)
      problems = ()
      if healthy and (options["deep"] or required_version is not None):
        problems = observability.deep_health_problems(
          backend_alias=backend_alias,
          max_age=max_age,
          required_process_version=required_version,
        )
    except ImproperlyConfigured as exc:
      raise CommandError(str(exc)) from exc

    if healthy and not problems:
      self.stdout.write("healthy")
      return

    if not healthy:
      self.stderr.write("unhealthy: no live dj_queue processes")
    else:
      for problem in problems:
        self.stderr.write(problem)
    raise SystemExit(1)
