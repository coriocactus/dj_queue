from django.core.management.base import BaseCommand

from dj_queue import observability


class Command(BaseCommand):
  help = "Check dj_queue process health"

  def add_arguments(self, parser):
    parser.add_argument("--backend", default="default")
    parser.add_argument("--max-age", type=int)
    parser.add_argument("--deep", action="store_true")

  def handle(self, *args, **options):
    backend_alias = options["backend"]
    max_age = options["max_age"]
    healthy = observability.has_live_processes(backend_alias=backend_alias, max_age=max_age)
    problems = ()
    if healthy and options["deep"]:
      problems = observability.deep_health_problems(backend_alias=backend_alias, max_age=max_age)

    if healthy and not problems:
      self.stdout.write("healthy")
      return

    for problem in problems:
      self.stderr.write(problem)
    raise SystemExit(1)
