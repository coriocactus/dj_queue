from django.core.management.base import BaseCommand

from dj_queue import observability


class Command(BaseCommand):
  help = "Check dj_queue process health"

  def add_arguments(self, parser):
    parser.add_argument("--backend", default="default")
    parser.add_argument("--max-age", type=int)

  def handle(self, *args, **options):
    backend_alias = options["backend"]
    max_age = options["max_age"]
    if observability.has_live_processes(backend_alias=backend_alias, max_age=max_age):
      self.stdout.write("healthy")
      return

    raise SystemExit(1)
