from datetime import timedelta

from django.core.management.base import BaseCommand
from django.utils import timezone

from dj_queue.config import load_backend_config
from dj_queue.db import get_database_alias
from dj_queue.models import Process


class Command(BaseCommand):
  help = "Check dj_queue process health"

  def add_arguments(self, parser):
    parser.add_argument("--backend", default="default")
    parser.add_argument("--max-age", type=int)

  def handle(self, *args, **options):
    backend_alias = options["backend"]
    config = load_backend_config(backend_alias)
    max_age = options["max_age"]
    if max_age is None:
      max_age = config.process_alive_threshold

    cutoff = timezone.now() - timedelta(seconds=max_age)
    alias = get_database_alias(backend_alias)
    healthy = (
      Process.objects.using(alias)
      .filter(
        metadata__backend_alias=backend_alias,
        last_heartbeat_at__gte=cutoff,
      )
      .exists()
    )
    if healthy:
      self.stdout.write("healthy")
      return

    raise SystemExit(1)
