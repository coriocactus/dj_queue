from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
  help = "Prune finished dj_queue jobs"

  def handle(self, *args, **options):
    raise CommandError("dj_queue_prune command is not implemented yet")
