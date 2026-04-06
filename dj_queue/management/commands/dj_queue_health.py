from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
  help = "Check dj_queue process health"

  def handle(self, *args, **options):
    raise CommandError("dj_queue_health command is not implemented yet")
