from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
  help = "Start the dj_queue supervisor"

  def handle(self, *args, **options):
    raise CommandError("dj_queue command is not implemented yet")
