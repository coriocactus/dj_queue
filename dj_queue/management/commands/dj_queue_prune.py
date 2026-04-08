from django.core.management.base import BaseCommand

from dj_queue.operations.cleanup import clear_finished_jobs


class Command(BaseCommand):
  help = "Prune finished dj_queue jobs"

  def add_arguments(self, parser):
    parser.add_argument("--backend", default="default")
    parser.add_argument("--older-than", type=int)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--task-path")

  def handle(self, *args, **options):
    deleted = clear_finished_jobs(
      older_than=options["older_than"],
      task_path=options["task_path"],
      batch_size=options["batch_size"],
      backend_alias=options["backend"],
    )
    self.stdout.write(f"deleted {deleted} finished jobs")
