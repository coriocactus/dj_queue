from django.core.management.base import BaseCommand

from dj_queue.operations.cleanup import (
  clear_failed_jobs,
  clear_finished_jobs,
  clear_recurring_executions,
)


class Command(BaseCommand):
  help = "Prune retained dj_queue rows"

  def add_arguments(self, parser):
    parser.add_argument("--backend", default="default")
    parser.add_argument("--older-than", type=int)
    parser.add_argument("--failed-older-than", type=int)
    parser.add_argument("--recurring-older-than", type=int)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--task-path")
    parser.add_argument("--task-key")

  def handle(self, *args, **options):
    finished_deleted = clear_finished_jobs(
      older_than=options["older_than"],
      task_path=options["task_path"],
      batch_size=options["batch_size"],
      backend_alias=options["backend"],
    )
    failed_deleted = clear_failed_jobs(
      older_than=options["failed_older_than"],
      task_path=options["task_path"],
      batch_size=options["batch_size"],
      backend_alias=options["backend"],
    )
    recurring_deleted = clear_recurring_executions(
      older_than=options["recurring_older_than"],
      task_key=options["task_key"],
      batch_size=options["batch_size"],
      backend_alias=options["backend"],
    )
    self.stdout.write(
      f"deleted {finished_deleted} finished jobs, {failed_deleted} failed jobs, "
      f"and {recurring_deleted} recurring executions"
    )
