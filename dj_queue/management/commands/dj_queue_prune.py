from django.core.exceptions import ImproperlyConfigured
from django.core.management.base import BaseCommand, CommandError

from dj_queue.operations.cleanup import (
  clear_failed_jobs,
  clear_finished_jobs,
  clear_recurring_executions,
)


class Command(BaseCommand):
  help = "Prune retained dj_queue rows"

  def add_arguments(self, parser):
    parser.add_argument(
      "--backend",
      default="default",
      help="TASKS backend alias (default: default)",
    )
    parser.add_argument(
      "--older-than",
      type=int,
      help="minimum finished-job age in seconds (default: backend configuration)",
    )
    parser.add_argument(
      "--failed-older-than",
      type=int,
      help="minimum failed-job age in seconds (default: backend configuration)",
    )
    parser.add_argument(
      "--recurring-older-than",
      type=int,
      help="minimum recurring-execution age in seconds (default: backend configuration)",
    )
    parser.add_argument(
      "--batch-size",
      type=int,
      default=500,
      help="maximum rows to prune per row type (default: 500)",
    )
    parser.add_argument(
      "--task-path",
      help="limit finished and failed jobs to one task import path",
    )
    parser.add_argument(
      "--task-key",
      help="limit recurring executions to one recurring task key",
    )

  def handle(self, *args, **options):
    for option_name in ("older_than", "failed_older_than", "recurring_older_than"):
      value = options[option_name]
      if value is not None and value < 0:
        raise CommandError(f"--{option_name.replace('_', '-')} must be non-negative")
    batch_size = options["batch_size"]
    if batch_size <= 0:
      raise CommandError("--batch-size must be positive")

    try:
      finished_deleted = clear_finished_jobs(
        older_than=options["older_than"],
        task_path=options["task_path"],
        batch_size=batch_size,
        backend_alias=options["backend"],
      )
      failed_deleted = clear_failed_jobs(
        older_than=options["failed_older_than"],
        task_path=options["task_path"],
        batch_size=batch_size,
        backend_alias=options["backend"],
      )
      recurring_deleted = clear_recurring_executions(
        older_than=options["recurring_older_than"],
        task_key=options["task_key"],
        batch_size=batch_size,
        backend_alias=options["backend"],
      )
    except ImproperlyConfigured as exc:
      raise CommandError(str(exc)) from exc

    self.stdout.write(
      f"deleted {finished_deleted} finished jobs, {failed_deleted} failed jobs, "
      f"and {recurring_deleted} recurring executions"
    )
    if batch_size in (finished_deleted, failed_deleted, recurring_deleted):
      self.stdout.write("batch limit reached; run dj_queue_prune again to check for more")
