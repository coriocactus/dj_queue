# ruff: noqa: RUF012

from django.db import migrations


class Migration(migrations.Migration):
  dependencies = [
    ("dj_queue", "0020_remove_process_root_name_unique"),
  ]

  # both columns stay in the database for N-1 processes that write or select them
  operations = [
    migrations.SeparateDatabaseAndState(
      state_operations=[
        migrations.RemoveField(
          model_name="semaphore",
          name="active_count",
        ),
        migrations.RemoveField(
          model_name="process",
          name="supervisor_identity",
        ),
      ],
    ),
  ]
