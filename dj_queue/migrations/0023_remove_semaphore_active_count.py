# ruff: noqa: RUF012

from django.db import migrations, models


class Migration(migrations.Migration):
  dependencies = [
    ("dj_queue", "0022_resolve_legacy_recurring_reservations"),
  ]

  # the field returns to state so RemoveField drops the column on every database
  operations = [
    migrations.SeparateDatabaseAndState(
      state_operations=[
        migrations.AddField(
          model_name="semaphore",
          name="active_count",
          field=models.IntegerField(null=True),
        ),
      ],
    ),
    migrations.RemoveField(
      model_name="semaphore",
      name="active_count",
    ),
  ]
