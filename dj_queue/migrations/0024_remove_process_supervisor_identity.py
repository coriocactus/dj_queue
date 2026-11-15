# ruff: noqa: RUF012

import django.db.models.functions.comparison
from django.db import migrations, models


class Migration(migrations.Migration):
  dependencies = [
    ("dj_queue", "0023_remove_semaphore_active_count"),
  ]

  # the field returns to state so RemoveField drops the column on every database
  operations = [
    migrations.SeparateDatabaseAndState(
      state_operations=[
        migrations.AddField(
          model_name="process",
          name="supervisor_identity",
          field=models.GeneratedField(
            db_persist=True,
            expression=django.db.models.functions.comparison.Coalesce(
              "supervisor_id",
              models.Value(0, output_field=models.BigIntegerField()),
              output_field=models.BigIntegerField(),
            ),
            output_field=models.BigIntegerField(),
          ),
        ),
      ],
    ),
    migrations.RemoveField(
      model_name="process",
      name="supervisor_identity",
    ),
  ]
