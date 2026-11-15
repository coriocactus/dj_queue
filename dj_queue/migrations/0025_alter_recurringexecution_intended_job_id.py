# ruff: noqa: RUF012

from django.db import migrations, models


class Migration(migrations.Migration):
  dependencies = [
    ("dj_queue", "0024_remove_process_supervisor_identity"),
  ]

  operations = [
    migrations.AlterField(
      model_name="recurringexecution",
      name="intended_job_id",
      field=models.UUIDField(),
    ),
  ]
