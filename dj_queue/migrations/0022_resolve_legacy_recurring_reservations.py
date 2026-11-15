# ruff: noqa: RUF012

from django.db import migrations
from django.db.models import F


def resolve_legacy_reservations(apps, schema_editor):
  RecurringExecution = apps.get_model("dj_queue", "RecurringExecution")
  legacy = RecurringExecution.objects.using(schema_editor.connection.alias).filter(
    intended_job_id__isnull=True
  )
  legacy.filter(job__isnull=False).update(intended_job_id=F("job_id"))
  legacy.filter(job__isnull=True).delete()


class Migration(migrations.Migration):
  dependencies = [
    ("dj_queue", "0021_remove_bridge_fields_from_state"),
  ]

  operations = [
    migrations.RunPython(resolve_legacy_reservations, migrations.RunPython.noop),
  ]
