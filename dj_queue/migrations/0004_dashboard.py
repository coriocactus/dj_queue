from django.db import migrations, models


class Migration(migrations.Migration):
  dependencies = [
    ("dj_queue", "0003_recurringtask_recurringexecution"),
  ]

  operations = [
    migrations.CreateModel(
      name="Dashboard",
      fields=[
        (
          "id",
          models.BigAutoField(
            auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
          ),
        ),
      ],
      options={
        "verbose_name": "dashboard",
        "verbose_name_plural": "dashboard",
        "managed": False,
        "default_permissions": (),
      },
    ),
  ]
