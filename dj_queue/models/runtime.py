from django.db import models


class Process(models.Model):
  kind = models.CharField(max_length=32)
  pid = models.IntegerField()
  hostname = models.CharField(max_length=255)
  name = models.CharField(max_length=255)
  metadata = models.JSONField(default=dict)
  supervisor = models.ForeignKey(
    "self",
    null=True,
    blank=True,
    on_delete=models.SET_NULL,
    related_name="children",
  )
  last_heartbeat_at = models.DateTimeField()
  created_at = models.DateTimeField(auto_now_add=True)

  class Meta:
    db_table = "dj_queue_processes"
    constraints = [
      models.UniqueConstraint(
        fields=["name", "supervisor"],
        name="dj_queue_processes_name_supervisor_unique",
      )
    ]
    indexes = [
      models.Index(fields=["name", "supervisor"]),
      models.Index(fields=["last_heartbeat_at"]),
    ]
