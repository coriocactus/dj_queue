from django.db import models


class Semaphore(models.Model):
  key = models.CharField(max_length=255, unique=True)
  value = models.IntegerField()
  limit = models.IntegerField()
  expires_at = models.DateTimeField()
  created_at = models.DateTimeField(auto_now_add=True)
  updated_at = models.DateTimeField(auto_now=True)

  class Meta:
    db_table = "dj_queue_semaphores"
    indexes = [
      models.Index(fields=["key", "value"]),
      models.Index(fields=["expires_at"]),
    ]


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


class Pause(models.Model):
  queue_name = models.CharField(max_length=64, unique=True)
  created_at = models.DateTimeField(auto_now_add=True)

  class Meta:
    db_table = "dj_queue_pauses"


class Dashboard(models.Model):
  class Meta:
    managed = False
    default_permissions = ()
    verbose_name = "dashboard"
    verbose_name_plural = "dashboard"
