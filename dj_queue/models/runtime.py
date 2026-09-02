from django.db import models
from django.db.models.functions import Coalesce


class Semaphore(models.Model):
  key = models.CharField(max_length=255, unique=True)
  value = models.IntegerField()
  active_count = models.IntegerField(null=True)
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

  @property
  def available_count(self):
    return max(0, self.value)

  @property
  def occupied_count(self):
    return max(0, self.limit - self.value)


class Process(models.Model):
  backend_alias = models.CharField(max_length=64, default="default")
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
  supervisor_identity = models.GeneratedField(
    expression=Coalesce(
      "supervisor_id",
      models.Value(0, output_field=models.BigIntegerField()),
      output_field=models.BigIntegerField(),
    ),
    output_field=models.BigIntegerField(),
    db_persist=True,
    editable=False,
  )
  last_heartbeat_at = models.DateTimeField()
  created_at = models.DateTimeField(auto_now_add=True)

  class Meta:
    db_table = "dj_queue_processes"
    constraints = [
      models.UniqueConstraint(
        fields=["backend_alias", "name", "supervisor_identity"],
        name="djq_pr_backend_name_parent_uniq",
      )
    ]
    indexes = [
      models.Index(fields=["backend_alias"]),
      models.Index(fields=["name", "supervisor"]),
      models.Index(fields=["last_heartbeat_at"]),
      models.Index(
        fields=["backend_alias", "last_heartbeat_at", "id"],
        name="djq_pr_b_heart_id_idx",
      ),
    ]


class Pause(models.Model):
  backend_alias = models.CharField(max_length=64, default="default")
  queue_name = models.CharField(max_length=64)
  created_at = models.DateTimeField(auto_now_add=True)

  class Meta:
    db_table = "dj_queue_pauses"
    constraints = [
      models.UniqueConstraint(
        fields=["backend_alias", "queue_name"],
        name="dj_queue_pauses_backend_alias_queue_name_unique",
      )
    ]
    indexes = [models.Index(fields=["backend_alias", "queue_name"])]


class Dashboard(models.Model):
  class Meta:
    managed = False
    default_permissions = ()
    verbose_name = "dashboard"
    verbose_name_plural = "dashboard"
