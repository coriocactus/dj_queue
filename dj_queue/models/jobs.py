import uuid

from django.core.exceptions import ValidationError
from django.db import models
from django.db.models import Q


class Job(models.Model):
  id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
  task_path = models.TextField()
  queue_name = models.CharField(max_length=64, default="default")
  priority = models.SmallIntegerField(default=0)
  payload = models.JSONField(default=dict)
  backend_name = models.CharField(max_length=64)
  scheduled_at = models.DateTimeField(null=True, blank=True)
  concurrency_key = models.CharField(max_length=255, null=True, blank=True)
  finished_at = models.DateTimeField(null=True, blank=True)
  return_value = models.JSONField(null=True, blank=True)
  created_at = models.DateTimeField(auto_now_add=True)
  updated_at = models.DateTimeField(auto_now=True)

  class Meta:
    db_table = "dj_queue_jobs"
    constraints = [
      models.CheckConstraint(
        condition=Q(priority__gte=-100) & Q(priority__lte=100),
        name="dj_queue_jobs_priority_range",
      )
    ]
    indexes = [
      models.Index(fields=["queue_name", "finished_at"]),
      models.Index(fields=["scheduled_at", "finished_at"]),
      models.Index(fields=["finished_at"]),
    ]


class ReadyExecution(models.Model):
  job = models.OneToOneField(
    Job,
    on_delete=models.CASCADE,
    related_name="ready_execution",
  )
  queue_name = models.CharField(max_length=64)
  priority = models.SmallIntegerField()
  created_at = models.DateTimeField(auto_now_add=True)

  class Meta:
    db_table = "dj_queue_ready_executions"
    indexes = [
      models.Index(fields=["priority", "id"]),
      models.Index(fields=["queue_name", "priority", "id"]),
    ]

  def clean(self):
    super().clean()
    _validate_live_state(self)

  def save(self, *args, **kwargs):
    self.full_clean()
    return super().save(*args, **kwargs)


class ScheduledExecution(models.Model):
  job = models.OneToOneField(
    Job,
    on_delete=models.CASCADE,
    related_name="scheduled_execution",
  )
  queue_name = models.CharField(max_length=64)
  priority = models.SmallIntegerField()
  scheduled_at = models.DateTimeField()
  created_at = models.DateTimeField(auto_now_add=True)

  class Meta:
    db_table = "dj_queue_scheduled_executions"
    indexes = [models.Index(fields=["scheduled_at", "priority", "id"])]

  def clean(self):
    super().clean()
    _validate_live_state(self)

  def save(self, *args, **kwargs):
    self.full_clean()
    return super().save(*args, **kwargs)


class ClaimedExecution(models.Model):
  job = models.OneToOneField(
    Job,
    on_delete=models.CASCADE,
    related_name="claimed_execution",
  )
  process = models.ForeignKey(
    "dj_queue.Process",
    null=True,
    blank=True,
    on_delete=models.SET_NULL,
    related_name="claimed_executions",
  )
  created_at = models.DateTimeField(auto_now_add=True)

  class Meta:
    db_table = "dj_queue_claimed_executions"
    indexes = [models.Index(fields=["process", "job"])]

  def clean(self):
    super().clean()
    _validate_live_state(self)

  def save(self, *args, **kwargs):
    self.full_clean()
    return super().save(*args, **kwargs)


class BlockedExecution(models.Model):
  job = models.OneToOneField(
    Job,
    on_delete=models.CASCADE,
    related_name="blocked_execution",
  )
  queue_name = models.CharField(max_length=64)
  priority = models.SmallIntegerField()
  concurrency_key = models.CharField(max_length=255)
  expires_at = models.DateTimeField()
  created_at = models.DateTimeField(auto_now_add=True)

  class Meta:
    db_table = "dj_queue_blocked_executions"
    indexes = [
      models.Index(fields=["concurrency_key", "priority", "id"]),
      models.Index(fields=["expires_at", "concurrency_key"]),
    ]

  def clean(self):
    super().clean()
    _validate_live_state(self)

  def save(self, *args, **kwargs):
    self.full_clean()
    return super().save(*args, **kwargs)


class FailedExecution(models.Model):
  job = models.OneToOneField(
    Job,
    on_delete=models.CASCADE,
    related_name="failed_execution",
  )
  exception_class = models.CharField(max_length=255)
  message = models.TextField(default="")
  traceback = models.TextField(default="")
  created_at = models.DateTimeField(auto_now_add=True)

  class Meta:
    db_table = "dj_queue_failed_executions"

  def clean(self):
    super().clean()
    _validate_live_state(self)

  def save(self, *args, **kwargs):
    self.full_clean()
    return super().save(*args, **kwargs)


def _validate_live_state(instance):
  if not instance.job_id:
    return

  for model in LIVE_STATE_MODELS:
    queryset = model._default_manager.filter(job_id=instance.job_id)
    if model is instance.__class__ and instance.pk is not None:
      queryset = queryset.exclude(pk=instance.pk)
    if queryset.exists():
      raise ValidationError({"job": "job already has a live execution state"})


LIVE_STATE_MODELS = (
  ReadyExecution,
  ScheduledExecution,
  ClaimedExecution,
  BlockedExecution,
  FailedExecution,
)
