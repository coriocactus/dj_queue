from croniter import croniter
from django.core.exceptions import ValidationError
from django.db import models


class RecurringTask(models.Model):
  key = models.CharField(max_length=255, unique=True)
  task_path = models.CharField(max_length=255)
  payload = models.JSONField(null=True, blank=True)
  schedule = models.CharField(max_length=255)
  queue_name = models.CharField(max_length=64, default="default")
  priority = models.SmallIntegerField(default=0)
  description = models.TextField(default="", blank=True)
  static = models.BooleanField(default=False)
  created_at = models.DateTimeField(auto_now_add=True)
  updated_at = models.DateTimeField(auto_now=True)

  class Meta:
    db_table = "dj_queue_recurring_tasks"

  def clean(self):
    super().clean()
    if not croniter.is_valid(str(self.schedule)):
      raise ValidationError({"schedule": "schedule must be a valid cron expression"})

  def save(self, *args, **kwargs):
    self.full_clean()
    return super().save(*args, **kwargs)


class RecurringExecution(models.Model):
  job = models.OneToOneField(
    "dj_queue.Job",
    null=True,
    blank=True,
    on_delete=models.CASCADE,
    related_name="recurring_execution",
  )
  task_key = models.CharField(max_length=255)
  run_at = models.DateTimeField()
  created_at = models.DateTimeField(auto_now_add=True)

  class Meta:
    db_table = "dj_queue_recurring_executions"
    constraints = [
      models.UniqueConstraint(
        fields=["task_key", "run_at"],
        name="dj_queue_recurring_executions_task_key_run_at_unique",
      )
    ]
    indexes = [models.Index(fields=["task_key", "run_at"])]
