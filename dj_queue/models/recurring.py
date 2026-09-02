from django.db import models


class RecurringTask(models.Model):
  backend_alias = models.CharField(max_length=64, default="default")
  key = models.CharField(max_length=255)
  task_path = models.CharField(max_length=255)
  payload = models.JSONField(null=True, blank=True)
  schedule = models.CharField(max_length=255)
  queue_name = models.CharField(max_length=64, default="default")
  priority = models.SmallIntegerField(default=0)
  description = models.TextField(default="", blank=True)
  static = models.BooleanField(default=False)
  next_run_at = models.DateTimeField(null=True, blank=True)
  created_at = models.DateTimeField(auto_now_add=True)
  updated_at = models.DateTimeField(auto_now=True)

  class Meta:
    db_table = "dj_queue_recurring_tasks"
    constraints = [
      models.UniqueConstraint(
        fields=["backend_alias", "key"],
        name="dj_queue_recurring_tasks_backend_alias_key_unique",
      )
    ]
    indexes = [
      models.Index(fields=["backend_alias", "key"]),
      models.Index(
        fields=["backend_alias", "next_run_at", "key"],
        name="dj_queue_rt_next_run_idx",
      ),
    ]


class RecurringExecution(models.Model):
  backend_alias = models.CharField(max_length=64, default="default")
  intended_job_id = models.UUIDField(null=True)
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
        fields=["backend_alias", "task_key", "run_at"],
        name="dj_queue_recur_exec_backend_run_at_unique",
      ),
    ]
    indexes = [
      models.Index(fields=["backend_alias", "task_key", "run_at"]),
      models.Index(fields=["backend_alias", "run_at", "id"]),
    ]
