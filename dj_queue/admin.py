import json

from django.contrib import admin, messages

from dj_queue.models import FailedExecution, Job, Pause, Process, RecurringTask, Semaphore


@admin.register(Job)
class JobAdmin(admin.ModelAdmin):
  list_display = ("id", "task_path", "queue_name", "priority", "status", "created_at")
  list_select_related = (
    "ready_execution",
    "scheduled_execution",
    "claimed_execution",
    "blocked_execution",
    "failed_execution",
  )
  readonly_fields = (
    "task_path",
    "queue_name",
    "priority",
    "payload",
    "backend_name",
    "scheduled_at",
    "concurrency_key",
    "finished_at",
    "return_value",
    "created_at",
    "updated_at",
  )


@admin.register(FailedExecution)
class FailedExecutionAdmin(admin.ModelAdmin):
  list_display = ("job", "exception_class", "message", "created_at")
  list_select_related = ("job",)
  actions = ("retry_jobs",)
  readonly_fields = ("job", "exception_class", "message", "traceback", "created_at")

  @admin.action(description="Retry selected failed jobs")
  def retry_jobs(self, request, queryset):
    retried = FailedExecution.retry_all(queryset)
    self.message_user(request, f"Retried {retried} failed jobs", level=messages.SUCCESS)


@admin.register(Process)
class ProcessAdmin(admin.ModelAdmin):
  list_display = ("name", "kind", "pid", "hostname", "metadata_json", "last_heartbeat_at")
  readonly_fields = (
    "kind",
    "pid",
    "hostname",
    "name",
    "metadata",
    "supervisor",
    "last_heartbeat_at",
  )

  @admin.display(description="metadata")
  def metadata_json(self, obj):
    return json.dumps(obj.metadata, sort_keys=True)


@admin.register(RecurringTask)
class RecurringTaskAdmin(admin.ModelAdmin):
  list_display = ("key", "task_path", "schedule", "queue_name", "priority", "static")
  readonly_fields = (
    "key",
    "task_path",
    "payload",
    "schedule",
    "queue_name",
    "priority",
    "description",
    "static",
    "created_at",
    "updated_at",
  )


@admin.register(Pause)
class PauseAdmin(admin.ModelAdmin):
  list_display = ("queue_name", "created_at")
  readonly_fields = ("queue_name", "created_at")


@admin.register(Semaphore)
class SemaphoreAdmin(admin.ModelAdmin):
  list_display = ("key", "value", "limit", "expires_at")
  readonly_fields = ("key", "value", "limit", "expires_at", "created_at", "updated_at")
