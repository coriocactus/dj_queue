import json
from functools import wraps
from urllib.parse import urlencode

from django.contrib import admin, messages
from django.http import HttpResponseNotAllowed, HttpResponseRedirect
from django.template.response import TemplateResponse
from django.urls import path, reverse
from django.utils.http import url_has_allowed_host_and_scheme

from dj_queue import dashboard
from dj_queue.models import (
  Dashboard,
  FailedExecution,
  Job,
  Pause,
  Process,
  RecurringTask,
  Semaphore,
)


@admin.register(Dashboard)
class DashboardAdmin(admin.ModelAdmin):
  def has_add_permission(self, request):
    return False

  def has_change_permission(self, request, obj=None):
    return False

  def has_delete_permission(self, request, obj=None):
    return False

  def has_module_permission(self, request):
    return bool(request.user and request.user.is_staff)

  def has_view_permission(self, request, obj=None):
    return bool(request.user and request.user.is_staff)

  def get_model_perms(self, request):
    if not self.has_view_permission(request):
      return {}
    return {"view": True}

  def changelist_view(self, request, extra_context=None):
    backend_alias = dashboard.resolve_backend_alias(request.GET.get("backend"))
    context = {
      **self.admin_site.each_context(request),
      **dashboard.dashboard_context(backend_alias=backend_alias),
    }
    if extra_context:
      context.update(extra_context)
    return TemplateResponse(request, "admin/dj_queue/dashboard.html", context)

  def get_urls(self):
    def wrap(view):
      @wraps(view)
      def wrapper(request, *args, **kwargs):
        request.current_app = self.admin_site.name
        return self.admin_site.admin_view(view)(request, *args, **kwargs)

      return wrapper

    return [
      path("queue/<str:queue_name>/", wrap(self.queue_view), name="dj_queue_dashboard_queue"),
      path(
        "queue/<str:queue_name>/action/",
        wrap(self.queue_action_view),
        name="dj_queue_dashboard_queue_action",
      ),
      path(
        "queue/<str:queue_name>/jobs/action/",
        wrap(self.job_action_view),
        name="dj_queue_dashboard_job_action",
      ),
    ] + super().get_urls()

  def queue_view(self, request, queue_name):
    backend_alias = dashboard.resolve_backend_alias(request.GET.get("backend"))
    state = request.GET.get("state", "ready")
    page_number = request.GET.get("page", 1)
    context = {
      **self.admin_site.each_context(request),
      **dashboard.queue_page_context(
        backend_alias=backend_alias,
        queue_name=queue_name,
        state=state,
        page_number=page_number,
      ),
      "job_actions": dashboard.job_actions_for_state(state),
    }
    return TemplateResponse(request, "admin/dj_queue/queue_jobs.html", context)

  def queue_action_view(self, request, queue_name):
    if request.method != "POST":
      return HttpResponseNotAllowed(["POST"])

    backend_alias = dashboard.resolve_backend_alias(request.POST.get("backend"))
    action = request.POST.get("action")
    try:
      message = dashboard.apply_queue_action(
        backend_alias=backend_alias,
        queue_name=queue_name,
        action=action,
      )
    except ValueError as exc:
      self.message_user(request, str(exc), level=messages.ERROR)
    else:
      self.message_user(request, message, level=messages.SUCCESS)
    return self._redirect(
      request, self._queue_url(backend_alias=backend_alias, queue_name=queue_name)
    )

  def job_action_view(self, request, queue_name):
    if request.method != "POST":
      return HttpResponseNotAllowed(["POST"])

    backend_alias = dashboard.resolve_backend_alias(request.POST.get("backend"))
    state = request.POST.get("state", "ready")
    job_ids = [job_id for job_id in request.POST.getlist("job_ids") if job_id]
    try:
      message = dashboard.apply_job_action(
        backend_alias=backend_alias,
        queue_name=queue_name,
        state=state,
        action=request.POST.get("action"),
        job_ids=job_ids,
      )
    except ValueError as exc:
      self.message_user(request, str(exc), level=messages.ERROR)
    else:
      self.message_user(request, message, level=messages.SUCCESS)
    return self._redirect(
      request,
      self._queue_url(backend_alias=backend_alias, queue_name=queue_name, state=state),
    )

  def _redirect(self, request, fallback_url):
    next_url = request.POST.get("next")
    if next_url and url_has_allowed_host_and_scheme(
      next_url,
      allowed_hosts={request.get_host()},
      require_https=request.is_secure(),
    ):
      return HttpResponseRedirect(next_url)
    return HttpResponseRedirect(fallback_url)

  def _queue_url(self, *, backend_alias, queue_name, state=None):
    url = reverse("admin:dj_queue_dashboard_queue", args=[queue_name])
    query = {"backend": backend_alias}
    if state is not None:
      query["state"] = state
    return f"{url}?{urlencode(query)}"


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
  actions = ("retry_jobs", "discard_jobs")
  readonly_fields = ("job", "exception_class", "message", "traceback", "created_at")

  @admin.action(description="Retry selected failed jobs")
  def retry_jobs(self, request, queryset):
    retried = FailedExecution.retry_all(queryset)
    self.message_user(request, f"Retried {retried} failed jobs", level=messages.SUCCESS)

  @admin.action(description="Discard selected failed jobs")
  def discard_jobs(self, request, queryset):
    discarded = 0
    for execution in queryset.select_related("job"):
      discarded += execution.discard()
    self.message_user(request, f"Discarded {discarded} failed jobs", level=messages.SUCCESS)


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
