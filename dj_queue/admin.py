import json
from datetime import timedelta
from functools import wraps
from urllib.parse import parse_qsl, urlencode

from django.contrib import admin, messages
from django.db.models import Case, Count, IntegerField, OuterRef, Subquery, Value, When
from django.db.models.functions import Coalesce
from django.http import HttpResponseNotAllowed, HttpResponseRedirect
from django.template.response import TemplateResponse
from django.urls import path, reverse
from django.utils.html import format_html
from django.utils.http import url_has_allowed_host_and_scheme
from django.utils import timezone

from dj_queue.config import load_backend_config
from dj_queue import dashboard
from dj_queue.db import get_database_alias
from dj_queue.models import (
  BlockedExecution,
  Dashboard,
  FailedExecution,
  Job,
  Pause,
  Process,
  RecurringTask,
  Semaphore,
)
from dj_queue.operations.jobs import enqueue_job_again


class DjQueueFirstAdminSite(admin.AdminSite):
  def _dashboard_app_url(self):
    return reverse("admin:dj_queue_dashboard_changelist", current_app=self.name)

  def get_app_list(self, request, app_label=None):
    app_list = super().get_app_list(request, app_label=app_label)
    for app in app_list:
      if app["app_label"] == "dj_queue":
        app["app_url"] = self._dashboard_app_url()
    return sorted(app_list, key=lambda app: app["app_label"] != "dj_queue")

  def app_index(self, request, app_label, extra_context=None):
    if app_label == "dj_queue":
      url = self._dashboard_app_url()
      query = request.GET.urlencode()
      if query:
        url = f"{url}?{query}"
      return HttpResponseRedirect(url)
    return super().app_index(request, app_label, extra_context=extra_context)


admin.site.__class__ = DjQueueFirstAdminSite


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
      **dashboard.dashboard_context(backend_alias=backend_alias, query_params=request.GET),
      "title": "dj_queue",
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
    queue_context = dashboard.queue_page_context(
      backend_alias=backend_alias,
      queue_name=queue_name,
      state=state,
      page_number=page_number,
      query_params=request.GET,
    )
    context = {
      **self.admin_site.each_context(request),
      **queue_context,
      "job_actions": dashboard.job_actions_for_state(state),
      "title": "dj_queue",
      "subtitle": queue_name,
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


class HiddenSidebarAdminMixin:
  backend_query_param = "backend"
  backend_filter_field = None
  ignored_filter_params = ()
  change_list_template = "admin/dj_queue/change_list.html"
  change_form_template = "admin/dj_queue/change_form.html"

  def get_list_filter(self, request):
    return (BackendListFilter, *tuple(super().get_list_filter(request)))

  def changelist_view(self, request, extra_context=None):
    extra_context = {**(extra_context or {}), "dashboard_url": self._dashboard_url(request)}
    return super().changelist_view(request, extra_context=extra_context)

  def changeform_view(self, request, object_id=None, form_url="", extra_context=None):
    obj = self.get_object(request, object_id) if object_id is not None else None
    if request.method == "POST":
      action = request.POST.get("_djq_object_action")
      if action and obj is not None:
        return self.handle_change_action(request, obj, action)

    extra_context = {
      **(extra_context or {}),
      "dashboard_url": self._dashboard_url(request),
      "changelist_url": self._changelist_url(backend_alias=self._backend_alias(request)),
      "change_actions": self.get_change_actions(request, obj),
    }
    return super().changeform_view(
      request,
      object_id=object_id,
      form_url=form_url,
      extra_context=extra_context,
    )

  def has_add_permission(self, request):
    return False

  def has_delete_permission(self, request, obj=None):
    return False

  def get_model_perms(self, request):
    if not self.has_view_permission(request):
      return {}
    return {}

  def get_queryset(self, request):
    queryset = super().get_queryset(request)
    queryset = queryset.using(self._backend_database_alias(request))
    if self.backend_filter_field is not None:
      queryset = queryset.filter(**{self.backend_filter_field: self._backend_alias(request)})
    return queryset

  def get_object(self, request, object_id, from_field=None):
    queryset = self.get_queryset(request)
    model = queryset.model
    field = model._meta.pk if from_field is None else model._meta.get_field(from_field)
    try:
      object_id = field.to_python(object_id)
      return queryset.get(**{field.name: object_id})
    except (model.DoesNotExist, ValueError):
      return None

  def get_changelist(self, request, **kwargs):
    parent = super().get_changelist(request, **kwargs)
    ignored_params = self.ignored_filter_params

    class BackendScopedChangeList(parent):
      def get_filters_params(self, params=None):
        lookup_params = super().get_filters_params(params=params)
        for ignored_param in ignored_params:
          lookup_params.pop(ignored_param, None)
        return lookup_params

      def get_query_string(self, new_params=None, remove=None):
        remove = [*(remove or ()), *ignored_params]
        return super().get_query_string(new_params=new_params, remove=remove)

    return BackendScopedChangeList

  def _backend_alias(self, request):
    backend_alias = request.GET.get(self.backend_query_param)
    if backend_alias:
      return dashboard.resolve_backend_alias(backend_alias)

    preserved_filters = request.GET.get("_changelist_filters")
    if preserved_filters:
      preserved = dict(parse_qsl(preserved_filters))
      return dashboard.resolve_backend_alias(preserved.get(self.backend_query_param))

    return dashboard.resolve_backend_alias(None)

  def _backend_database_alias(self, request):
    return get_database_alias(self._backend_alias(request))

  def _dashboard_url(self, request):
    return f"{reverse('admin:dj_queue_dashboard_changelist')}?{urlencode({'backend': self._backend_alias(request)})}"

  def get_change_actions(self, request, obj):
    return ()

  def handle_change_action(self, request, obj, action):
    return HttpResponseRedirect(request.get_full_path())

  def _change_url(self, *, object_id, backend_alias):
    url = reverse(
      f"admin:{self.model._meta.app_label}_{self.model._meta.model_name}_change",
      args=[object_id],
    )
    return f"{url}?{urlencode({'backend': backend_alias})}"

  def _changelist_url(self, *, backend_alias):
    url = reverse(f"admin:{self.model._meta.app_label}_{self.model._meta.model_name}_changelist")
    return f"{url}?{urlencode({'backend': backend_alias})}"


class JobStatusListFilter(admin.SimpleListFilter):
  title = "status"
  parameter_name = "status"

  def lookups(self, request, model_admin):
    return (
      ("ready", "ready"),
      ("scheduled", "scheduled"),
      ("claimed", "claimed"),
      ("blocked", "blocked"),
      ("failed", "failed"),
      ("finished", "finished"),
    )

  def queryset(self, request, queryset):
    value = self.value()
    if value == "ready":
      return queryset.ready()
    if value == "scheduled":
      return queryset.scheduled()
    if value == "claimed":
      return queryset.claimed()
    if value == "blocked":
      return queryset.blocked()
    if value == "failed":
      return queryset.failed()
    if value == "finished":
      return queryset.finished()
    return queryset


class BackendListFilter(admin.SimpleListFilter):
  title = "backend"
  parameter_name = "backend"

  def lookups(self, request, model_admin):
    return tuple((choice.alias, choice.alias) for choice in dashboard.backend_choices())

  def queryset(self, request, queryset):
    return queryset

  def choices(self, changelist):
    selected_backend = dashboard.resolve_backend_alias(self.value())
    for lookup, title in self.lookup_choices:
      yield {
        "selected": lookup == selected_backend,
        "query_string": changelist.get_query_string({self.parameter_name: lookup}),
        "display": title,
      }


class JobRecurringTaskKeyListFilter(admin.SimpleListFilter):
  title = "recurring task key"
  parameter_name = "recurring_task_key"

  def lookups(self, request, model_admin):
    alias = model_admin._backend_database_alias(request)
    return tuple(RecurringTask.objects.using(alias).order_by("key").values_list("key", "key"))

  def queryset(self, request, queryset):
    if not self.value():
      return queryset
    return queryset.filter(recurring_execution__task_key=self.value())


class JobConcurrencyKeyListFilter(admin.SimpleListFilter):
  title = "concurrency key"
  parameter_name = "concurrency_key"

  def lookups(self, request, model_admin):
    alias = model_admin._backend_database_alias(request)
    return tuple(
      Job.objects.using(alias)
      .exclude(concurrency_key__isnull=True)
      .exclude(concurrency_key="")
      .order_by("concurrency_key")
      .values_list("concurrency_key", "concurrency_key")
      .distinct()
    )

  def queryset(self, request, queryset):
    if not self.value():
      return queryset
    return queryset.filter(concurrency_key=self.value())


class ProcessStatusListFilter(admin.SimpleListFilter):
  title = "status"
  parameter_name = "status"

  def lookups(self, request, model_admin):
    return (("live", "live"), ("stale", "stale"))

  def queryset(self, request, queryset):
    value = self.value()
    if not value:
      return queryset
    cutoff = timezone.now() - timedelta(
      seconds=load_backend_config(
        dashboard.resolve_backend_alias(request.GET.get("backend"))
      ).process_alive_threshold
    )
    if value == "live":
      return queryset.filter(last_heartbeat_at__gte=cutoff)
    if value == "stale":
      return queryset.filter(last_heartbeat_at__lt=cutoff)
    return queryset


@admin.register(Job)
class JobAdmin(HiddenSidebarAdminMixin, admin.ModelAdmin):
  backend_filter_field = "backend_name"
  ignored_filter_params = ("backend_name",)
  list_display = ("id", "task_path", "queue_name_link", "priority", "display_status", "created_at")
  list_filter = (
    "queue_name",
    JobStatusListFilter,
    JobRecurringTaskKeyListFilter,
    JobConcurrencyKeyListFilter,
  )
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
  search_fields = ("id", "task_path", "queue_name", "concurrency_key")

  def get_queryset(self, request):
    queryset = super().get_queryset(request)
    return queryset.annotate(
      status_rank=Case(
        When(ready_execution__isnull=False, then=Value(0)),
        When(scheduled_execution__isnull=False, then=Value(1)),
        When(claimed_execution__isnull=False, then=Value(2)),
        When(blocked_execution__isnull=False, then=Value(3)),
        When(failed_execution__isnull=False, then=Value(4)),
        When(finished_at__isnull=False, then=Value(5)),
        default=Value(99),
        output_field=IntegerField(),
      )
    )

  @admin.display(description="status", ordering="status_rank")
  def display_status(self, obj):
    return obj.status

  @admin.display(description="queue name", ordering="queue_name")
  def queue_name_link(self, obj):
    query = {"backend": obj.backend_name}
    if obj.status is not None:
      query["state"] = obj.status
    url = f"{reverse('admin:dj_queue_dashboard_queue', args=[obj.queue_name])}?{urlencode(query)}"
    return format_html('<a href="{}">{}</a>', url, obj.queue_name)

  def get_change_actions(self, request, obj):
    if obj is None:
      return ()
    actions = [{"name": "enqueue", "label": "Enqueue job", "css_class": "djq-object-action-retry"}]
    if obj.status == "failed":
      actions.extend(
        (
          {
            "name": "retry",
            "label": "Retry failed job",
            "css_class": "djq-object-action-retry",
          },
          {
            "name": "discard",
            "label": "Discard failed job",
            "css_class": "djq-object-action-discard",
          },
        )
      )
    return tuple(actions)

  def handle_change_action(self, request, obj, action):
    if action == "enqueue":
      try:
        new_job = enqueue_job_again(obj.pk, backend_alias=obj.backend_name)
      except Exception as exc:
        self.message_user(request, f"Could not enqueue job: {exc}", level=messages.ERROR)
        return HttpResponseRedirect(
          self._change_url(object_id=obj.pk, backend_alias=obj.backend_name)
        )

      self.message_user(
        request,
        format_html(
          'Enqueued job <a href="{}">{}</a>.',
          self._change_url(object_id=new_job.pk, backend_alias=new_job.backend_name),
          new_job.pk,
        ),
        level=messages.SUCCESS,
      )
      return HttpResponseRedirect(
        self._change_url(object_id=obj.pk, backend_alias=obj.backend_name)
      )

    if obj.status != "failed":
      self.message_user(request, "This job is not failed", level=messages.ERROR)
      return HttpResponseRedirect(
        self._change_url(object_id=obj.pk, backend_alias=obj.backend_name)
      )

    if action == "retry":
      obj.failed_execution.retry()
      self.message_user(request, "Retried failed job", level=messages.SUCCESS)
      return HttpResponseRedirect(
        self._change_url(object_id=obj.pk, backend_alias=obj.backend_name)
      )

    if action == "discard":
      obj.failed_execution.discard()
      self.message_user(request, "Discarded failed job", level=messages.SUCCESS)
      return HttpResponseRedirect(self._changelist_url(backend_alias=obj.backend_name))

    return HttpResponseRedirect(self._change_url(object_id=obj.pk, backend_alias=obj.backend_name))


@admin.register(FailedExecution)
class FailedExecutionAdmin(HiddenSidebarAdminMixin, admin.ModelAdmin):
  backend_filter_field = "job__backend_name"
  ignored_filter_params = ("job__backend_name",)
  list_display = ("job", "exception_class", "message", "created_at")
  list_filter = (
    ("job__queue_name", admin.AllValuesFieldListFilter),
    "exception_class",
  )
  list_select_related = ("job",)
  actions = ("retry_jobs", "discard_jobs")
  readonly_fields = ("job", "exception_class", "message", "traceback", "created_at")
  search_fields = ("job__id", "job__task_path", "message", "exception_class")

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

  def get_change_actions(self, request, obj):
    if obj is None:
      return ()
    return (
      {"name": "retry", "label": "Retry failed job", "css_class": "djq-object-action-retry"},
      {
        "name": "discard",
        "label": "Discard failed job",
        "css_class": "djq-object-action-discard",
      },
    )

  def handle_change_action(self, request, obj, action):
    backend_alias = obj.job.backend_name

    if action == "retry":
      job_id = obj.job_id
      obj.retry()
      self.message_user(request, "Retried failed job", level=messages.SUCCESS)
      url = reverse("admin:dj_queue_job_change", args=[job_id])
      return HttpResponseRedirect(f"{url}?{urlencode({'backend': backend_alias})}")

    if action == "discard":
      obj.discard()
      self.message_user(request, "Discarded failed job", level=messages.SUCCESS)
      return HttpResponseRedirect(self._changelist_url(backend_alias=backend_alias))

    return HttpResponseRedirect(self._change_url(object_id=obj.pk, backend_alias=backend_alias))


@admin.register(Process)
class ProcessAdmin(HiddenSidebarAdminMixin, admin.ModelAdmin):
  list_display = (
    "name",
    "kind",
    "display_status",
    "pid",
    "hostname",
    "metadata_json",
    "last_heartbeat_at",
  )
  list_filter = (ProcessStatusListFilter, "kind", "hostname")
  readonly_fields = (
    "kind",
    "pid",
    "hostname",
    "name",
    "metadata",
    "supervisor",
    "last_heartbeat_at",
  )
  search_fields = ("name", "kind", "hostname")

  def get_queryset(self, request):
    queryset = super().get_queryset(request)
    cutoff = timezone.now() - timedelta(
      seconds=load_backend_config(self._backend_alias(request)).process_alive_threshold
    )
    return queryset.annotate(
      live_rank=Case(
        When(last_heartbeat_at__gte=cutoff, then=Value(0)),
        default=Value(1),
        output_field=IntegerField(),
      )
    )

  @admin.display(description="status", ordering="live_rank")
  def display_status(self, obj):
    return "live" if getattr(obj, "live_rank", 1) == 0 else "stale"

  @admin.display(description="metadata")
  def metadata_json(self, obj):
    return json.dumps(obj.metadata, sort_keys=True)


@admin.register(RecurringTask)
class RecurringTaskAdmin(HiddenSidebarAdminMixin, admin.ModelAdmin):
  list_display = ("key", "task_path", "schedule", "queue_name", "priority", "static")
  list_filter = ("queue_name", "static")
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
  search_fields = ("key", "task_path", "queue_name")


@admin.register(Pause)
class PauseAdmin(HiddenSidebarAdminMixin, admin.ModelAdmin):
  list_display = ("queue_name", "created_at")
  readonly_fields = ("queue_name", "created_at")
  search_fields = ("queue_name",)


@admin.register(Semaphore)
class SemaphoreAdmin(HiddenSidebarAdminMixin, admin.ModelAdmin):
  list_display = ("key", "value", "limit", "display_blocked_waiters", "expires_at")
  readonly_fields = ("key", "value", "limit", "expires_at", "created_at", "updated_at")
  search_fields = ("key",)

  def get_queryset(self, request):
    queryset = super().get_queryset(request)
    alias = self._backend_database_alias(request)
    blocked_waiters = (
      BlockedExecution.objects.using(alias)
      .filter(concurrency_key=OuterRef("key"))
      .values("concurrency_key")
      .annotate(total=Count("id"))
      .values("total")[:1]
    )
    return queryset.annotate(
      blocked_waiter_count=Coalesce(
        Subquery(blocked_waiters, output_field=IntegerField()),
        Value(0),
      )
    )

  @admin.display(description="blocked waiters", ordering="blocked_waiter_count")
  def display_blocked_waiters(self, obj):
    return obj.blocked_waiter_count
