import uuid

from django.core.exceptions import ObjectDoesNotExist
from django.db import models
from django.db.models import Q
from django.utils.module_loading import import_string

from dj_queue.db import get_database_alias
from dj_queue.exceptions import UndiscardableError

JOB_STATUS_RELATIONS = (
  ("ready", "ready_execution"),
  ("scheduled", "scheduled_execution"),
  ("claimed", "claimed_execution"),
  ("blocked", "blocked_execution"),
  ("failed", "failed_execution"),
)


class JobQuerySet(models.QuerySet):
  def ready(self):
    return self.filter(ready_execution__isnull=False)

  def scheduled(self):
    return self.filter(scheduled_execution__isnull=False)

  def claimed(self):
    return self.filter(claimed_execution__isnull=False)

  def blocked(self):
    return self.filter(blocked_execution__isnull=False)

  def failed(self):
    return self.filter(failed_execution__isnull=False)

  def finished(self):
    return self.filter(finished_at__isnull=False)


class Job(models.Model):
  objects = JobQuerySet.as_manager()

  id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
  task_path = models.TextField()
  queue_name = models.CharField(max_length=64, default="default")
  priority = models.SmallIntegerField(default=0)
  payload = models.JSONField(default=dict)
  backend_alias = models.CharField(max_length=64)
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
      models.Index(fields=["backend_alias", "finished_at", "id"]),
    ]

  @property
  def status(self):
    if self.finished_at is not None:
      return "finished"

    for status_name, relation_name in JOB_STATUS_RELATIONS:
      if self._has_state_relation(relation_name):
        return status_name
    return None

  @property
  def ready(self):
    return self.status == "ready"

  @property
  def scheduled(self):
    return self.status == "scheduled"

  @property
  def claimed(self):
    return self.status == "claimed"

  @property
  def blocked(self):
    return self.status == "blocked"

  @property
  def failed(self):
    return self.status == "failed"

  @property
  def finished(self):
    return self.status == "finished"

  def _has_state_relation(self, relation_name):
    try:
      getattr(self, relation_name)
    except ObjectDoesNotExist:
      return False
    return True


class ReadyExecution(models.Model):
  job = models.OneToOneField(
    Job,
    on_delete=models.CASCADE,
    related_name="ready_execution",
  )
  backend_alias = models.CharField(max_length=64)
  queue_name = models.CharField(max_length=64)
  priority = models.SmallIntegerField()
  created_at = models.DateTimeField(auto_now_add=True)
  latency_started_at = models.DateTimeField(null=True, blank=True)

  class Meta:
    db_table = "dj_queue_ready_executions"
    indexes = [
      models.Index(
        fields=["backend_alias", "priority", "id"], name="dj_queue_re_backend_prio_idx"
      ),
      models.Index(
        fields=["backend_alias", "queue_name", "priority", "id"],
        name="dj_queue_re_backend_queue_idx",
      ),
      models.Index(
        fields=["backend_alias", "-priority", "id"],
        name="dj_queue_re_backend_prio_desc_idx",
      ),
      models.Index(
        fields=["backend_alias", "queue_name", "-priority", "id"],
        name="dj_queue_re_backend_queue_desc_idx",
      ),
    ]

  @classmethod
  def discard_all_in_batches(cls, *, batch_size=500, backend_alias="default"):
    operation = import_string("dj_queue.operations.jobs.discard_ready_jobs")
    return _discard_jobs_for_state(
      cls,
      operation,
      batch_size=batch_size,
      backend_alias=backend_alias,
    )


class ScheduledExecution(models.Model):
  job = models.OneToOneField(
    Job,
    on_delete=models.CASCADE,
    related_name="scheduled_execution",
  )
  backend_alias = models.CharField(max_length=64)
  queue_name = models.CharField(max_length=64)
  priority = models.SmallIntegerField()
  scheduled_at = models.DateTimeField()
  created_at = models.DateTimeField(auto_now_add=True)

  class Meta:
    db_table = "dj_queue_scheduled_executions"
    indexes = [
      models.Index(
        fields=["backend_alias", "scheduled_at", "priority", "id"],
        name="dj_queue_se_backend_due_idx",
      ),
      models.Index(
        fields=["backend_alias", "scheduled_at", "-priority", "id"],
        name="dj_queue_se_backend_due_desc_idx",
      ),
    ]


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

  @classmethod
  def discard_all_in_batches(cls, **_kwargs):
    raise UndiscardableError("cannot discard in-progress jobs")


class BlockedExecution(models.Model):
  job = models.OneToOneField(
    Job,
    on_delete=models.CASCADE,
    related_name="blocked_execution",
  )
  backend_alias = models.CharField(max_length=64)
  queue_name = models.CharField(max_length=64)
  priority = models.SmallIntegerField()
  concurrency_key = models.CharField(max_length=255)
  expires_at = models.DateTimeField()
  created_at = models.DateTimeField(auto_now_add=True)

  class Meta:
    db_table = "dj_queue_blocked_executions"
    indexes = [
      models.Index(
        fields=["backend_alias", "concurrency_key", "priority", "id"],
        name="dj_queue_bl_backend_conc_idx",
      ),
      models.Index(
        fields=["backend_alias", "expires_at", "concurrency_key"],
        name="dj_queue_bl_backend_exp_conc_idx",
      ),
      models.Index(
        fields=["backend_alias", "concurrency_key", "-priority", "id"],
        name="dj_queue_bl_backend_conc_desc_idx",
      ),
      models.Index(
        fields=["backend_alias", "expires_at", "-priority", "id"],
        name="dj_queue_bl_backend_exp_desc_idx",
      ),
    ]

  @classmethod
  def discard_all_in_batches(cls, *, batch_size=500, backend_alias="default"):
    operation = import_string("dj_queue.operations.jobs.discard_blocked_jobs")
    return _discard_jobs_for_state(
      cls,
      operation,
      batch_size=batch_size,
      backend_alias=backend_alias,
    )


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
    indexes = [models.Index(fields=["created_at", "job"])]

  def retry(self):
    return _retry_failed_job(self.job_id, backend_alias=self.job.backend_alias)

  def discard(self):
    return _discard_failed_job(self.job_id, backend_alias=self.job.backend_alias)

  @classmethod
  def retry_all(cls, queryset):
    retried = 0
    for execution in queryset.select_related("job"):
      execution.retry()
      retried += 1
    return retried

  @classmethod
  def discard_all_in_batches(cls, *, batch_size=500, backend_alias="default"):
    operation = import_string("dj_queue.operations.jobs.discard_failed_jobs")
    return _discard_jobs_for_state(
      cls,
      operation,
      batch_size=batch_size,
      backend_alias=backend_alias,
    )


def _retry_failed_job(job_id, *, backend_alias):
  operation = import_string("dj_queue.operations.jobs.retry_failed_job")
  return operation(job_id, backend_alias=backend_alias)


def _discard_failed_job(job_id, *, backend_alias):
  operation = import_string("dj_queue.operations.jobs.discard_failed_job")
  return operation(job_id, backend_alias=backend_alias)


def _discard_jobs_for_state(model, operation, *, batch_size, backend_alias):
  alias = get_database_alias(backend_alias)
  deleted = 0
  while True:
    filter_kwargs = (
      {"backend_alias": backend_alias}
      if any(field.name == "backend_alias" for field in model._meta.fields)
      else {"job__backend_alias": backend_alias}
    )
    job_ids = list(
      model.objects.using(alias)
      .filter(**filter_kwargs)
      .values_list("job_id", flat=True)[:batch_size]
    )
    if not job_ids:
      return deleted
    deleted += operation(job_ids=job_ids, batch_size=batch_size, backend_alias=backend_alias)
