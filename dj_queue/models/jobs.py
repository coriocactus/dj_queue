import uuid
from itertools import combinations

from django.core.exceptions import ObjectDoesNotExist
from django.db import models
from django.db.models import Q

JOB_STATUS_RELATIONS = (
  ("ready", "ready_execution"),
  ("scheduled", "scheduled_execution"),
  ("claimed", "claimed_execution"),
  ("blocked", "blocked_execution"),
  ("failed", "failed_execution"),
)
JOB_STATUS_RELATION_BY_NAME = dict(JOB_STATUS_RELATIONS)
INVALID_JOB_STATUS = "invalid"


class JobQuerySet(models.QuerySet):
  def ready(self):
    return self._valid_state("ready")

  def scheduled(self):
    return self._valid_state("scheduled")

  def claimed(self):
    return self._valid_state("claimed")

  def blocked(self):
    return self._valid_state("blocked")

  def failed(self):
    return self._valid_state("failed")

  def finished(self):
    return self._valid_state("finished")

  def invalid_execution_state(self):
    return self.filter(invalid_execution_state_query())

  def _valid_state(self, status):
    return self.filter(**job_status_query_filter(status)).exclude(invalid_execution_state_query())


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
      models.Index(fields=["backend_alias", "queue_name", "id"], name="djq_jobs_b_queue_id_idx"),
      models.Index(fields=["backend_alias", "concurrency_key"], name="djq_jobs_b_conc_idx"),
    ]

  @property
  def status(self):
    live_state_names = self.live_execution_state_names
    if len(live_state_names) > 1:
      return INVALID_JOB_STATUS
    if self.finished_at is not None:
      if live_state_names:
        return INVALID_JOB_STATUS
      return "finished"

    if live_state_names:
      return live_state_names[0]
    return None

  @property
  def live_execution_state_names(self):
    return tuple(
      status_name
      for status_name, relation_name in JOB_STATUS_RELATIONS
      if self._has_state_relation(relation_name)
    )

  @property
  def execution_state_names(self):
    live_state_names = self.live_execution_state_names
    if self.finished_at is None:
      return live_state_names
    return ("finished", *live_state_names)

  @property
  def has_invalid_execution_state(self):
    return self.status == INVALID_JOB_STATUS

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


def invalid_execution_state_query():
  relation_names = job_status_relation_names()
  query = None
  for first_relation, second_relation in combinations(relation_names, 2):
    query = _or_query(
      query,
      Q(
        **{
          f"{first_relation}__isnull": False,
          f"{second_relation}__isnull": False,
        }
      ),
    )
  for relation_name in relation_names:
    query = _or_query(
      query,
      Q(finished_at__isnull=False, **{f"{relation_name}__isnull": False}),
    )
  return query or Q(pk__in=[])


def job_status_relation_names():
  return tuple(relation_name for _status_name, relation_name in JOB_STATUS_RELATIONS)


def job_status_query_filter(status):
  if status == "finished":
    return {"finished_at__isnull": False}
  return {f"{JOB_STATUS_RELATION_BY_NAME[status]}__isnull": False}


def _or_query(current, next_query):
  if current is None:
    return next_query
  return current | next_query


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
        fields=["backend_alias", "-priority", "id"],
        name="djq_re_b_prio_d_idx",
      ),
      models.Index(
        fields=["backend_alias", "queue_name", "-priority", "id"],
        name="djq_re_b_queue_d_idx",
      ),
    ]


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
        fields=["backend_alias", "scheduled_at", "-priority", "id"],
        name="djq_se_b_due_d_idx",
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
        fields=["backend_alias", "concurrency_key", "-priority", "id"],
        name="djq_bl_b_conc_d_idx",
      ),
      models.Index(
        fields=["backend_alias", "expires_at", "-priority", "id"],
        name="djq_bl_b_exp_d_idx",
      ),
    ]


class FailedExecution(models.Model):
  job = models.OneToOneField(
    Job,
    on_delete=models.CASCADE,
    related_name="failed_execution",
  )
  exception_class = models.CharField(max_length=255)
  message = models.TextField(default="")
  traceback = models.TextField(default="")
  retry_at = models.DateTimeField(null=True, blank=True)
  created_at = models.DateTimeField(auto_now_add=True)

  class Meta:
    db_table = "dj_queue_failed_executions"
    indexes = [
      models.Index(fields=["created_at", "job"]),
      models.Index(fields=["retry_at", "job"]),
    ]
