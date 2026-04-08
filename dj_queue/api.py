from functools import partial

from django.db import transaction
from django.utils import timezone
from django.utils.module_loading import import_string

from dj_queue.db import get_database_alias
from dj_queue.log import log_event
from dj_queue.models import Pause, ReadyExecution, RecurringTask


class QueueInfo:
  def __init__(self, queue_name, *, backend_alias="default"):
    self.queue_name = queue_name
    self.backend_alias = backend_alias

  @property
  def size(self):
    return self._ready_queryset().count()

  @property
  def latency(self):
    oldest = (
      self._ready_queryset().order_by("created_at").values_list("created_at", flat=True).first()
    )
    if oldest is None:
      return 0.0
    return (timezone.now() - oldest).total_seconds()

  @property
  def paused(self):
    alias = get_database_alias(self.backend_alias)
    return Pause.objects.using(alias).filter(queue_name=self.queue_name).exists()

  def pause(self):
    alias = get_database_alias(self.backend_alias)
    Pause.objects.using(alias).get_or_create(queue_name=self.queue_name)
    log_event("queue.paused", backend_alias=self.backend_alias, queue_name=self.queue_name)

  def resume(self):
    alias = get_database_alias(self.backend_alias)
    deleted, _ = Pause.objects.using(alias).filter(queue_name=self.queue_name).delete()
    if deleted:
      log_event("queue.resumed", backend_alias=self.backend_alias, queue_name=self.queue_name)

  def clear(self, *, batch_size=500):
    deleted = 0
    while True:
      job_ids = list(self._ready_queryset().values_list("job_id", flat=True)[:batch_size])
      if not job_ids:
        return deleted
      deleted += _discard_ready_jobs(
        job_ids=job_ids,
        batch_size=batch_size,
        backend_alias=self.backend_alias,
      )

  @classmethod
  def all(cls, *, backend_alias="default"):
    alias = get_database_alias(backend_alias)
    queue_names = (
      ReadyExecution.objects.using(alias)
      .order_by("queue_name")
      .values_list(
        "queue_name",
        flat=True,
      )
      .distinct()
    )
    return [cls(queue_name, backend_alias=backend_alias) for queue_name in queue_names]

  def _ready_queryset(self):
    alias = get_database_alias(self.backend_alias)
    return ReadyExecution.objects.using(alias).filter(queue_name=self.queue_name)


def _discard_ready_jobs(*, job_ids, batch_size, backend_alias):
  discard_ready_jobs = import_string("dj_queue.operations.jobs.discard_ready_jobs")
  return discard_ready_jobs(job_ids=job_ids, batch_size=batch_size, backend_alias=backend_alias)


def enqueue_on_commit(task, *args, using=None, **kwargs):
  transaction.on_commit(partial(task.enqueue, *args, **kwargs), using=using)


def schedule_recurring_task(
  *,
  key,
  task_path,
  schedule,
  args=(),
  kwargs=None,
  queue_name="default",
  priority=0,
  description="",
  backend_alias="default",
):
  alias = get_database_alias(backend_alias)
  if kwargs is None:
    kwargs = {}

  recurring_task, _ = RecurringTask.objects.using(alias).update_or_create(
    key=key,
    defaults={
      "task_path": task_path,
      "payload": {"args": list(args), "kwargs": dict(kwargs)},
      "schedule": schedule,
      "queue_name": queue_name,
      "priority": priority,
      "description": description,
      "static": False,
    },
  )
  recurring_task.full_clean()
  recurring_task.save(using=alias)
  return recurring_task


def unschedule_recurring_task(key, *, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  queryset = RecurringTask.objects.using(alias).filter(key=key, static=False)
  deleted = queryset.count()
  queryset.delete()
  return deleted
