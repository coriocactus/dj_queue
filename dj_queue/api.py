from functools import partial

from django.db.models.functions import Coalesce
from django.db import transaction
from django.utils import timezone
from django.utils.module_loading import import_string

from dj_queue.db import get_database_alias
from dj_queue.models import Pause, ReadyExecution


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
      self._ready_queryset()
      .annotate(latency_at=Coalesce("latency_started_at", "created_at"))
      .order_by("latency_at", "created_at")
      .values_list("latency_at", flat=True)
      .first()
    )
    if oldest is None:
      return 0.0
    return (timezone.now() - oldest).total_seconds()

  @property
  def paused(self):
    alias = get_database_alias(self.backend_alias)
    return (
      Pause.objects.using(alias)
      .filter(
        backend_alias=self.backend_alias,
        queue_name=self.queue_name,
      )
      .exists()
    )

  def pause(self):
    pause_queue = import_string("dj_queue.operations.queues.pause_queue")
    pause_queue(self.queue_name, backend_alias=self.backend_alias)

  def resume(self):
    resume_queue = import_string("dj_queue.operations.queues.resume_queue")
    resume_queue(self.queue_name, backend_alias=self.backend_alias)

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
      .filter(backend_alias=backend_alias)
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
    return ReadyExecution.objects.using(alias).filter(
      backend_alias=self.backend_alias,
      queue_name=self.queue_name,
    )


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
  operation = import_string("dj_queue.operations.recurring.schedule_recurring_task")
  return operation(
    key=key,
    task_path=task_path,
    schedule=schedule,
    args=args,
    kwargs=kwargs,
    queue_name=queue_name,
    priority=priority,
    description=description,
    backend_alias=backend_alias,
  )


def unschedule_recurring_task(key, *, backend_alias="default"):
  operation = import_string("dj_queue.operations.recurring.unschedule_recurring_task")
  return operation(key, backend_alias=backend_alias)
