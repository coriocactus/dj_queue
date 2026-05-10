from functools import partial

from django.db.models.functions import Coalesce
from django.db import transaction
from django.utils import timezone

from dj_queue.db import get_database_alias
from dj_queue.models import Pause, ReadyExecution
from dj_queue.operations.jobs import (
  ClaimedJob,
  claim_ready_jobs,
  discard_blocked_jobs,
  discard_failed_job,
  discard_failed_jobs,
  discard_ready_jobs,
  discard_scheduled_jobs,
  execute_claimed_job,
  retry_failed_job,
  retry_failed_jobs,
)
from dj_queue.operations.queues import pause_queue, resume_queue
from dj_queue.operations.recurring import schedule_recurring_task, unschedule_recurring_task

__all__ = [
  "ClaimedJob",
  "QueueInfo",
  "claim_ready_jobs",
  "discard_blocked_jobs",
  "discard_failed_job",
  "discard_failed_jobs",
  "discard_ready_jobs",
  "discard_scheduled_jobs",
  "enqueue_on_commit",
  "execute_claimed_job",
  "retry_failed_job",
  "retry_failed_jobs",
  "schedule_recurring_task",
  "unschedule_recurring_task",
]


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
    pause_queue(self.queue_name, backend_alias=self.backend_alias)

  def resume(self):
    resume_queue(self.queue_name, backend_alias=self.backend_alias)

  def clear(self, *, batch_size=500):
    deleted = 0
    while True:
      job_ids = list(self._ready_queryset().values_list("job_id", flat=True)[:batch_size])
      if not job_ids:
        return deleted
      deleted += discard_ready_jobs(
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


def enqueue_on_commit(task, *args, using=None, **kwargs):
  transaction.on_commit(partial(task.enqueue, *args, **kwargs), using=using)
