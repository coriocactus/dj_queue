from datetime import timedelta
from functools import partial

from django.db import transaction
from django.utils import timezone

from dj_queue import observability
from dj_queue.config import load_backend_config
from dj_queue.db import get_database_alias
from dj_queue.models import ReadyExecution
from dj_queue.operations.jobs import (
  ClaimedJob,
  claim_ready_jobs,
  discard_blocked_jobs,
  discard_failed_job,
  discard_failed_jobs,
  discard_ready_jobs_for_queue,
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
    paused = observability.queue_is_paused(
      backend_alias=self.backend_alias,
      queue_name=self.queue_name,
    )
    if paused:
      return None

    latency = observability.queue_latency_seconds(
      backend_alias=self.backend_alias,
      queue_name=self.queue_name,
      paused=False,
    )
    return 0.0 if latency is None else latency

  @property
  def paused(self):
    return observability.queue_is_paused(
      backend_alias=self.backend_alias,
      queue_name=self.queue_name,
    )

  def pause(self):
    pause_queue(self.queue_name, backend_alias=self.backend_alias)

  def resume(self):
    resume_queue(self.queue_name, backend_alias=self.backend_alias)

  def clear(self, *, batch_size=500):
    deleted = 0
    while True:
      batch_deleted = discard_ready_jobs_for_queue(
        self.queue_name,
        batch_size=batch_size,
        backend_alias=self.backend_alias,
      )
      if not batch_deleted:
        return deleted
      deleted += batch_deleted

  @classmethod
  def all(cls, *, backend_alias="default"):
    now = timezone.now()
    config = load_backend_config(backend_alias)
    process_cutoff = now - timedelta(seconds=config.process_alive_threshold)
    queue_rows = observability.queue_rows(
      backend_alias=backend_alias,
      now=now,
      process_cutoff=process_cutoff,
    )
    return [cls(row["name"], backend_alias=backend_alias) for row in queue_rows]

  def _ready_queryset(self):
    alias = get_database_alias(self.backend_alias)
    return ReadyExecution.objects.using(alias).filter(
      backend_alias=self.backend_alias,
      queue_name=self.queue_name,
    )


def enqueue_on_commit(task, *args, using=None, **kwargs):
  transaction.on_commit(partial(task.enqueue, *args, **kwargs), using=using)
