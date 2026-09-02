from collections.abc import Callable, Mapping
from functools import partial
from typing import Any, Literal, Self, TypeVar

from django.db import transaction
from django.tasks import Task

from dj_queue import observability
from dj_queue.operations.claiming import ClaimedJob, claim_ready_jobs
from dj_queue.operations.jobs import (
  discard_blocked_jobs,
  discard_failed_job,
  discard_failed_jobs,
  discard_ready_jobs,
  discard_ready_jobs_for_queue,
  discard_scheduled_jobs,
  execute_claimed_job,
  retry_failed_job,
  retry_failed_jobs,
  schedule_failed_job_retry,
)
from dj_queue.operations.queues import pause_queue, resume_queue
from dj_queue.operations.recurring import schedule_recurring_task, unschedule_recurring_task

__all__ = [
  "ClaimedJob",
  "QueueInfo",
  "claim_ready_jobs",
  "concurrency",
  "discard_blocked_jobs",
  "discard_failed_job",
  "discard_failed_jobs",
  "discard_ready_jobs",
  "discard_scheduled_jobs",
  "enqueue_on_commit",
  "execute_claimed_job",
  "retry_failed_job",
  "retry_failed_jobs",
  "schedule_failed_job_retry",
  "schedule_recurring_task",
  "unschedule_recurring_task",
]

_Decorated = TypeVar("_Decorated")


def concurrency(
  *,
  key: str | Callable[..., str],
  limit: int,
  duration: int | None = None,
  on_conflict: Literal["block", "discard"] = "block",
) -> Callable[[_Decorated], _Decorated]:
  def decorator(target: _Decorated) -> _Decorated:
    func = getattr(target, "func", target)
    func.concurrency_key = key
    func.concurrency_limit = limit
    if duration is not None:
      func.concurrency_duration = duration
    func.on_conflict = on_conflict
    return target

  return decorator


class QueueInfo:
  def __init__(
    self,
    queue_name: str,
    *,
    backend_alias: str = "default",
    snapshot: Mapping[str, Any] | None = None,
  ) -> None:
    self.queue_name = queue_name
    self.backend_alias = backend_alias
    self._snapshot = snapshot

  @property
  def size(self) -> int:
    if self._snapshot is not None:
      return self._snapshot["ready_count"]
    return observability.queue_ready_count(
      backend_alias=self.backend_alias,
      queue_name=self.queue_name,
    )

  @property
  def latency(self) -> float | None:
    if self._snapshot is not None:
      latency = self._snapshot["latency_seconds"]
      if latency is None and not self._snapshot["paused"]:
        return 0.0
      return latency

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
  def paused(self) -> bool:
    if self._snapshot is not None:
      return self._snapshot["paused"]
    return observability.queue_is_paused(
      backend_alias=self.backend_alias,
      queue_name=self.queue_name,
    )

  def pause(self) -> None:
    pause_queue(self.queue_name, backend_alias=self.backend_alias)
    self._snapshot = None

  def resume(self) -> None:
    resume_queue(self.queue_name, backend_alias=self.backend_alias)
    self._snapshot = None

  def clear(self, *, batch_size: int = 500) -> int:
    deleted = 0
    while True:
      batch_deleted = discard_ready_jobs_for_queue(
        self.queue_name,
        batch_size=batch_size,
        backend_alias=self.backend_alias,
      )
      if not batch_deleted:
        self._snapshot = None
        return deleted
      deleted += batch_deleted

  @classmethod
  def all(cls, *, backend_alias: str = "default") -> list[Self]:
    queue_rows = observability.queue_rows_for_backend(backend_alias=backend_alias)
    return [cls(row["name"], backend_alias=backend_alias, snapshot=row) for row in queue_rows]


def enqueue_on_commit(
  task: Task,
  *args: Any,
  using: str | None = None,
  **kwargs: Any,
) -> None:
  transaction.on_commit(partial(task.enqueue, *args, **kwargs), using=using)
