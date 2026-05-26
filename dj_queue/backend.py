from asgiref.sync import sync_to_async
from django.db import close_old_connections, connections
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.exceptions import TaskResultDoesNotExist

from dj_queue.db import get_database_alias
from dj_queue.models import Job
from dj_queue.operations.jobs import (
  DispatchOutcome,
  enqueue_job_with_dispatch,
  enqueue_jobs_bulk,
  validate_priority,
  validate_queue_allowed,
)
from dj_queue.task_results import task_result_from_enqueued_job, task_result_from_job


class DjQueueBackend(BaseTaskBackend):
  supports_async_task = True
  supports_defer = True
  supports_get_result = True
  supports_priority = True

  def __init__(self, alias, params):
    if not params.get("QUEUES"):
      params = {**params, "QUEUES": []}
    super().__init__(alias, params)

  def validate_task(self, task):
    validate_queue_allowed(task.queue_name, backend_alias=self.alias)
    validate_priority(task.priority)
    return super().validate_task(task)

  def enqueue(self, task, args, kwargs):
    self.validate_task(task)
    job, dispatch_outcome = enqueue_job_with_dispatch(
      task,
      args,
      kwargs,
      backend_alias=self.alias,
      validate=False,
    )
    return task_result_from_enqueued_job(
      job,
      task,
      successful=dispatch_outcome is DispatchOutcome.DISCARDED,
    )

  async def aenqueue(self, task, args, kwargs):
    return await sync_to_async(_async_backend_call, thread_sensitive=True)(
      self.enqueue,
      task=task,
      args=args,
      kwargs=kwargs,
    )

  def enqueue_all(self, task_calls):
    jobs = []
    for task, args, kwargs in task_calls:
      self.validate_task(task)
      jobs.append((task, args, kwargs))

    created_jobs = enqueue_jobs_bulk(jobs, backend_alias=self.alias, validate=False)
    return [
      task_result_from_enqueued_job(
        job,
        task,
        successful=dispatch_outcome is DispatchOutcome.DISCARDED,
      )
      for job, task, dispatch_outcome in created_jobs
    ]

  def get_result(self, result_id):
    alias = get_database_alias(self.alias)
    try:
      job = (
        Job.objects.using(alias)
        .select_related(
          "ready_execution",
          "scheduled_execution",
          "claimed_execution__process",
          "blocked_execution",
          "failed_execution",
        )
        .get(pk=result_id, backend_alias=self.alias)
      )
    except Job.DoesNotExist as exc:
      raise TaskResultDoesNotExist(str(result_id)) from exc

    return task_result_from_job(job)

  async def aget_result(self, result_id):
    return await sync_to_async(_async_backend_call, thread_sensitive=True)(
      self.get_result,
      result_id=result_id,
    )


def _async_backend_call(method, /, **kwargs):
  close_old_connections()
  try:
    return method(**kwargs)
  finally:
    connections.close_all()
