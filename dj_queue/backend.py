from asgiref.sync import sync_to_async
from django.db import close_old_connections
from django.tasks import TaskResult, TaskResultStatus
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.base import TaskError
from django.tasks.exceptions import TaskResultDoesNotExist
from django.utils.module_loading import import_string

from dj_queue.db import get_database_alias
from dj_queue.models import Job
from dj_queue.operations.jobs import enqueue_job, enqueue_jobs_bulk


class DjQueueBackend(BaseTaskBackend):
  supports_async_task = True
  supports_defer = True
  supports_get_result = True
  supports_priority = True

  def enqueue(self, task, args, kwargs):
    self.validate_task(task)
    job = enqueue_job(task, args, kwargs, backend_alias=self.alias)
    return self.get_result(str(job.id))

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

    created_jobs = enqueue_jobs_bulk(jobs, backend_alias=self.alias)
    return [
      _task_result_from_enqueued_job(job, task, dispatched_as)
      for job, task, dispatched_as in created_jobs
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
        .get(pk=result_id)
      )
    except Job.DoesNotExist as exc:
      raise TaskResultDoesNotExist(str(result_id)) from exc

    return _task_result_from_job(job)

  async def aget_result(self, result_id):
    return await sync_to_async(_async_backend_call, thread_sensitive=True)(
      self.get_result,
      result_id=result_id,
    )


def _async_backend_call(method, /, **kwargs):
  try:
    return method(**kwargs)
  finally:
    close_old_connections()


def _task_result_from_job(job):
  task = import_string(job.task_path)
  if hasattr(task, "using"):
    task = task.using(
      priority=job.priority,
      queue_name=job.queue_name,
      run_after=job.scheduled_at,
      backend=job.backend_name,
    )

  status = TaskResultStatus.READY
  started_at = None
  finished_at = job.finished_at
  last_attempted_at = None
  errors = []
  worker_ids = []

  if job.failed:
    status = TaskResultStatus.FAILED
    finished_at = job.failed_execution.created_at
    last_attempted_at = job.failed_execution.created_at
    errors = [
      TaskError(
        exception_class_path=job.failed_execution.exception_class,
        traceback=job.failed_execution.traceback,
      )
    ]
  elif job.claimed:
    status = TaskResultStatus.RUNNING
    started_at = job.claimed_execution.created_at
    last_attempted_at = job.claimed_execution.created_at
    if job.claimed_execution.process_id is not None:
      worker_ids = [job.claimed_execution.process.name]
  elif job.finished:
    status = TaskResultStatus.SUCCESSFUL

  result = TaskResult(
    task=task,
    id=str(job.id),
    status=status,
    enqueued_at=job.created_at,
    started_at=started_at,
    finished_at=finished_at,
    last_attempted_at=last_attempted_at,
    args=job.payload.get("args", []),
    kwargs=job.payload.get("kwargs", {}),
    backend=job.backend_name,
    errors=errors,
    worker_ids=worker_ids,
  )
  if status == TaskResultStatus.SUCCESSFUL:
    object.__setattr__(result, "_return_value", job.return_value)
  return result


def _task_result_from_enqueued_job(job, task, dispatched_as):
  if hasattr(task, "using"):
    task = task.using(
      priority=job.priority,
      queue_name=job.queue_name,
      run_after=job.scheduled_at,
      backend=job.backend_name,
    )

  status = TaskResultStatus.SUCCESSFUL if dispatched_as == "discarded" else TaskResultStatus.READY
  finished_at = job.finished_at if status == TaskResultStatus.SUCCESSFUL else None

  result = TaskResult(
    task=task,
    id=str(job.id),
    status=status,
    enqueued_at=job.created_at,
    started_at=None,
    finished_at=finished_at,
    last_attempted_at=None,
    args=job.payload.get("args", []),
    kwargs=job.payload.get("kwargs", {}),
    backend=job.backend_name,
    errors=[],
    worker_ids=[],
  )
  if status == TaskResultStatus.SUCCESSFUL:
    object.__setattr__(result, "_return_value", job.return_value)
  return result
