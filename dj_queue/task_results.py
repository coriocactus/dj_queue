from django.tasks import TaskResult, TaskResultStatus
from django.tasks.base import TaskError
from django.utils.module_loading import import_string


def task_result_from_job(job):
  task = task_for_job(job)

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

  return build_task_result(
    task=task,
    job=job,
    status=status,
    started_at=started_at,
    finished_at=finished_at,
    last_attempted_at=last_attempted_at,
    errors=errors,
    worker_ids=worker_ids,
  )


def task_result_from_enqueued_job(job, task, *, successful=False):
  task = bind_task_to_job(task, job)
  status = TaskResultStatus.SUCCESSFUL if successful else TaskResultStatus.READY
  finished_at = job.finished_at if successful else None
  return build_task_result(task=task, job=job, status=status, finished_at=finished_at)


def task_result_for_claimed_job(task, claimed_job):
  return build_task_result(
    task=task,
    job=claimed_job.job,
    status=TaskResultStatus.RUNNING,
    started_at=claimed_job.claimed_at,
    last_attempted_at=claimed_job.claimed_at,
    worker_ids=claimed_job.worker_ids,
  )


def task_for_job(job):
  return bind_task_to_job(import_string(job.task_path), job)


def bind_task_to_job(task, job):
  if hasattr(task, "using"):
    return task.using(
      priority=job.priority,
      queue_name=job.queue_name,
      run_after=job.scheduled_at,
      backend=job.backend_alias,
    )
  return task


def build_task_result(
  *,
  task,
  job,
  status,
  started_at=None,
  finished_at=None,
  last_attempted_at=None,
  errors=(),
  worker_ids=(),
):
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
    backend=job.backend_alias,
    errors=list(errors),
    worker_ids=list(worker_ids),
  )
  if status == TaskResultStatus.SUCCESSFUL:
    object.__setattr__(result, "_return_value", job.return_value)
  return result
