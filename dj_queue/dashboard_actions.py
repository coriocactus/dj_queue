from dj_queue.api import QueueInfo
from dj_queue.operations.jobs import (
  discard_blocked_jobs,
  discard_failed_jobs,
  discard_ready_jobs,
  discard_scheduled_jobs,
  enqueue_job_again,
  retry_failed_jobs,
)
from dj_queue.queue_state import queue_state_queryset

QUEUE_JOB_ACTIONS = {
  "ready": ({"name": "discard", "label": "discard selected"},),
  "claimed": (),
  "scheduled": ({"name": "discard", "label": "discard selected"},),
  "blocked": ({"name": "discard", "label": "discard selected"},),
  "failed": (
    {"name": "retry", "label": "retry selected"},
    {"name": "discard", "label": "discard selected"},
  ),
  "finished": ({"name": "enqueue", "label": "enqueue selected again"},),
  "invalid": (),
}
DISCARD_OPERATIONS = {
  "ready": discard_ready_jobs,
  "scheduled": discard_scheduled_jobs,
  "blocked": discard_blocked_jobs,
  "failed": discard_failed_jobs,
}


def apply_queue_action(*, backend_alias, queue_name, action):
  queue_info = QueueInfo(queue_name, backend_alias=backend_alias)
  if action == "pause":
    queue_info.pause()
    return f"paused queue {queue_name}"
  if action == "resume":
    queue_info.resume()
    return f"resumed queue {queue_name}"
  if action == "clear":
    deleted = queue_info.clear()
    return f"cleared {deleted} ready jobs from {queue_name}"
  raise ValueError(f"unsupported queue action {action!r}")


def apply_job_action(*, backend_alias, queue_name, state, action, job_ids):
  if not action:
    raise ValueError("No action selected.")
  if not job_ids:
    raise ValueError("select at least one job")
  if action not in {entry["name"] for entry in QUEUE_JOB_ACTIONS.get(state, ())}:
    raise ValueError(f"unsupported {state!r} job action {action!r}")

  job_ids = list(
    queue_state_queryset(backend_alias=backend_alias, queue_name=queue_name, state=state)
    .filter(pk__in=job_ids)
    .values_list("pk", flat=True)
  )
  if action == "enqueue":
    for job_id in job_ids:
      enqueue_job_again(job_id, backend_alias=backend_alias)
    return f"enqueued {len(job_ids)} finished jobs again from {queue_name}"

  operation = retry_failed_jobs if action == "retry" else DISCARD_OPERATIONS[state]
  count = operation(job_ids=job_ids, batch_size=max(len(job_ids), 1), backend_alias=backend_alias)
  verb = "retried" if action == "retry" else "discarded"
  return f"{verb} {count} {state} jobs from {queue_name}"


def job_actions_for_state(state):
  return QUEUE_JOB_ACTIONS[state]
