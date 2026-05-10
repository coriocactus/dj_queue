import json

from dj_queue.db import database_capabilities
from dj_queue.exceptions import EnqueueError
from dj_queue.models import Pause


def _normalize_payload(args, kwargs):
  try:
    return json.loads(json.dumps({"args": list(args), "kwargs": dict(kwargs)}))
  except (TypeError, ValueError) as exc:
    raise EnqueueError("payload must be JSON round-trippable") from exc


def _task_option(task, name, default=None):
  if hasattr(task, name):
    return getattr(task, name)
  return getattr(task.func, name, default)


def _lock_active_pauses(alias, backend_alias, queue_names=None):
  queryset = Pause.objects.using(alias).select_for_update().filter(backend_alias=backend_alias)
  if queue_names is not None:
    active_queue_names = tuple(queue_name for queue_name in queue_names if queue_name)
    if not active_queue_names:
      return set()
    queryset = queryset.filter(queue_name__in=active_queue_names)
  return set(queryset.values_list("queue_name", flat=True))


def _consume_selected_rows(alias, model, rows):
  if not database_capabilities(alias).uses_serialized_writes:
    model.objects.using(alias).filter(pk__in=[row.pk for row in rows]).delete()
    return rows

  consumed_rows = []
  for row in rows:
    deleted, _ = model.objects.using(alias).filter(pk=row.pk).delete()
    if deleted:
      consumed_rows.append(row)
  return consumed_rows
