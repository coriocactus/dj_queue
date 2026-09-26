from django.core.exceptions import PermissionDenied

ACTION_PERMISSIONS = {
  "enqueue": "dj_queue.add_job",
  "enqueue_copy_now": "dj_queue.add_job",
  "run_now": "dj_queue.change_job",
  "retry": "dj_queue.change_job",
  "discard": "dj_queue.delete_job",
  "clear": "dj_queue.delete_job",
  "pause": "dj_queue.add_pause",
  "resume": "dj_queue.delete_pause",
  "unschedule": "dj_queue.delete_recurringtask",
}


def has_action_permission(user, action):
  permission = ACTION_PERMISSIONS.get(action)
  return bool(permission and user.is_active and user.is_staff and user.has_perm(permission))


def require_action_permission(user, action):
  if not has_action_permission(user, action):
    raise PermissionDenied


def permitted_actions(user, actions):
  return tuple(action for action in actions if has_action_permission(user, action["name"]))
