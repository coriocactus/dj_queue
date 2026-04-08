def schedule_recurring_task(*args, **kwargs):
  from dj_queue.api import schedule_recurring_task as api_schedule_recurring_task

  return api_schedule_recurring_task(*args, **kwargs)


def unschedule_recurring_task(*args, **kwargs):
  from dj_queue.api import unschedule_recurring_task as api_unschedule_recurring_task

  return api_unschedule_recurring_task(*args, **kwargs)


__all__ = ["schedule_recurring_task", "unschedule_recurring_task"]
