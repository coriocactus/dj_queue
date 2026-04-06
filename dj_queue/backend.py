from django.tasks.backends.base import BaseTaskBackend


class DjQueueBackend(BaseTaskBackend):
  supports_async_task = True
  supports_defer = True
  supports_get_result = True
  supports_priority = True

  def enqueue(self, task, args, kwargs):
    raise NotImplementedError("DjQueueBackend.enqueue() is not implemented yet")

  def get_result(self, result_id):
    raise NotImplementedError("DjQueueBackend.get_result() is not implemented yet")
