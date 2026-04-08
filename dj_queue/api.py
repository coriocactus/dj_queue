from functools import partial

from django.db import transaction


def enqueue_on_commit(task, *args, using=None, **kwargs):
  transaction.on_commit(partial(task.enqueue, *args, **kwargs), using=using)
