import time

from django.tasks import task


@task
def echo(value=None):
  return value


@task
def add(left, right):
  return left + right


@task
def fail(value=None):
  raise ValueError(value or "boom")


@task(queue_name="other")
def other_queue(value):
  return value


@task
def limited(account_id, value=None):
  return value


limited.func.concurrency_key = "account:{account_id}"
limited.func.concurrency_limit = 1
limited.func.concurrency_duration = 60


@task
def limited_discard(account_id, value=None):
  return value


limited_discard.func.concurrency_key = "account:{account_id}"
limited_discard.func.concurrency_limit = 1
limited_discard.func.concurrency_duration = 60
limited_discard.func.on_conflict = "discard"


@task
async def async_echo(value=None):
  return value


@task(takes_context=True)
def with_context(context, value=None):
  return {
    "job_id": context.task_result.id,
    "attempt": context.attempt,
    "value": value,
  }


@task
def sleep_for(seconds):
  time.sleep(seconds)
  return seconds
