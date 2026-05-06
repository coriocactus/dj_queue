import time

from django.tasks import task


@task
def noop(value=None):
  return value


@task
def tiny_cpu(iterations=100):
  total = 0
  for index in range(iterations):
    total += index
  return total


@task
def tiny_sleep(seconds=0.001):
  time.sleep(seconds)
  return seconds


@task
def limited(account_id, value=None):
  return value


limited.func.concurrency_key = "account:{account_id}"
limited.func.concurrency_limit = 1
limited.func.concurrency_duration = 60
