from datetime import timedelta

import pytest
from django.utils import timezone

from dj_queue.config import load_backend_config
from dj_queue.models import BlockedExecution, ClaimedExecution, ReadyExecution, Semaphore
from dj_queue.operations.claiming import claim_ready_jobs
from dj_queue.operations.concurrency import (
  promote_expired_blocked_jobs,
  release_concurrency_slot,
  release_recovered_concurrency_slots,
  semaphore_acquire,
  semaphore_acquire_many,
  semaphore_release,
)
from dj_queue.operations.execution import complete_claimed_job
from tests.factories import make_blocked_job, make_job
from tests.tasks import limited

pytestmark = pytest.mark.django_db(transaction=True)

KEY = "account:1"


def acquire(limit):
  return semaphore_acquire(KEY, limit=limit, duration_seconds=60, config=load_backend_config())


def acquire_grouped(limit, count=1):
  return semaphore_acquire_many(
    KEY, count=count, limit=limit, duration_seconds=60, config=load_backend_config()
  )


def release():
  return semaphore_release(KEY, duration_seconds=60, config=load_backend_config())


def hold(value, limit):
  Semaphore.objects.create(
    key=KEY, value=value, limit=limit, expires_at=timezone.now() + timedelta(minutes=1)
  )


def slots():
  return Semaphore.objects.values_list("value", "limit").get(key=KEY)


def policy(limit):
  return {
    "task": limited,
    "concurrency_key": KEY,
    "concurrency_limit": limit,
    "concurrency_duration": 60,
    "concurrency_on_conflict": "block",
  }


def waiter(limit, *, expired=False):
  expires_at = timezone.now() + timedelta(minutes=-1 if expired else 1)
  job = make_blocked_job(expires_at=expires_at, **policy(limit))
  return BlockedExecution.objects.get(job=job)


def holders():
  return (
    ReadyExecution.objects.filter(job__concurrency_key=KEY).count()
    + ClaimedExecution.objects.filter(job__concurrency_key=KEY).count()
  )


@pytest.mark.parametrize("take", [acquire, acquire_grouped])
def test_lowered_limit_holds_until_the_old_jobs_release(take):
  assert [take(3) for _ in range(3)] == [True] * 3
  held = Semaphore.objects.values().get(key=KEY)

  assert not take(1)
  assert Semaphore.objects.values().get(key=KEY) == held

  release()
  assert not take(1)
  assert slots() == (1, 3)

  release()
  release()
  assert take(1)
  assert slots() == (0, 1)


@pytest.mark.parametrize("take", [acquire, acquire_grouped])
def test_failed_acquire_at_a_lower_limit_forgets_no_running_job(take):
  assert [take(3) for _ in range(3)] == [True] * 3

  assert not take(1)
  assert not take(3)
  assert slots() == (0, 3)


def test_raised_limit_admits_on_the_next_acquire():
  assert acquire(1)

  assert acquire(3)
  assert slots() == (1, 3)
  assert acquire_grouped(5, count=9) == 3
  assert slots() == (0, 5)


def test_completions_at_the_old_limit_add_no_phantom_credit(monkeypatch):
  config = load_backend_config()
  monkeypatch.setattr(limited.func, "concurrency_limit", 3)
  running = [limited.enqueue(1) for _ in range(3)]
  claim_ready_jobs(limit=3, config=config)
  monkeypatch.setattr(limited.func, "concurrency_limit", 1)
  blocked = limited.enqueue(1)
  assert slots() == (0, 3)

  for result in running:
    complete_claimed_job(result.id, None, config=config)
    value, limit = slots()
    assert limit - value == holders()

  assert slots() == (0, 1)
  assert ReadyExecution.objects.filter(job_id=blocked.id).exists()


def test_handoff_waits_until_occupancy_is_below_the_waiter_limit():
  hold(0, 3)
  blocked = waiter(2)

  release_concurrency_slot(make_job(**policy(3)), config=load_backend_config())
  assert slots() == (1, 3)
  assert BlockedExecution.objects.get() == blocked

  release_concurrency_slot(make_job(**policy(3)), config=load_backend_config())
  assert slots() == (0, 2)
  assert ReadyExecution.objects.get().job_id == blocked.job_id


def test_handoff_sets_the_row_limit_to_the_waiter_limit():
  hold(0, 1)
  blocked = waiter(2)

  release_concurrency_slot(make_job(**policy(1)), config=load_backend_config())

  assert slots() == (1, 2)
  assert ReadyExecution.objects.get().job_id == blocked.job_id


def test_handoff_from_an_idle_row_counts_the_waiter():
  hold(1, 1)
  blocked = waiter(1)

  release_concurrency_slot(make_job(**policy(1)), config=load_backend_config())

  assert slots() == (0, 1)
  assert ReadyExecution.objects.get().job_id == blocked.job_id


def test_recovered_release_admits_waiters_at_their_own_limits():
  hold(0, 3)
  recovered = [make_job(**policy(3)) for _ in range(3)]
  first, second = waiter(1), waiter(1)

  assert release_recovered_concurrency_slots(recovered[:1], config=load_backend_config()) == []
  assert slots() == (1, 3)
  assert not ReadyExecution.objects.exists()

  assert release_recovered_concurrency_slots(recovered[1:], config=load_backend_config()) == []
  assert slots() == (0, 1)
  assert ReadyExecution.objects.get().job_id == first.job_id
  assert BlockedExecution.objects.get() == second


def test_expired_waiter_that_cannot_acquire_leaves_the_semaphore_untouched():
  hold(1, 3)
  held = Semaphore.objects.values().get(key=KEY)
  waiter(1, expired=True)

  assert promote_expired_blocked_jobs(config=load_backend_config()) == []

  assert Semaphore.objects.values().get(key=KEY) == held
  assert BlockedExecution.objects.get().expires_at > timezone.now()
