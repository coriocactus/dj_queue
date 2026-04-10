from datetime import datetime, timedelta
import random
from uuid import uuid4

import pytest
from django.db.models import Count
from django.utils import timezone

from dj_queue.api import schedule_recurring_task, unschedule_recurring_task
from dj_queue.config import DispatcherConfig, WorkerConfig
from dj_queue.models import (
  BlockedExecution,
  ClaimedExecution,
  FailedExecution,
  Job,
  Process,
  ReadyExecution,
  RecurringExecution,
  ScheduledExecution,
  Semaphore,
)
from dj_queue.runtime.dispatcher import Dispatcher
from dj_queue.runtime.scheduler import Scheduler
from dj_queue.runtime.worker import Worker
from tests.tasks import echo, limited

pytestmark = pytest.mark.django_db(transaction=True)


class InlinePool:
  def __init__(self, max_workers):
    self.idle_capacity = max_workers

  def submit(self, fn, *args, **kwargs):
    from concurrent.futures import Future

    future = Future()
    try:
      future.set_result(fn(*args, **kwargs))
    except Exception as exc:
      future.set_exception(exc)
    return future

  def shutdown(self, timeout, *, on_drained=None):
    if on_drained is not None:
      on_drained()
    return True


class FakeSleeper:
  def wake_up(self):
    return None


class FakeWakeupBackend:
  def start(self):
    return None

  def stop(self):
    return None


def fixed_now():
  return datetime(2026, 4, 8, 12, 0, 1, tzinfo=timezone.get_current_timezone())


def simulation_tasks_settings():
  return {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {
        "workers": [{"queues": "*", "threads": 1, "processes": 1, "polling_interval": 0.01}],
        "dispatchers": [
          {
            "batch_size": 10,
            "polling_interval": 0.01,
            "concurrency_maintenance": True,
            "concurrency_maintenance_interval": 0,
          }
        ],
        "scheduler": {
          "dynamic_tasks_enabled": True,
          "polling_interval": 1,
        },
        "recurring": {},
        "preserve_finished_jobs": True,
        "clear_finished_jobs_after": None,
      },
    }
  }


class RuntimeSimulation:
  def __init__(self, *, seed, monkeypatch):
    self.seed = seed
    self.monkeypatch = monkeypatch
    self.rng = random.Random(seed)
    self.now = fixed_now()
    self.counter = 0
    self.active_recurring_keys = set()
    self.worker = Worker(
      WorkerConfig(queues=("*",), threads=1, processes=1, polling_interval=0.01),
      backend_alias="default",
      name=f"sim-worker-{uuid4()}",
      pid=11001,
      hostname="localhost",
      sleeper=FakeSleeper(),
      pool=InlinePool(1),
      wakeup_backend=FakeWakeupBackend(),
    )
    self.dispatcher = Dispatcher(
      DispatcherConfig(
        batch_size=10,
        polling_interval=0.01,
        concurrency_maintenance=True,
        concurrency_maintenance_interval=0,
      ),
      backend_alias="default",
      name=f"sim-dispatcher-{uuid4()}",
      pid=11002,
      hostname="localhost",
    )
    self.scheduler = Scheduler.from_backend_config(
      backend_alias="default",
      tasks_settings=simulation_tasks_settings(),
      name=f"sim-scheduler-{uuid4()}",
      pid=11003,
      hostname="localhost",
    )

  def start(self):
    self.worker.start()
    self.dispatcher.start()
    self.scheduler.start()

  def stop(self):
    self.worker.stop()
    self.dispatcher.stop()
    self.scheduler.stop()

  def run(self, *, steps):
    actions = (
      self.enqueue_ready,
      self.enqueue_scheduled,
      self.enqueue_limited,
      self.schedule_recurring,
      self.unschedule_recurring,
      self.worker_tick,
      self.dispatcher_tick,
      self.scheduler_tick,
      self.advance_time,
    )

    for _ in range(steps):
      action = self.rng.choice(actions)
      action()
      self.assert_invariants()

  def drain(self):
    for key in list(self.active_recurring_keys):
      unschedule_recurring_task(key)
      self.active_recurring_keys.discard(key)

    for _ in range(60):
      self.scheduler_tick()
      self.dispatcher_tick()
      self.worker_tick()
      self.assert_invariants()
      if self._non_terminal_count() == 0:
        return
      self.now += timedelta(minutes=1)

    assert self._non_terminal_count() == 0, f"seed {self.seed} left live jobs behind"

  def enqueue_ready(self):
    echo.using(priority=self.rng.randint(-5, 5)).enqueue(self._next_value("ready"))

  def enqueue_scheduled(self):
    echo.using(
      priority=self.rng.randint(-5, 5),
      run_after=self.now + timedelta(minutes=self.rng.randint(1, 3)),
    ).enqueue(self._next_value("scheduled"))

  def enqueue_limited(self):
    limited.using(priority=self.rng.randint(-5, 5)).enqueue(
      self.rng.randint(1, 2),
      value=self._next_value("limited"),
    )

  def schedule_recurring(self):
    key = f"sim-recurring-{self.rng.randint(0, 4)}"
    schedule_recurring_task(
      key=key,
      task_path="tests.tasks.echo",
      schedule=self.rng.choice(("* * * * *", "*/2 * * * *")),
      args=(self._next_value("recurring"),),
    )
    self.active_recurring_keys.add(key)

  def unschedule_recurring(self):
    if not self.active_recurring_keys:
      return
    key = sorted(self.active_recurring_keys)[self.rng.randrange(len(self.active_recurring_keys))]
    unschedule_recurring_task(key)
    self.active_recurring_keys.discard(key)

  def worker_tick(self):
    self.worker.poll_once()

  def dispatcher_tick(self):
    with self.monkeypatch.context() as mp:
      mp.setattr("dj_queue.runtime.dispatcher.timezone.now", lambda: self.now)
      mp.setattr("dj_queue.operations.jobs.timezone.now", lambda: self.now)
      mp.setattr("dj_queue.operations.concurrency.timezone.now", lambda: self.now)
      self.dispatcher.poll_once()

  def scheduler_tick(self):
    self.scheduler.poll_once(now=self.now)

  def advance_time(self):
    self.now += timedelta(minutes=1)

  def assert_invariants(self):
    duplicate_recurring_runs = (
      RecurringExecution.objects.values("task_key", "run_at")
      .annotate(row_count=Count("id"))
      .filter(row_count__gt=1)
      .exists()
    )
    assert duplicate_recurring_runs is False, f"seed {self.seed} created duplicate recurring runs"

    for semaphore in Semaphore.objects.all():
      assert 0 <= semaphore.value <= semaphore.limit, (
        f"seed {self.seed} invalid semaphore {semaphore.key}={semaphore.value}/{semaphore.limit}"
      )

    for job in Job.objects.order_by("id"):
      state_count = sum(
        (
          ReadyExecution.objects.filter(job_id=job.id).exists(),
          ScheduledExecution.objects.filter(job_id=job.id).exists(),
          ClaimedExecution.objects.filter(job_id=job.id).exists(),
          BlockedExecution.objects.filter(job_id=job.id).exists(),
          FailedExecution.objects.filter(job_id=job.id).exists(),
          job.finished_at is not None,
        )
      )
      assert state_count == 1, f"seed {self.seed} job {job.id} has {state_count} states"

    assert Process.objects.count() == 3, f"seed {self.seed} lost a runtime process row"

  def _next_value(self, prefix):
    value = f"{prefix}-{self.seed}-{self.counter:03d}"
    self.counter += 1
    return value

  def _non_terminal_count(self):
    return (
      ReadyExecution.objects.count()
      + ScheduledExecution.objects.count()
      + ClaimedExecution.objects.count()
      + BlockedExecution.objects.count()
    )


@pytest.mark.parametrize("seed", [1, 7, 19])
def test_seeded_runtime_simulation_preserves_invariants(seed, monkeypatch):
  simulation = RuntimeSimulation(seed=seed, monkeypatch=monkeypatch)

  simulation.start()

  try:
    simulation.run(steps=90)
    simulation.drain()
  finally:
    simulation.stop()

  assert Process.objects.count() == 0
  assert ReadyExecution.objects.count() == 0
  assert ScheduledExecution.objects.count() == 0
  assert ClaimedExecution.objects.count() == 0
  assert BlockedExecution.objects.count() == 0
  assert FailedExecution.objects.count() == 0
