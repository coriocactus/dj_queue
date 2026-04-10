from datetime import datetime, timedelta
import os
import random
from concurrent.futures import Future
from uuid import uuid4

import pytest
from django.db.models import Count
from django.utils import timezone

from dj_queue.api import QueueInfo, schedule_recurring_task, unschedule_recurring_task
from dj_queue.config import load_backend_config
from dj_queue.exceptions import ProcessExitError
from dj_queue.models import (
  BlockedExecution,
  ClaimedExecution,
  FailedExecution,
  Job,
  Pause,
  Process,
  ReadyExecution,
  RecurringExecution,
  ScheduledExecution,
  Semaphore,
)
from dj_queue.runtime.supervisor import AsyncSupervisor
from tests.tasks import echo, limited

pytestmark = pytest.mark.django_db(transaction=True)


def _simulation_seeds():
  configured = os.environ.get("SIM_SEEDS")
  if configured:
    return [int(value.strip()) for value in configured.split(",") if value.strip()]
  if os.environ.get("RUN_STRESS") == "1":
    return list(range(50))
  return [1, 7, 19]


def _simulation_steps():
  configured = os.environ.get("SIM_STEPS")
  if configured:
    return int(configured)
  if os.environ.get("RUN_STRESS") == "1":
    return 250
  return 90


SIMULATION_SEEDS = _simulation_seeds()
SIMULATION_STEPS = _simulation_steps()


def fixed_now():
  return datetime(2026, 4, 8, 12, 0, 1, tzinfo=timezone.get_current_timezone())


def simulation_tasks_settings():
  return {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": [],
      "OPTIONS": {
        "mode": "async",
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
        "process_heartbeat_interval": 0,
        "process_alive_threshold": 10_000_000,
        "preserve_finished_jobs": True,
        "clear_finished_jobs_after": None,
      },
    }
  }


class FakeSleeper:
  def sleep(self, _seconds):
    return None

  def wake_up(self):
    return None

  def close(self):
    return None


class FakeWakeupBackend:
  def start(self):
    return None

  def stop(self):
    return None


class ManualWorkerPool:
  def __init__(self, max_workers):
    self.max_workers = max_workers
    self._running = []
    self._on_drained = None

  @property
  def idle_capacity(self):
    return max(0, self.max_workers - len(self._running))

  def submit(self, fn, *args, **kwargs):
    future = Future()
    self._running.append((future, fn, args, kwargs))
    return future

  def run_one(self):
    if not self._running:
      return False

    future, fn, args, kwargs = self._running.pop(0)
    try:
      future.set_result(fn(*args, **kwargs))
    except Exception as exc:
      future.set_exception(exc)

    if not self._running and self._on_drained is not None:
      callback = self._on_drained
      self._on_drained = None
      callback()
    return True

  def clear(self):
    self._running.clear()

  def shutdown(self, timeout, *, on_drained=None):
    self._on_drained = on_drained
    if not self._running:
      if on_drained is not None:
        self._on_drained = None
        on_drained()
      return True
    return False


class RuntimeSimulation:
  def __init__(self, *, seed, monkeypatch):
    self.seed = seed
    self.monkeypatch = monkeypatch
    self.rng = random.Random(seed)
    self.now = fixed_now()
    self.counter = 0
    self.claimed_job_ids_by_pause = {}
    self.active_recurring_keys = set()
    self.crash_count = 0
    self.expected_crash_failures = 0
    self.supervisor = AsyncSupervisor.from_backend_config(
      backend_alias="default",
      tasks_settings=simulation_tasks_settings(),
      standalone=False,
      name=f"sim-supervisor-{uuid4()}",
      pid=11000,
      hostname="localhost",
    )

  def start(self):
    self._patch_time()
    self.supervisor._acquire_pidfile()
    self.supervisor.process = self.supervisor._register_process()
    self.supervisor._started = True
    self.supervisor.fail_startup_orphaned_jobs()
    self.supervisor.runners = list(self.supervisor._build_runners())
    self.supervisor.runner_threads = []
    self.supervisor._stop_event.clear()

    for runner in self.supervisor.runners:
      runner.sleeper = FakeSleeper()
      if runner.process_kind == "Worker":
        runner.pool = ManualWorkerPool(runner.config.threads)
        runner.wakeup_backend = FakeWakeupBackend()
      runner.start()

    assert all(runner.process is not None for runner in self.supervisor.runners), [
      (runner.process_kind, runner.process) for runner in self.supervisor.runners
    ]

  def stop(self):
    for runner in self.supervisor.runners:
      if runner.process_kind == "Worker":
        runner.pool.clear()
    self.supervisor.stop()

  def run(self, *, steps):
    actions = (
      self.enqueue_ready,
      self.enqueue_scheduled,
      self.enqueue_limited,
      self.schedule_recurring,
      self.unschedule_recurring,
      self.pause_random_queue,
      self.resume_random_queue,
      self.worker_tick,
      self.dispatcher_tick,
      self.scheduler_tick,
      self.complete_worker_task,
      self.crash_random_runner,
      self.advance_time,
      self.supervisor_tick,
    )

    for _ in range(steps):
      action = self.rng.choice(actions)
      action()
      self.assert_invariants()

  def drain(self):
    for key in list(self.active_recurring_keys):
      unschedule_recurring_task(key)
      self.active_recurring_keys.discard(key)

    for pause in Pause.objects.all():
      QueueInfo(pause.queue_name).resume()

    for _ in range(180):
      self.supervisor_tick()
      self.scheduler_tick()
      self.dispatcher_tick()
      self.worker_tick()
      self.complete_worker_task()
      self.assert_invariants()
      if self._non_terminal_count() == 0 and self._running_tasks() == 0:
        return
      self.now += timedelta(minutes=1)

    assert self._non_terminal_count() == 0, f"seed {self.seed} left live jobs behind"
    assert self._running_tasks() == 0, f"seed {self.seed} left in-flight work behind"

  def enqueue_ready(self):
    queue_name = self.rng.choice(("default", "alpha", "beta"))
    echo.using(priority=self.rng.randint(-5, 5), queue_name=queue_name).enqueue(
      self._next_value("ready")
    )

  def enqueue_scheduled(self):
    queue_name = self.rng.choice(("default", "alpha", "beta"))
    echo.using(
      priority=self.rng.randint(-5, 5),
      queue_name=queue_name,
      run_after=self.now + timedelta(minutes=self.rng.randint(1, 3)),
    ).enqueue(self._next_value("scheduled"))

  def enqueue_limited(self):
    limited.using(priority=self.rng.randint(-5, 5)).enqueue(
      self.rng.randint(1, 2),
      value=self._next_value("limited"),
    )

  def schedule_recurring(self):
    key = f"sim-recurring-{self.rng.randint(0, 4)}"
    queue_name = self.rng.choice(("default", "alpha", "beta"))
    schedule_recurring_task(
      key=key,
      task_path="tests.tasks.echo",
      schedule=self.rng.choice(("* * * * *", "*/2 * * * *")),
      args=(self._next_value("recurring"),),
      queue_name=queue_name,
    )
    self.active_recurring_keys.add(key)

  def unschedule_recurring(self):
    if not self.active_recurring_keys:
      return
    key = sorted(self.active_recurring_keys)[self.rng.randrange(len(self.active_recurring_keys))]
    unschedule_recurring_task(key)
    self.active_recurring_keys.discard(key)

  def pause_random_queue(self):
    queue_name = self.rng.choice(("default", "alpha", "beta"))
    self.claimed_job_ids_by_pause[queue_name] = set(
      ClaimedExecution.objects.filter(job__queue_name=queue_name).values_list("job_id", flat=True)
    )
    QueueInfo(queue_name).pause()

  def resume_random_queue(self):
    paused = list(Pause.objects.values_list("queue_name", flat=True))
    if not paused:
      return
    queue_name = self.rng.choice(paused)
    QueueInfo(queue_name).resume()
    self.claimed_job_ids_by_pause.pop(queue_name, None)

  def worker_tick(self):
    for runner in self.supervisor.runners:
      if runner.process_kind == "Worker":
        runner.poll_once()

  def dispatcher_tick(self):
    for runner in self.supervisor.runners:
      if runner.process_kind == "Dispatcher":
        runner.poll_once()

  def scheduler_tick(self):
    for runner in self.supervisor.runners:
      if runner.process_kind == "Scheduler":
        runner.poll_once(now=self.now)

  def complete_worker_task(self):
    workers = [runner for runner in self.supervisor.runners if runner.process_kind == "Worker"]
    if not workers:
      return
    worker = self.rng.choice(workers)
    worker.pool.run_one()

  def crash_random_runner(self):
    crashable = [runner for runner in self.supervisor.runners if runner.process is not None]
    if not crashable:
      return
    runner = self.rng.choice(crashable)
    self._crash_runner(runner)

  def supervisor_tick(self):
    self.supervisor.poll_once()

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

    for pause in Pause.objects.values_list("queue_name", flat=True):
      paused_claim_ids = set(
        ClaimedExecution.objects.filter(job__queue_name=pause).values_list("job_id", flat=True)
      )
      assert paused_claim_ids.issubset(
        self.claimed_job_ids_by_pause.get(pause, paused_claim_ids)
      ), f"seed {self.seed} claimed new paused-queue work for {pause}"

    runtime_processes = list(Process.objects.order_by("kind", "name"))
    assert len(runtime_processes) == 4, f"seed {self.seed} lost a runtime process row"
    assert {process.kind for process in runtime_processes} == {
      "Supervisor",
      "Worker",
      "Dispatcher",
      "Scheduler",
    }

    crash_failures = FailedExecution.objects.filter(message__contains="runner thread crashed")
    failed_exit_classes = set(crash_failures.values_list("exception_class", flat=True))
    if failed_exit_classes:
      assert failed_exit_classes == {
        f"{ProcessExitError.__module__}.{ProcessExitError.__qualname__}"
      }
    assert crash_failures.count() >= self.expected_crash_failures

  def _crash_runner(self, runner):
    self.crash_count += 1
    if runner.process_kind == "Worker" and runner.process is not None:
      self.expected_crash_failures += ClaimedExecution.objects.filter(
        process=runner.process
      ).count()
    if runner.process_kind == "Worker":
      runner.pool.clear()
    self.supervisor._fail_crashed_runner_jobs(runner)
    runner.stop(timeout=0) if runner.process_kind == "Worker" else runner.stop()
    replacement = self.supervisor._rebuild_runner(runner)
    replacement.sleeper = FakeSleeper()
    if replacement.process_kind == "Worker":
      replacement.pool = ManualWorkerPool(replacement.config.threads)
      replacement.wakeup_backend = FakeWakeupBackend()
    replacement.start()
    self.supervisor._replace_runner(runner, replacement)
    assert replacement.process is not None

  def _patch_time(self):
    self.monkeypatch.setattr("dj_queue.runtime.dispatcher.timezone.now", lambda: self.now)
    self.monkeypatch.setattr("dj_queue.runtime.scheduler.timezone.now", lambda: self.now)
    self.monkeypatch.setattr("dj_queue.operations.jobs.timezone.now", lambda: self.now)
    self.monkeypatch.setattr("dj_queue.operations.concurrency.timezone.now", lambda: self.now)
    self.monkeypatch.setattr("dj_queue.api.timezone.now", lambda: self.now)

  def _next_value(self, prefix):
    value = f"{prefix}-{self.seed}-{self.counter:04d}"
    self.counter += 1
    return value

  def _non_terminal_count(self):
    return (
      ReadyExecution.objects.count()
      + ScheduledExecution.objects.count()
      + ClaimedExecution.objects.count()
      + BlockedExecution.objects.count()
    )

  def _running_tasks(self):
    return sum(
      len(runner.pool._running)
      for runner in self.supervisor.runners
      if runner.process_kind == "Worker"
    )


@pytest.mark.parametrize("seed", SIMULATION_SEEDS)
def test_seeded_runtime_simulation_preserves_invariants(seed, monkeypatch):
  simulation = RuntimeSimulation(seed=seed, monkeypatch=monkeypatch)

  simulation.start()

  try:
    simulation.run(steps=SIMULATION_STEPS)
    simulation.drain()
  finally:
    simulation.stop()

  assert Process.objects.count() == 0
  assert ReadyExecution.objects.count() == 0
  assert ScheduledExecution.objects.count() == 0
  assert ClaimedExecution.objects.count() == 0
  assert BlockedExecution.objects.count() == 0
