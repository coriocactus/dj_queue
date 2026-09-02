import logging
import random
from concurrent.futures import Future
from datetime import timedelta
from uuid import uuid4

from django.db.models import Count

from dj_queue.api import QueueInfo, schedule_recurring_task, unschedule_recurring_task
from dj_queue.exceptions import ProcessExitError, ProcessMissingError, ProcessPrunedError
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
from dj_queue.operations.jobs import retry_failed_job, schedule_failed_job_retry
from dj_queue.runtime.supervisor import AsyncSupervisor
from tests.sim.config import fixed_now, simulation_tasks_settings
from tests.tasks import echo, limited

logger = logging.getLogger("dj_queue.stress")


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

  def stop(self, *, timeout=None):
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
    self.step = 0
    self.recovered_failure_classes = set()
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
    self._log_state(logging.INFO, "start")

  def stop(self):
    for runner in self.supervisor.runners:
      if runner.process_kind == "Worker":
        runner.pool.clear()
    self.supervisor.stop()
    self._log_state(logging.INFO, "stop")

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
      self.prune_random_runner,
      self.retry_random_failed_job,
      self.schedule_random_failed_job_retry,
      self.advance_time,
      self.supervisor_tick,
    )

    for step in range(1, steps + 1):
      self.step = step
      action = self.rng.choice(actions)
      action()
      self.assert_invariants()
      self._log_state(logging.DEBUG, action.__name__)
      if step == 1 or step % 25 == 0:
        self._log_state(logging.INFO, action.__name__)

  def run_actions(self, actions):
    for action in actions:
      self.step += 1
      action()
      self.assert_invariants()
      self._log_state(logging.INFO, action.__name__)

  def drain(self):
    for key in list(self.active_recurring_keys):
      unschedule_recurring_task(key)
      self.active_recurring_keys.discard(key)

    for pause in Pause.objects.all():
      QueueInfo(pause.queue_name).resume()

    for iteration in range(1, 181):
      self.supervisor_tick()
      self.scheduler_tick()
      self.dispatcher_tick()
      self.worker_tick()
      self.complete_worker_task()
      self.assert_invariants()
      if self._non_terminal_count() == 0 and self._running_tasks() == 0:
        self._log_state(logging.INFO, "drain_complete")
        return
      if iteration == 1 or iteration % 10 == 0:
        self._log_state(logging.INFO, "drain_wait")
      self.now += timedelta(minutes=1)

    assert self._non_terminal_count() == 0, f"seed {self.seed} left live jobs behind"
    assert self._running_tasks() == 0, f"seed {self.seed} left in-flight work behind"

  def enqueue_ready(self):
    queue_name = self.rng.choice(("default", "alpha", "beta"))
    echo.using(priority=self.rng.randint(-5, 5), queue_name=queue_name).enqueue(
      self._next_value("ready")
    )

  def enqueue_ready_on_queue(self, queue_name, *, priority=0):
    echo.using(priority=priority, queue_name=queue_name).enqueue(self._next_value(queue_name))

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
    self.schedule_fixed_recurring(key, queue_name=queue_name)

  def schedule_fixed_recurring(self, key, *, queue_name="default"):
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

  def pause_random_queue_named(self, queue_name):
    self.claimed_job_ids_by_pause[queue_name] = set(
      ClaimedExecution.objects.filter(job__queue_name=queue_name).values_list("job_id", flat=True)
    )
    QueueInfo(queue_name).pause()
    logger.info("simulation seed=%s action=pause queue=%s", self.seed, queue_name)

  def resume_random_queue(self):
    paused = list(Pause.objects.values_list("queue_name", flat=True))
    if not paused:
      return
    queue_name = self.rng.choice(paused)
    self.resume_queue(queue_name)

  def resume_queue(self, queue_name):
    QueueInfo(queue_name).resume()
    self.claimed_job_ids_by_pause.pop(queue_name, None)
    logger.info("simulation seed=%s action=resume queue=%s", self.seed, queue_name)

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
    logger.info(
      "simulation seed=%s action=crash runner=%s name=%s",
      self.seed,
      runner.process_kind,
      runner.name,
    )
    self._crash_runner(runner)

  def prune_random_runner(self):
    prunable = [runner for runner in self.supervisor.runners if runner.process is not None]
    if not prunable:
      return
    runner = self.rng.choice(prunable)
    logger.info(
      "simulation seed=%s action=prune runner=%s name=%s",
      self.seed,
      runner.process_kind,
      runner.name,
    )
    self._prune_runner(runner)

  def retry_random_failed_job(self):
    failed_job_ids = list(FailedExecution.objects.values_list("job_id", flat=True))
    if not failed_job_ids:
      return
    job_id = self.rng.choice(failed_job_ids)
    logger.info("simulation seed=%s action=retry job_id=%s", self.seed, job_id)
    retry_failed_job(job_id)

  def schedule_random_failed_job_retry(self):
    failed_job_ids = list(FailedExecution.objects.values_list("job_id", flat=True))
    if not failed_job_ids:
      return
    job_id = self.rng.choice(failed_job_ids)
    retry_at = self.now + timedelta(minutes=self.rng.randint(1, 3))
    logger.info(
      "simulation seed=%s action=schedule_retry job_id=%s retry_at=%s",
      self.seed,
      job_id,
      retry_at,
    )
    schedule_failed_job_retry(job_id, retry_at=retry_at)

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
      assert semaphore.active_count >= 0, (
        f"seed {self.seed} invalid active count {semaphore.key}={semaphore.active_count}"
      )
      assert semaphore.value == semaphore.available_count, (
        f"seed {self.seed} inconsistent semaphore {semaphore.key}: "
        f"active={semaphore.active_count} available={semaphore.value} limit={semaphore.limit}"
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

    pruned_failures = FailedExecution.objects.filter(message__contains="process heartbeat expired")
    pruned_classes = set(pruned_failures.values_list("exception_class", flat=True))
    if pruned_classes:
      assert pruned_classes == {
        f"{ProcessPrunedError.__module__}.{ProcessPrunedError.__qualname__}"
      }

    missing_failures = FailedExecution.objects.filter(
      message__contains="process no longer registered at supervisor startup"
    )
    missing_classes = set(missing_failures.values_list("exception_class", flat=True))
    if missing_classes:
      assert missing_classes == {
        f"{ProcessMissingError.__module__}.{ProcessMissingError.__qualname__}"
      }

    observed_failure_classes = failed_exit_classes | pruned_classes | missing_classes
    assert observed_failure_classes.issubset(
      {
        f"{ProcessExitError.__module__}.{ProcessExitError.__qualname__}",
        f"{ProcessPrunedError.__module__}.{ProcessPrunedError.__qualname__}",
        f"{ProcessMissingError.__module__}.{ProcessMissingError.__qualname__}",
      }
    )
    self.recovered_failure_classes.update(observed_failure_classes)

  def _crash_runner(self, runner):
    self.crash_count += 1
    expected_failures = 0
    if runner.process_kind == "Worker" and runner.process is not None:
      expected_failures = ClaimedExecution.objects.filter(process=runner.process).count()
    if runner.process_kind == "Worker":
      runner.pool.clear()
    self.supervisor._fail_crashed_runner_jobs(runner)
    if expected_failures:
      assert (
        FailedExecution.objects.filter(message__contains="runner thread crashed").count()
        >= expected_failures
      )
    runner.stop(timeout=0) if runner.process_kind == "Worker" else runner.stop()
    replacement = self.supervisor._rebuild_runner(runner)
    replacement.sleeper = FakeSleeper()
    if replacement.process_kind == "Worker":
      replacement.pool = ManualWorkerPool(replacement.config.threads)
      replacement.wakeup_backend = FakeWakeupBackend()
    replacement.start()
    self.supervisor._replace_runner(runner, replacement)
    assert replacement.process is not None

  def _prune_runner(self, runner):
    if runner.process is None:
      return

    stale_process = runner.process
    if runner.process_kind == "Worker":
      runner.pool.clear()

    stale_process.last_heartbeat_at = self.now - timedelta(days=365)
    stale_process.save(update_fields=["last_heartbeat_at"])
    self.supervisor.prune_stale_process_rows(now=self.now)
    assert Process.objects.filter(pk=stale_process.pk).exists() is False

    replacement = self.supervisor._rebuild_runner(runner)
    replacement.sleeper = FakeSleeper()
    if replacement.process_kind == "Worker":
      replacement.pool = ManualWorkerPool(replacement.config.threads)
      replacement.wakeup_backend = FakeWakeupBackend()
    replacement.start()
    self.supervisor._replace_runner(runner, replacement)
    assert replacement.process is not None

  def inject_startup_orphan(self):
    orphan_job = limited.enqueue(1, value=self._next_value("orphan"))
    worker = next(runner for runner in self.supervisor.runners if runner.process_kind == "Worker")
    worker.poll_once()
    worker.pool.clear()
    claimed = ClaimedExecution.objects.get(job_id=orphan_job.id)
    claimed.process = None
    claimed.save(update_fields=["process"])
    self.supervisor.fail_startup_orphaned_jobs()
    failed = FailedExecution.objects.get(job_id=orphan_job.id)
    assert failed.exception_class == (
      f"{ProcessMissingError.__module__}.{ProcessMissingError.__qualname__}"
    )
    logger.info(
      "simulation seed=%s action=inject_startup_orphan job_id=%s", self.seed, orphan_job.id
    )
    return orphan_job.id

  def _log_state(self, level, action):
    if not logger.isEnabledFor(level):
      return
    logger.log(
      level,
      (
        "simulation seed=%s step=%s action=%s ready=%s scheduled=%s claimed=%s "
        "blocked=%s failed=%s finished=%s running=%s pauses=%s recurring=%s crashes=%s"
      ),
      self.seed,
      self.step,
      action,
      ReadyExecution.objects.count(),
      ScheduledExecution.objects.count(),
      ClaimedExecution.objects.count(),
      BlockedExecution.objects.count(),
      FailedExecution.objects.count(),
      Job.objects.filter(finished_at__isnull=False).count(),
      self._running_tasks(),
      Pause.objects.count(),
      RecurringExecution.objects.count(),
      self.crash_count,
    )

  def _patch_time(self):
    self.monkeypatch.setattr("dj_queue.runtime.dispatcher.timezone.now", lambda: self.now)
    self.monkeypatch.setattr("dj_queue.runtime.scheduler.timezone.now", lambda: self.now)
    self.monkeypatch.setattr("dj_queue.operations.jobs.timezone.now", lambda: self.now)
    self.monkeypatch.setattr("dj_queue.operations.concurrency.timezone.now", lambda: self.now)
    self.monkeypatch.setattr("dj_queue.observability.timezone.now", lambda: self.now)

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
