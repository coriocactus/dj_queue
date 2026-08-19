import pytest

from dj_queue.exceptions import ProcessExitError, ProcessMissingError, ProcessPrunedError
from dj_queue.models import FailedExecution, Job, Process, ReadyExecution, RecurringExecution
from tests.factories import make_failed_job
from tests.sim.config import simulation_seeds, simulation_steps
from tests.sim.runtime import RuntimeSimulation

pytestmark = pytest.mark.django_db(transaction=True)

ALLOWED_FAILURE_CLASSES = {
  f"{ProcessExitError.__module__}.{ProcessExitError.__qualname__}",
  f"{ProcessMissingError.__module__}.{ProcessMissingError.__qualname__}",
  f"{ProcessPrunedError.__module__}.{ProcessPrunedError.__qualname__}",
}


@pytest.mark.parametrize("seed", simulation_seeds())
def test_seeded_recurring_retry_simulation_preserves_dedupe_and_recovery(seed, monkeypatch):
  simulation = RuntimeSimulation(seed=seed, monkeypatch=monkeypatch)

  simulation.start()

  try:
    simulation.schedule_fixed_recurring("retry-a", queue_name="alpha")
    simulation.schedule_fixed_recurring("retry-b", queue_name="beta")

    simulation.run_actions(
      [
        simulation.scheduler_tick,
        simulation.scheduler_tick,
        simulation.worker_tick,
        simulation.crash_random_runner,
        simulation.retry_random_failed_job,
        simulation.dispatcher_tick,
        simulation.worker_tick,
        simulation.complete_worker_task,
      ]
    )

    assert RecurringExecution.objects.filter(task_key="retry-a").count() == 1
    assert RecurringExecution.objects.filter(task_key="retry-b").count() == 1
    assert Process.objects.count() == 4
    assert Job.objects.filter(queue_name__in=["alpha", "beta"]).exists() is True

    simulation.run(steps=simulation_steps())
    simulation.drain()
  finally:
    simulation.stop()

  assert Process.objects.count() == 0
  assert ReadyExecution.objects.count() == 0
  assert set(FailedExecution.objects.values_list("exception_class", flat=True)).issubset(
    ALLOWED_FAILURE_CLASSES
  )


def test_seeded_simulation_schedules_failed_retry_until_due(monkeypatch):
  simulation = RuntimeSimulation(seed=1, monkeypatch=monkeypatch)

  simulation.start()

  try:
    job = make_failed_job(queue_name="alpha")

    simulation.run_actions(
      [
        simulation.schedule_random_failed_job_retry,
        simulation.dispatcher_tick,
      ]
    )

    failed = FailedExecution.objects.get(job=job)
    assert failed.retry_at is not None
    assert ReadyExecution.objects.filter(job=job).exists() is False

    simulation.now = failed.retry_at
    simulation.run_actions([simulation.dispatcher_tick])

    assert FailedExecution.objects.filter(job=job).exists() is False
    assert ReadyExecution.objects.filter(job=job).exists() is True
  finally:
    simulation.stop()
