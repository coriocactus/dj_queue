import pytest

from dj_queue.exceptions import ProcessExitError, ProcessMissingError, ProcessPrunedError
from dj_queue.models import ClaimedExecution, FailedExecution, Job, Pause, ReadyExecution
from tests.sim.config import simulation_seeds, simulation_steps
from tests.sim.runtime import RuntimeSimulation

pytestmark = pytest.mark.django_db(transaction=True)

ALLOWED_FAILURE_CLASSES = {
  f"{ProcessExitError.__module__}.{ProcessExitError.__qualname__}",
  f"{ProcessMissingError.__module__}.{ProcessMissingError.__qualname__}",
  f"{ProcessPrunedError.__module__}.{ProcessPrunedError.__qualname__}",
}


@pytest.mark.parametrize("seed", simulation_seeds())
def test_seeded_queue_control_simulation_preserves_pause_and_fairness(seed, monkeypatch):
  simulation = RuntimeSimulation(seed=seed, monkeypatch=monkeypatch)

  simulation.start()

  try:
    simulation.enqueue_ready_on_queue("alpha", priority=10)
    simulation.enqueue_ready_on_queue("beta", priority=5)
    simulation.enqueue_ready_on_queue("default", priority=0)
    simulation.assert_invariants()

    simulation.run_actions(
      [
        lambda: simulation.pause_random_queue_named("alpha"),
        simulation.worker_tick,
        simulation.complete_worker_task,
      ]
    )

    alpha_job_ids = set(Job.objects.filter(queue_name="alpha").values_list("id", flat=True))
    assert alpha_job_ids
    assert ClaimedExecution.objects.filter(job_id__in=alpha_job_ids).exists() is False
    assert ReadyExecution.objects.filter(job_id__in=alpha_job_ids).exists() is True

    simulation.run_actions(
      [
        lambda: simulation.resume_queue("alpha"),
        simulation.worker_tick,
        simulation.complete_worker_task,
        simulation.worker_tick,
        simulation.complete_worker_task,
      ]
    )

    finished_queues = list(
      Job.objects.filter(finished_at__isnull=False)
      .order_by("finished_at", "id")
      .values_list("queue_name", flat=True)
    )
    assert "alpha" in finished_queues
    assert set(finished_queues).issuperset({"alpha", "beta", "default"})

    simulation.run(steps=simulation_steps())
    simulation.drain()
  finally:
    simulation.stop()

  assert Pause.objects.count() == 0
  assert ReadyExecution.objects.count() == 0
  assert ClaimedExecution.objects.count() == 0
  assert set(FailedExecution.objects.values_list("exception_class", flat=True)).issubset(
    ALLOWED_FAILURE_CLASSES
  )
