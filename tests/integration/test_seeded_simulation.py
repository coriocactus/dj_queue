import pytest

from dj_queue.models import (
  BlockedExecution,
  ClaimedExecution,
  FailedExecution,
  Process,
  ReadyExecution,
  ScheduledExecution,
)
from tests.sim.config import simulation_seeds, simulation_steps
from tests.sim.runtime import RuntimeSimulation

pytestmark = pytest.mark.django_db(transaction=True)


@pytest.mark.parametrize("seed", simulation_seeds())
def test_seeded_runtime_simulation_preserves_invariants(seed, monkeypatch):
  simulation = RuntimeSimulation(seed=seed, monkeypatch=monkeypatch)

  simulation.start()

  try:
    orphan_job_id = simulation.inject_startup_orphan()
    simulation.assert_invariants()
    assert FailedExecution.objects.filter(job_id=orphan_job_id).exists() is True

    simulation.run(steps=simulation_steps())
    simulation.drain()
  finally:
    simulation.stop()

  assert Process.objects.count() == 0
  assert ReadyExecution.objects.count() == 0
  assert ScheduledExecution.objects.count() == 0
  assert ClaimedExecution.objects.count() == 0
  assert BlockedExecution.objects.count() == 0
