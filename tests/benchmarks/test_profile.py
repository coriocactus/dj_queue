import importlib.util
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("profile_cli", PROJECT_ROOT / "bin" / "profile.py")
profile_cli = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(profile_cli)


def test_default_profile_scenarios_cover_observability_hot_surfaces():
  expected = {
    "backend-snapshot",
    "stats-payload",
    "metric-families",
    "dashboard-overview",
    "dashboard-queues-sort-ready",
    "dashboard-semaphores-sort-blocked-waiters",
    "queue-info-all",
  }

  assert expected.issubset(profile_cli.SCENARIO_ORDER)
  assert expected.issubset(profile_cli.SCENARIOS)


def test_ordered_selector_profile_is_explicit_only():
  assert "ordered-selector-claim" in profile_cli.SCENARIO_CHOICES
  assert "ordered-selector-claim" in profile_cli.SCENARIOS
  assert "ordered-selector-claim" not in profile_cli.SCENARIO_ORDER
