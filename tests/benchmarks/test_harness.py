import pytest

from benchmarks.harness import assert_persistent_connection_budget, parse_sizes


def test_parse_sizes_uses_default_for_empty_value():
  assert parse_sizes(None, default=[100]) == [100]
  assert parse_sizes("", default=[100]) == [100]


def test_parse_sizes_rejects_non_positive_values():
  with pytest.raises(ValueError, match="positive"):
    parse_sizes("100,0", default=[100])


def test_assert_persistent_connection_budget_allows_estimate_below_capacity():
  assert (
    assert_persistent_connection_budget(
      estimated_connections=74,
      available_connections=97,
    )
    is None
  )


def test_assert_persistent_connection_budget_rejects_estimate_at_capacity():
  with pytest.raises(RuntimeError, match="estimated 97 worker connections"):
    assert_persistent_connection_budget(
      estimated_connections=97,
      available_connections=97,
    )
