import pytest

from benchmarks.harness import parse_sizes


def test_parse_sizes_uses_default_for_empty_value():
  assert parse_sizes(None, default=[100]) == [100]
  assert parse_sizes("", default=[100]) == [100]


def test_parse_sizes_rejects_non_positive_values():
  with pytest.raises(ValueError, match="positive"):
    parse_sizes("100,0", default=[100])
