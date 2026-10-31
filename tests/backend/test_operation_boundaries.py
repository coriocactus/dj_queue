import pytest

from dj_queue.models import Job
from dj_queue.operations.dispatch import (
  DispatchDecision,
  DispatchEntry,
  DispatchRows,
  _append_dispatch_row,
)


def test_unresolved_dispatch_is_not_discarded():
  job = Job()
  entry = DispatchEntry(job=job, decision=DispatchDecision(None))
  rows = DispatchRows()

  with pytest.raises(ValueError, match="unexpected dispatch outcome"):
    _append_dispatch_row(rows, entry, backend_alias="default", now=None)

  assert not rows.discarded
  assert job.finished_at is None
