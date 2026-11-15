import pytest
from django.utils import timezone

from dj_queue.exceptions import EnqueueError
from dj_queue.models import Job
from dj_queue.operations.dispatch import (
  DispatchDecision,
  DispatchEntry,
  DispatchOutcome,
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


@pytest.mark.parametrize(
  "outcome", [DispatchOutcome.READY, DispatchOutcome.SCHEDULED, DispatchOutcome.BLOCKED]
)
def test_dispatch_rows_reject_a_job_from_another_backend(outcome):
  job = Job(backend_alias="secondary")
  decision = DispatchDecision(None, concurrency_key="account:1", duration_seconds=60)
  entry = DispatchEntry(job=job, decision=decision, outcome=outcome)

  with pytest.raises(EnqueueError, match="belongs to backend 'secondary'"):
    _append_dispatch_row(DispatchRows(), entry, backend_alias="default", now=timezone.now())
