from importlib import import_module

import pytest

from dj_queue.models import Job
from dj_queue.operations import jobs
from dj_queue.operations.dispatch import (
  DispatchDecision,
  DispatchEntry,
  DispatchRows,
  _append_dispatch_row,
)


@pytest.mark.parametrize(
  ("owner", "names"),
  [
    ("claiming", ("ClaimedJob", "claim_ready_jobs")),
    ("dispatch", ("DispatchOutcome",)),
    (
      "enqueue",
      (
        "enqueue_job",
        "enqueue_job_with_dispatch",
        "enqueue_jobs_bulk",
        "validate_priority",
        "validate_queue_allowed",
      ),
    ),
    ("execution", ("execute_claimed_job", "complete_claimed_job", "fail_claimed_job")),
    (
      "recovery",
      (
        "fail_orphaned_claimed_jobs",
        "fail_claimed_jobs_for_process",
        "fail_claimed_jobs_for_pid",
        "fail_claimed_jobs_for_child",
        "prune_stale_processes",
      ),
    ),
  ],
)
def test_jobs_preserves_operation_imports(owner, names):
  module = import_module(f"dj_queue.operations.{owner}")
  for name in names:
    assert getattr(jobs, name) is getattr(module, name)


def test_unresolved_dispatch_is_not_discarded():
  job = Job()
  entry = DispatchEntry(job=job, decision=DispatchDecision(None))
  rows = DispatchRows()

  with pytest.raises(ValueError, match="unexpected dispatch outcome"):
    _append_dispatch_row(rows, entry, backend_alias="default", now=None)

  assert not rows.discarded
  assert job.finished_at is None
