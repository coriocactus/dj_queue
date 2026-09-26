import pytest
from django.utils import timezone

from dj_queue import health
from dj_queue.models import BlockedExecution, ReadyExecution, ScheduledExecution
from dj_queue.queue_state import queue_state_summaries_by_queue
from tests.factories import make_job

pytestmark = pytest.mark.django_db


@pytest.mark.parametrize(
  ("state", "model"),
  [("ready", ReadyExecution), ("scheduled", ScheduledExecution), ("blocked", BlockedExecution)],
)
@pytest.mark.parametrize("backend_alias", ["default", "other"])
def test_health_reports_queue_drift_in_scope_without_changing_canonical_counts(
  state, model, backend_alias, settings, django_assert_num_queries
):
  settings.TASKS = {
    alias: {"BACKEND": "dj_queue.backend.DjQueueBackend", "OPTIONS": {}}
    for alias in ("default", "other")
  }
  now = timezone.now()
  job = make_job(backend_alias=backend_alias, queue_name="canonical", concurrency_key="account:1")
  fields = {}
  if state == "scheduled":
    fields["scheduled_at"] = now
  elif state == "blocked":
    fields.update(concurrency_key=job.concurrency_key, expires_at=now)
  model.objects.create(
    job=job,
    backend_alias=backend_alias,
    queue_name="drifted",
    priority=0,
    **fields,
  )

  summaries = queue_state_summaries_by_queue(backend_alias="default")
  if backend_alias == "default":
    assert list(summaries) == ["canonical"]
    assert summaries["canonical"].count(state) == 1
  else:
    assert summaries == {}
  diagnostic = f"1 {state} execution rows have mismatched queue ownership"
  assert (diagnostic in health.deep_health_problems(backend_alias="default", now=now)) is (
    backend_alias == "default"
  )

  with django_assert_num_queries(1):
    counts = health._state_ownership_mismatch_counts(
      model, alias="default", backend_alias="default"
    )
  assert counts == {"backend": 0, "queue": int(backend_alias == "default")}
