from unittest.mock import Mock

import pytest
from django.db import OperationalError

from dj_queue.models import ClaimedExecution, Job, ScheduledExecution
from dj_queue.operations import jobs
from tests.tasks import echo, limited_discard


@pytest.mark.django_db
@pytest.mark.parametrize("preserve_finished", [False, True])
def test_completion_retry_keeps_identity_and_executes_task_once(
  settings, monkeypatch, preserve_finished
):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {"preserve_finished_jobs": preserve_finished},
    }
  }
  job = jobs.enqueue_job(echo, ["done"], {})
  job_id = job.pk
  claimed = jobs.claim_ready_jobs(limit=1)[0]
  call_task = Mock(wraps=jobs._call_task)
  release = Mock(side_effect=[OperationalError("database is locked"), None])
  monkeypatch.setattr(jobs, "_call_task", call_task)
  monkeypatch.setattr(jobs, "_release_concurrency_slot", release)

  jobs.execute_claimed_job(claimed)

  assert call_task.call_count == 1
  assert release.call_count == 2
  assert not ClaimedExecution.objects.filter(job_id=job_id).exists()
  assert Job.objects.filter(pk=job_id).exists() is preserve_finished
  if preserve_finished:
    assert Job.objects.get(pk=job_id).return_value == "done"


@pytest.mark.django_db
def test_bulk_retry_rebuilds_discarded_jobs_when_capacity_changes(monkeypatch):
  bulk_create = jobs._bulk_create
  acquire = jobs.semaphore_acquire_many
  attempts = 0
  job_ids = []

  def create_with_conflict(alias, model, objects):
    nonlocal attempts
    if model is Job:
      job_ids.append(tuple(job.pk for job in objects))
    if model is ScheduledExecution:
      attempts += 1
      if attempts == 1:
        raise OperationalError("database is locked")
    return bulk_create(alias, model, objects)

  def acquire_after_retry(*args, **kwargs):
    if attempts == 0:
      return 0
    return acquire(*args, **kwargs)

  monkeypatch.setattr(jobs, "_bulk_create", create_with_conflict)
  monkeypatch.setattr(jobs, "semaphore_acquire_many", acquire_after_retry)

  [(job, _task, outcome)] = jobs.enqueue_jobs_bulk([(limited_discard, [1], {})])

  assert attempts == 2
  assert job_ids[0] == job_ids[1]
  assert outcome is jobs.DispatchOutcome.READY
  job.refresh_from_db()
  assert job.finished_at is None
  assert job.status == "ready"
  assert Job.objects.count() == 1
