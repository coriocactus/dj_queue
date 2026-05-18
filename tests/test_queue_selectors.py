import pytest

from dj_queue.models import Job
from dj_queue.queue_selectors import (
  any_queue_matches_selectors,
  filter_by_queue_selectors,
  queue_matches_selectors,
)


def test_queue_matches_exact_wildcard_and_prefix_selectors():
  assert queue_matches_selectors("email-critical", "email-critical") is True
  assert queue_matches_selectors("email-critical", "email*") is True
  assert queue_matches_selectors("email-critical", "*") is True
  assert queue_matches_selectors("email-critical", ("default", "email*")) is True
  assert queue_matches_selectors("email-critical", ("default", "billing")) is False


def test_any_queue_matches_payload_names_against_worker_selectors():
  assert any_queue_matches_selectors(("default", "email-critical"), ("email*",)) is True
  assert any_queue_matches_selectors(("default",), ("email*",)) is False
  assert any_queue_matches_selectors(None, ("email*",)) is True


@pytest.mark.django_db
def test_filter_by_queue_selectors_builds_the_orm_adapter():
  default = _job_on_queue("default")
  email = _job_on_queue("email-critical")
  _job_on_queue("billing")

  jobs = filter_by_queue_selectors(
    Job.objects.order_by("queue_name"),
    ("default", "email*"),
  )

  assert list(jobs) == [default, email]


@pytest.mark.django_db
def test_filter_by_queue_selectors_preserves_empty_list_as_no_selection():
  _job_on_queue("default")

  jobs = filter_by_queue_selectors(Job.objects.all(), [])

  assert list(jobs) == []


def _job_on_queue(queue_name):
  return Job.objects.create(
    task_path="tests.tasks.echo",
    queue_name=queue_name,
    priority=0,
    payload={"args": [], "kwargs": {}},
    backend_alias="default",
  )
