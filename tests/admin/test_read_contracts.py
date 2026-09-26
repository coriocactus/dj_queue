from datetime import timedelta

import pytest
from django.utils import timezone

from dj_queue import dashboard
from dj_queue.models import RecurringTask, Semaphore
from dj_queue.reads import controls

pytestmark = pytest.mark.django_db


@pytest.mark.parametrize("sort", ["key", "-last_run", "next_run"])
def test_recurring_pages_share_projection_without_loading_unneeded_rows(
  sort, monkeypatch, django_assert_num_queries
):
  now = timezone.now()
  RecurringTask.objects.bulk_create(
    [
      RecurringTask(
        backend_alias="default",
        key=f"task-{index:02}",
        task_path="tests.tasks.echo",
        schedule="@daily",
      )
      for index in range(30)
    ]
  )
  projected = []
  project_row = controls.recurring_row

  def capture(task, **kwargs):
    projected.append(task.key)
    return project_row(task, **kwargs)

  monkeypatch.setattr(controls, "recurring_row", capture)
  with django_assert_num_queries(2):
    page = dashboard._recurring_overview_page(
      backend_alias="default",
      now=now,
      page_size=3,
      page_number=2,
      sort=sort,
    )
  assert len(projected) == (30 if sort == "next_run" else 3)
  assert len(page["rows"]) == 3
  assert page["total_count"] == 30
  bulk_rows = {
    row["key"]: row
    for row in controls.recurring_rows_for_backend(backend_alias="default", now=now)
  }
  assert [{k: v for k, v in row.items() if k != "jobs_url"} for row in page["rows"]] == [
    bulk_rows[row["key"]] for row in page["rows"]
  ]


@pytest.mark.parametrize("sort", ["key", "-available", "-active", "-blocked_waiters"])
def test_semaphore_pages_share_projection_without_loading_unneeded_rows(
  sort, monkeypatch, django_assert_num_queries
):
  Semaphore.objects.bulk_create(
    [
      Semaphore(
        key=f"key-{index:02}",
        value=index % 3,
        active_count=None,
        limit=3,
        expires_at=timezone.now() + timedelta(minutes=1),
      )
      for index in range(30)
    ]
  )
  projected = []
  project_row = controls.semaphore_row

  def capture(semaphore, **kwargs):
    projected.append(semaphore.key)
    return project_row(semaphore, **kwargs)

  monkeypatch.setattr(controls, "semaphore_row", capture)
  with django_assert_num_queries(2):
    page = dashboard._semaphore_overview_page(
      backend_alias="default",
      page_size=3,
      page_number=2,
      sort=sort,
    )
  assert len(projected) == (30 if sort in {"-active", "-blocked_waiters"} else 3)
  assert len(page["rows"]) == 3
  assert page["total_count"] == 30
  bulk_rows = {
    row["key"]: row for row in controls.semaphore_rows_for_backend(backend_alias="default")
  }
  assert [{k: v for k, v in row.items() if k != "jobs_url"} for row in page["rows"]] == [
    bulk_rows[row["key"]] for row in page["rows"]
  ]


@pytest.mark.parametrize(
  ("sort", "expected"),
  [("active", ["low", "middle", "high"]), ("-active", ["high", "middle", "low"])],
)
def test_semaphore_sort_uses_displayed_occupancy_not_bridge(sort, expected, monkeypatch):
  for key, value, mirror in [("low", 2, 7), ("middle", 1, None), ("high", 0, 0)]:
    Semaphore.objects.create(
      key=key,
      value=value,
      active_count=mirror,
      limit=3,
      expires_at=timezone.now() + timedelta(minutes=1),
    )
  context = dashboard.dashboard_context(
    backend_alias="default", query_params={"semaphores_sort": sort}
  )
  rows = context["semaphore_section"]["rows"]
  assert [row["key"] for row in rows] == expected
  assert {row["key"]: row["active_count"] for row in rows} == {"low": 1, "middle": 2, "high": 3}

  monkeypatch.setitem(dashboard.OVERVIEW_PAGE_SIZES, "semaphores", 1)
  page = dashboard.dashboard_context(
    backend_alias="default", query_params={"semaphores_sort": sort, "semaphores_page": 1}
  )["semaphore_section"]
  assert [row["key"] for row in page["rows"]] == expected[:1]
  assert page["total_count"] == 3
