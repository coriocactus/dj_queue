from datetime import timedelta

import pytest
from django.utils import timezone

from dj_queue import dashboard
from dj_queue.models import Semaphore

pytestmark = pytest.mark.django_db


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
