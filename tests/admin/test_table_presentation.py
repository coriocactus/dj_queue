from urllib.parse import parse_qs, urlsplit

from django.http import QueryDict

from dj_queue import dashboard_tables as tables


def test_sort_and_pagination_preserve_oversized_process_groups():
  rows = [
    {"name": "z-parent", "is_child": False},
    *({"name": f"child-{index:02}", "is_child": True} for index in range(12)),
    {"name": "a-parent", "is_child": False},
  ]
  sorted_rows = tables.sort_overview_rows(rows=rows, section="processes", sort="-name")
  first = tables._paginate_process_rows(rows=sorted_rows, page_size=10, page_number=1)
  second = tables._paginate_process_rows(rows=sorted_rows, page_size=10, page_number=2)
  assert [row["name"] for row in first["rows"]] == [
    "z-parent",
    *(f"child-{index:02}" for index in reversed(range(12))),
  ]
  assert [row["name"] for row in second["rows"]] == ["a-parent"]
  assert (first["start_index"], first["end_index"], first["total_count"]) == (1, 13, 14)
  assert (second["start_index"], second["end_index"]) == (14, 14)


def test_column_links_preserve_multivalued_filters_and_anchor_policy():
  params = QueryDict("backend=secondary&tag=one&tag=two&page=4&sort=-task.priority")
  headers = tables.queue_page_headers(
    state="ready",
    query_params=params,
    sort="-task.priority",
    explicit_sort=True,
    page_param="page",
    anchor="result_list",
  )
  task = next(header for header in headers if header["text"] == "task")
  assert task["sort_priority"] == 1
  for key in ("url_primary", "url_toggle", "url_remove"):
    url = urlsplit(task[key])
    query = parse_qs(url.query)
    assert query["tag"] == ["one", "two"]
    assert query["backend"] == ["secondary"]
    assert "page" not in query
    assert url.fragment == ""
  assert parse_qs(urlsplit(task["url_toggle"]).query)["sort"] == ["task.priority"]
  assert parse_qs(urlsplit(task["url_remove"]).query)["sort"] == ["priority"]
  overview = tables._overview_headers(
    section="queues",
    query_params=params,
    sort_param="queues_sort",
    sort="name",
    explicit_sort=True,
    page_param="queues_page",
    anchor="queue-summary",
  )
  assert urlsplit(overview[0]["url_primary"]).fragment == "queue-summary"
  assert params.getlist("tag") == ["one", "two"]
  assert params["page"] == "4"


def test_shared_job_columns_do_not_share_mutable_field_definitions():
  assert (
    tables.QUEUE_PAGE_SORTS["ready"]["fields"]["id"]
    == tables.QUEUE_PAGE_SORTS["failed"]["fields"]["id"]
  )
  assert (
    tables.QUEUE_PAGE_SORTS["ready"]["fields"]["id"]
    is not tables.QUEUE_PAGE_SORTS["failed"]["fields"]["id"]
  )


def test_multicolumn_sort_keeps_nulls_last_and_ties_stable():
  rows = [
    {"name": "first", "count": 2, "age": None},
    {"name": "second", "count": 2, "age": None},
    {"name": "young", "count": 2, "age": 1},
    {"name": "old", "count": 2, "age": 3},
    {"name": "missing", "count": None, "age": 5},
  ]
  sorted_rows = tables._sort_rows_by_keys(rows=rows, sort_specs=[("count", True), ("age", True)])
  assert [row["name"] for row in sorted_rows] == ["old", "young", "first", "second", "missing"]
