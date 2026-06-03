from dataclasses import dataclass
from datetime import datetime

from django.db.models import Case, Count, IntegerField, Min, Q, Value, When
from django.db.models.functions import Coalesce

from dj_queue.db import get_database_alias
from dj_queue.models import (
  BlockedExecution,
  ClaimedExecution,
  FailedExecution,
  Job,
  ReadyExecution,
  ScheduledExecution,
)
from dj_queue.models.jobs import (
  INVALID_JOB_STATUS,
  invalid_execution_state_query,
  job_status_query_filter,
)
from dj_queue.sql import common as sql_common


@dataclass(frozen=True, slots=True)
class QueueStateDefinition:
  name: str
  label: str
  query_filter: dict[str, bool]
  query_order: tuple[str, ...]
  rank: int

  @property
  def count_key(self):
    return f"{self.name}_count"


@dataclass(frozen=True, slots=True)
class QueueStateSummary:
  queue_name: str
  state_counts: tuple[tuple[str, int], ...]
  oldest_ready_at: datetime | None = None
  oldest_scheduled_at: datetime | None = None
  oldest_blocked_at: datetime | None = None

  def count(self, state):
    return self.counts_by_state().get(state, 0)

  def counts_by_state(self):
    return dict(self.state_counts)

  def count_fields(self):
    return queue_state_count_fields(self.counts_by_state())


QUEUE_STATE_DEFINITIONS = (
  QueueStateDefinition(
    name="ready",
    label="ready",
    query_filter=job_status_query_filter("ready"),
    query_order=("-priority", "ready_execution__id"),
    rank=0,
  ),
  QueueStateDefinition(
    name="scheduled",
    label="scheduled",
    query_filter=job_status_query_filter("scheduled"),
    query_order=(
      "scheduled_execution__scheduled_at",
      "-priority",
      "scheduled_execution__id",
    ),
    rank=1,
  ),
  QueueStateDefinition(
    name="claimed",
    label="claimed",
    query_filter=job_status_query_filter("claimed"),
    query_order=("claimed_execution__created_at", "id"),
    rank=2,
  ),
  QueueStateDefinition(
    name="blocked",
    label="blocked",
    query_filter=job_status_query_filter("blocked"),
    query_order=("blocked_execution__expires_at", "-priority", "blocked_execution__id"),
    rank=3,
  ),
  QueueStateDefinition(
    name="failed",
    label="failed",
    query_filter=job_status_query_filter("failed"),
    query_order=("-failed_execution__created_at", "id"),
    rank=4,
  ),
  QueueStateDefinition(
    name="finished",
    label="finished",
    query_filter=job_status_query_filter("finished"),
    query_order=("-finished_at", "id"),
    rank=5,
  ),
  QueueStateDefinition(
    name=INVALID_JOB_STATUS,
    label=INVALID_JOB_STATUS,
    query_filter={},
    query_order=("id",),
    rank=6,
  ),
)

QUEUE_STATE_BY_NAME = {definition.name: definition for definition in QUEUE_STATE_DEFINITIONS}
QUEUE_STATES = tuple((definition.name, definition.label) for definition in QUEUE_STATE_DEFINITIONS)
QUEUE_STATE_LABELS = {definition.name: definition.label for definition in QUEUE_STATE_DEFINITIONS}
QUEUE_STATE_COUNT_KEYS = tuple(definition.count_key for definition in QUEUE_STATE_DEFINITIONS)


def queue_state_definition(state):
  return QUEUE_STATE_BY_NAME[state]


def is_queue_state(state):
  return state in QUEUE_STATE_BY_NAME


def queue_state_count_key(state):
  return queue_state_definition(state).count_key


def queue_state_count_fields(counts):
  return {
    definition.count_key: counts.get(definition.name, 0) for definition in QUEUE_STATE_DEFINITIONS
  }


def filter_queue_state(queryset, state):
  definition = queue_state_definition(state)
  return _filter_state_queryset(queryset, definition)


def queue_state_queryset(*, backend_alias, queue_name, state):
  alias = get_database_alias(backend_alias)
  definition = queue_state_definition(state)
  queryset = (
    Job.objects.using(alias)
    .filter(backend_alias=backend_alias, queue_name=queue_name)
    .select_related(
      "ready_execution",
      "scheduled_execution",
      "claimed_execution__process",
      "blocked_execution",
      "failed_execution",
    )
  )
  return _filter_state_queryset(queryset, definition).order_by(*definition.query_order)


def queue_state_counts(*, backend_alias, queue_name):
  return queue_state_summary(backend_alias=backend_alias, queue_name=queue_name).counts_by_state()


def queue_state_summary(*, backend_alias, queue_name):
  alias = get_database_alias(backend_alias)
  if _invalid_execution_state_exists(alias, backend_alias, queue_name=queue_name):
    return _job_state_summary(alias, backend_alias=backend_alias, queue_name=queue_name)
  return _state_table_summaries_by_queue(
    alias,
    backend_alias=backend_alias,
    queue_name=queue_name,
  ).get(queue_name) or empty_queue_state_summary(queue_name)


def queue_state_summaries_by_queue(*, backend_alias):
  alias = get_database_alias(backend_alias)
  return _job_state_summaries_by_queue(alias, backend_alias=backend_alias)


def _job_state_summary(alias, *, backend_alias, queue_name):
  base_queryset = Job.objects.using(alias).filter(
    backend_alias=backend_alias,
    queue_name=queue_name,
  )
  return _summary_from_row(
    queue_name,
    base_queryset.aggregate(**_summary_annotations()),
  )


def _job_state_summaries_by_queue(alias, *, backend_alias):
  base_queryset = Job.objects.using(alias).filter(backend_alias=backend_alias)
  return {
    row["queue_name"]: _summary_from_row(row["queue_name"], row)
    for row in base_queryset.values("queue_name")
    .annotate(**_summary_annotations())
    .order_by("queue_name")
  }


def _state_table_summaries_by_queue(alias, *, backend_alias, queue_name=None):
  rows = {}
  _merge_ready_summary(rows, alias=alias, backend_alias=backend_alias, queue_name=queue_name)
  _merge_scheduled_summary(rows, alias=alias, backend_alias=backend_alias, queue_name=queue_name)
  _merge_blocked_summary(rows, alias=alias, backend_alias=backend_alias, queue_name=queue_name)
  _merge_claimed_summary(rows, alias=alias, backend_alias=backend_alias, queue_name=queue_name)
  _merge_failed_summary(rows, alias=alias, backend_alias=backend_alias, queue_name=queue_name)
  _merge_finished_summary(rows, alias=alias, backend_alias=backend_alias, queue_name=queue_name)
  return {name: _summary_from_row(name, row) for name, row in sorted(rows.items())}


def _empty_summary_values(queue_name):
  return {
    "queue_name": queue_name,
    **{definition.name: 0 for definition in QUEUE_STATE_DEFINITIONS},
    "oldest_ready_at": None,
    "oldest_scheduled_at": None,
    "oldest_blocked_at": None,
  }


def _summary_values(rows, queue_name):
  return rows.setdefault(queue_name, _empty_summary_values(queue_name))


def _merge_ready_summary(rows, *, alias, backend_alias, queue_name):
  queryset = ReadyExecution.objects.using(alias).filter(
    backend_alias=backend_alias,
    job__backend_alias=backend_alias,
  )
  if queue_name is not None:
    queryset = queryset.filter(job__queue_name=queue_name)
  for row in queryset.values("job__queue_name").annotate(
    count=Count("id"),
    oldest_ready_at=Min(Coalesce("latency_started_at", "created_at")),
  ):
    values = _summary_values(rows, row["job__queue_name"])
    values["ready"] = row["count"]
    values["oldest_ready_at"] = row["oldest_ready_at"]


def _merge_scheduled_summary(rows, *, alias, backend_alias, queue_name):
  queryset = ScheduledExecution.objects.using(alias).filter(
    backend_alias=backend_alias,
    job__backend_alias=backend_alias,
  )
  if queue_name is not None:
    queryset = queryset.filter(job__queue_name=queue_name)
  for row in queryset.values("job__queue_name").annotate(
    count=Count("id"),
    oldest_scheduled_at=Min("scheduled_at"),
  ):
    values = _summary_values(rows, row["job__queue_name"])
    values["scheduled"] = row["count"]
    values["oldest_scheduled_at"] = row["oldest_scheduled_at"]


def _merge_blocked_summary(rows, *, alias, backend_alias, queue_name):
  queryset = BlockedExecution.objects.using(alias).filter(
    backend_alias=backend_alias,
    job__backend_alias=backend_alias,
  )
  if queue_name is not None:
    queryset = queryset.filter(job__queue_name=queue_name)
  for row in queryset.values("job__queue_name").annotate(
    count=Count("id"),
    oldest_blocked_at=Min("expires_at"),
  ):
    values = _summary_values(rows, row["job__queue_name"])
    values["blocked"] = row["count"]
    values["oldest_blocked_at"] = row["oldest_blocked_at"]


def _merge_claimed_summary(rows, *, alias, backend_alias, queue_name):
  queryset = ClaimedExecution.objects.using(alias).filter(job__backend_alias=backend_alias)
  if queue_name is not None:
    queryset = queryset.filter(job__queue_name=queue_name)
  for row in queryset.values("job__queue_name").annotate(count=Count("id")):
    _summary_values(rows, row["job__queue_name"])["claimed"] = row["count"]


def _merge_failed_summary(rows, *, alias, backend_alias, queue_name):
  queryset = FailedExecution.objects.using(alias).filter(job__backend_alias=backend_alias)
  if queue_name is not None:
    queryset = queryset.filter(job__queue_name=queue_name)
  for row in queryset.values("job__queue_name").annotate(count=Count("id")):
    _summary_values(rows, row["job__queue_name"])["failed"] = row["count"]


def _merge_finished_summary(rows, *, alias, backend_alias, queue_name):
  queryset = Job.objects.using(alias).filter(
    backend_alias=backend_alias,
    finished_at__isnull=False,
  )
  if queue_name is not None:
    queryset = queryset.filter(queue_name=queue_name)
  for row in queryset.values("queue_name").annotate(count=Count("id")):
    _summary_values(rows, row["queue_name"])["finished"] = row["count"]


def _invalid_execution_state_exists(alias, backend_alias, *, queue_name=None):
  return sql_common.invalid_execution_state_exists(
    alias,
    backend_alias=backend_alias,
    queue_name=queue_name,
  )


def empty_queue_state_summary(queue_name):
  return QueueStateSummary(
    queue_name=queue_name,
    state_counts=_state_counts_tuple({}),
  )


def status_rank_expression():
  return Case(
    *(
      When(_state_query(definition), then=Value(definition.rank))
      for definition in QUEUE_STATE_DEFINITIONS
    ),
    default=Value(99),
    output_field=IntegerField(),
  )


def _queue_state_counts(base_queryset):
  return base_queryset.aggregate(**_state_count_annotations())


def _summary_annotations():
  return {
    **_state_count_annotations(),
    "oldest_ready_at": Min(
      _ready_latency_expression(),
      filter=_state_query(QUEUE_STATE_BY_NAME["ready"]),
    ),
    "oldest_scheduled_at": Min(
      "scheduled_execution__scheduled_at",
      filter=_state_query(QUEUE_STATE_BY_NAME["scheduled"]),
    ),
    "oldest_blocked_at": Min(
      "blocked_execution__expires_at",
      filter=_state_query(QUEUE_STATE_BY_NAME["blocked"]),
    ),
  }


def _state_count_annotations():
  return {
    definition.name: Count("id", filter=_state_query(definition))
    for definition in QUEUE_STATE_DEFINITIONS
  }


def _summary_from_row(queue_name, row):
  return QueueStateSummary(
    queue_name=queue_name,
    state_counts=_state_counts_tuple(row),
    oldest_ready_at=row["oldest_ready_at"],
    oldest_scheduled_at=row["oldest_scheduled_at"],
    oldest_blocked_at=row["oldest_blocked_at"],
  )


def _state_counts_tuple(counts):
  return tuple(
    (definition.name, counts.get(definition.name, 0)) for definition in QUEUE_STATE_DEFINITIONS
  )


def _ready_latency_expression():
  return Coalesce("ready_execution__latency_started_at", "ready_execution__created_at")


def _filter_state_queryset(queryset, definition):
  if definition.name == INVALID_JOB_STATUS:
    return queryset.filter(invalid_execution_state_query())
  return queryset.filter(**definition.query_filter).exclude(invalid_execution_state_query())


def _state_query(definition):
  if definition.name == INVALID_JOB_STATUS:
    return invalid_execution_state_query()
  return Q(**definition.query_filter) & ~invalid_execution_state_query()
