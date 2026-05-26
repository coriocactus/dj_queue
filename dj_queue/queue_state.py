from dataclasses import dataclass
from datetime import datetime

from django.db.models import Case, Count, IntegerField, Min, Value, When
from django.db.models.functions import Coalesce

from dj_queue.db import get_database_alias
from dj_queue.models import Job


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
    query_filter={"ready_execution__isnull": False},
    query_order=("-priority", "ready_execution__id"),
    rank=0,
  ),
  QueueStateDefinition(
    name="scheduled",
    label="scheduled",
    query_filter={"scheduled_execution__isnull": False},
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
    query_filter={"claimed_execution__isnull": False},
    query_order=("claimed_execution__created_at", "id"),
    rank=2,
  ),
  QueueStateDefinition(
    name="blocked",
    label="blocked",
    query_filter={"blocked_execution__isnull": False},
    query_order=("blocked_execution__expires_at", "-priority", "blocked_execution__id"),
    rank=3,
  ),
  QueueStateDefinition(
    name="failed",
    label="failed",
    query_filter={"failed_execution__isnull": False},
    query_order=("-failed_execution__created_at", "id"),
    rank=4,
  ),
  QueueStateDefinition(
    name="finished",
    label="finished",
    query_filter={"finished_at__isnull": False},
    query_order=("-finished_at", "id"),
    rank=5,
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
  return queryset.filter(**definition.query_filter)


def queue_state_queryset(*, backend_alias, queue_name, state):
  alias = get_database_alias(backend_alias)
  definition = queue_state_definition(state)
  return (
    Job.objects.using(alias)
    .filter(backend_alias=backend_alias, queue_name=queue_name)
    .select_related(
      "ready_execution",
      "scheduled_execution",
      "claimed_execution__process",
      "blocked_execution",
      "failed_execution",
    )
    .filter(**definition.query_filter)
    .order_by(*definition.query_order)
  )


def queue_state_counts(*, backend_alias, queue_name):
  alias = get_database_alias(backend_alias)
  base_queryset = Job.objects.using(alias).filter(
    backend_alias=backend_alias,
    queue_name=queue_name,
  )
  return _queue_state_counts(base_queryset)


def queue_state_summary(*, backend_alias, queue_name):
  alias = get_database_alias(backend_alias)
  base_queryset = Job.objects.using(alias).filter(
    backend_alias=backend_alias,
    queue_name=queue_name,
  )
  return QueueStateSummary(
    queue_name=queue_name,
    state_counts=_state_counts_tuple(_queue_state_counts(base_queryset)),
    oldest_ready_at=_oldest_value(
      base_queryset,
      state="ready",
      expression=_ready_latency_expression(),
    ),
    oldest_scheduled_at=_oldest_value(
      base_queryset,
      state="scheduled",
      expression="scheduled_execution__scheduled_at",
    ),
    oldest_blocked_at=_oldest_value(
      base_queryset,
      state="blocked",
      expression="blocked_execution__expires_at",
    ),
  )


def queue_state_summaries_by_queue(*, backend_alias):
  alias = get_database_alias(backend_alias)
  base_queryset = Job.objects.using(alias).filter(backend_alias=backend_alias)
  counts_by_queue = {}
  queue_names = set()

  for definition in QUEUE_STATE_DEFINITIONS:
    for row in (
      base_queryset.filter(**definition.query_filter)
      .values("queue_name")
      .annotate(count=Count("id"))
    ):
      queue_name = row["queue_name"]
      queue_names.add(queue_name)
      counts_by_queue.setdefault(queue_name, {})[definition.name] = row["count"]

  oldest_ready = _oldest_values_by_queue(
    base_queryset,
    state="ready",
    expression=_ready_latency_expression(),
  )
  oldest_scheduled = _oldest_values_by_queue(
    base_queryset,
    state="scheduled",
    expression="scheduled_execution__scheduled_at",
  )
  oldest_blocked = _oldest_values_by_queue(
    base_queryset,
    state="blocked",
    expression="blocked_execution__expires_at",
  )
  queue_names.update(oldest_ready)
  queue_names.update(oldest_scheduled)
  queue_names.update(oldest_blocked)

  return {
    queue_name: QueueStateSummary(
      queue_name=queue_name,
      state_counts=_state_counts_tuple(counts_by_queue.get(queue_name, {})),
      oldest_ready_at=oldest_ready.get(queue_name),
      oldest_scheduled_at=oldest_scheduled.get(queue_name),
      oldest_blocked_at=oldest_blocked.get(queue_name),
    )
    for queue_name in sorted(queue_names)
  }


def empty_queue_state_summary(queue_name):
  return QueueStateSummary(
    queue_name=queue_name,
    state_counts=_state_counts_tuple({}),
  )


def status_rank_expression():
  return Case(
    *(
      When(**definition.query_filter, then=Value(definition.rank))
      for definition in QUEUE_STATE_DEFINITIONS
    ),
    default=Value(99),
    output_field=IntegerField(),
  )


def _queue_state_counts(base_queryset):
  return {
    definition.name: base_queryset.filter(**definition.query_filter).count()
    for definition in QUEUE_STATE_DEFINITIONS
  }


def _state_counts_tuple(counts):
  return tuple(
    (definition.name, counts.get(definition.name, 0)) for definition in QUEUE_STATE_DEFINITIONS
  )


def _oldest_value(base_queryset, *, state, expression):
  definition = queue_state_definition(state)
  return base_queryset.filter(**definition.query_filter).aggregate(oldest=Min(expression))[
    "oldest"
  ]


def _oldest_values_by_queue(base_queryset, *, state, expression):
  definition = queue_state_definition(state)
  return {
    row["queue_name"]: row["oldest"]
    for row in base_queryset.filter(**definition.query_filter)
    .values("queue_name")
    .annotate(oldest=Min(expression))
  }


def _ready_latency_expression():
  return Coalesce("ready_execution__latency_started_at", "ready_execution__created_at")
