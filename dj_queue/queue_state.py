from dataclasses import dataclass

from django.db.models import Case, IntegerField, Value, When

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
  return {
    definition.name: base_queryset.filter(**definition.query_filter).count()
    for definition in QUEUE_STATE_DEFINITIONS
  }


def status_rank_expression():
  return Case(
    *(
      When(**definition.query_filter, then=Value(definition.rank))
      for definition in QUEUE_STATE_DEFINITIONS
    ),
    default=Value(99),
    output_field=IntegerField(),
  )
