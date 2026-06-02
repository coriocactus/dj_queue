from django.db import transaction
from django.db.models import Case, DateTimeField, DurationField, ExpressionWrapper, F, Value, When
from django.utils import timezone

from dj_queue.db import get_database_alias
from dj_queue.log import log_event
from dj_queue.models import Pause, ReadyExecution


def pause_queue(queue_name, *, backend_alias="default"):
  alias = get_database_alias(backend_alias)
  with transaction.atomic(using=alias):
    Pause.objects.using(alias).get_or_create(
      backend_alias=backend_alias,
      queue_name=queue_name,
    )

  with transaction.atomic(using=alias):
    list(
      ReadyExecution.objects.using(alias)
      .select_for_update()
      .filter(backend_alias=backend_alias, queue_name=queue_name)
      .values_list("pk", flat=True)
    )
  log_event("queue.paused", backend_alias=backend_alias, queue_name=queue_name)


def resume_queue(queue_name, *, backend_alias="default", resumed_at=None):
  alias = get_database_alias(backend_alias)
  with transaction.atomic(using=alias):
    pause = (
      Pause.objects.using(alias)
      .select_for_update()
      .filter(backend_alias=backend_alias, queue_name=queue_name)
      .first()
    )
    if pause is None:
      return False

    if resumed_at is None:
      resumed_at = timezone.now()
    paused_at = pause.created_at
    pause_duration = Value(resumed_at - paused_at, output_field=DurationField())
    ReadyExecution.objects.using(alias).filter(
      backend_alias=backend_alias,
      queue_name=queue_name,
    ).update(
      latency_started_at=Case(
        When(
          latency_started_at__isnull=True,
          created_at__lt=paused_at,
          then=ExpressionWrapper(F("created_at") + pause_duration, output_field=DateTimeField()),
        ),
        When(
          latency_started_at__lt=paused_at,
          then=ExpressionWrapper(
            F("latency_started_at") + pause_duration, output_field=DateTimeField()
          ),
        ),
        default=Value(resumed_at, output_field=DateTimeField()),
        output_field=DateTimeField(),
      ),
    )
    pause.delete()
  log_event("queue.resumed", backend_alias=backend_alias, queue_name=queue_name)
  return True
