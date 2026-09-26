from datetime import timedelta

from django.db.models import (
  Count,
  Min,
)
from django.utils import timezone

from dj_queue.db import get_database_alias
from dj_queue.models import (
  FailedExecution,
)


def failed_job_metrics(*, backend_alias, now=None, retention_seconds=None):
  if now is None:
    now = timezone.now()
  alias = get_database_alias(backend_alias)
  queryset = FailedExecution.objects.using(alias).filter(job__backend_alias=backend_alias)
  metrics = _failed_job_aggregate(queryset, now=now)
  metrics["retention_seconds"] = retention_seconds
  metrics["over_retention_count"] = 0
  metrics["oldest_over_retention_created_at"] = None
  metrics["oldest_over_retention_age_seconds"] = None
  if retention_seconds is None:
    return metrics

  cutoff = now - timedelta(seconds=retention_seconds)
  over_retention = _failed_job_aggregate(queryset.filter(created_at__lt=cutoff), now=now)
  metrics["over_retention_count"] = over_retention["count"]
  metrics["oldest_over_retention_created_at"] = over_retention["oldest_created_at"]
  metrics["oldest_over_retention_age_seconds"] = over_retention["oldest_age_seconds"]
  return metrics


def _failed_job_aggregate(queryset, *, now):
  aggregate = queryset.aggregate(count=Count("id"), oldest_created_at=Min("created_at"))
  oldest_created_at = aggregate["oldest_created_at"]
  return {
    "count": aggregate["count"],
    "oldest_created_at": oldest_created_at,
    "oldest_age_seconds": _age_seconds(now, oldest_created_at),
  }


def _age_seconds(now, timestamp):
  if timestamp is None:
    return None
  return max((now - timestamp).total_seconds(), 0.0)
