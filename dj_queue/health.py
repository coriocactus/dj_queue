from django.db.models import Count, F, Q
from django.utils import timezone

from dj_queue.config import load_backend_config
from dj_queue.db import get_database_alias
from dj_queue.models import (
  BlockedExecution,
  ClaimedExecution,
  Job,
  Process,
  ReadyExecution,
  RecurringExecution,
  ScheduledExecution,
  Semaphore,
)
from dj_queue.postgres_diagnostics import (
  postgres_diagnostics_for_backend,
  postgres_xmin_blocker_rows,
)
from dj_queue.reads.jobs import failed_job_metrics
from dj_queue.reads.processes import process_cutoff_for_backend
from dj_queue.runtime.base import ROLLOUT_PROTOCOL_VERSION

POSTGRES_DEAD_TUPLE_WARNING_COUNT = 10_000


POSTGRES_DEAD_TUPLE_WARNING_RATIO = 0.2


def deep_health_problems(*, backend_alias, max_age=None, now=None, required_process_version=None):
  if now is None:
    now = timezone.now()
  alias = get_database_alias(backend_alias)
  process_cutoff = process_cutoff_for_backend(backend_alias, now=now, max_age=max_age)
  return (
    *_process_health_problems(
      alias=alias,
      backend_alias=backend_alias,
      process_cutoff=process_cutoff,
      required_process_version=required_process_version,
    ),
    *_job_health_problems(alias=alias, backend_alias=backend_alias),
    *_claim_health_problems(
      alias=alias, backend_alias=backend_alias, process_cutoff=process_cutoff
    ),
    *_recurring_health_problems(alias=alias, backend_alias=backend_alias),
    *_semaphore_health_problems(alias=alias),
    *_retention_health_problems(backend_alias=backend_alias, now=now),
    *postgres_health_problems(backend_alias=backend_alias, max_age=max_age, now=now),
  )


def _process_health_problems(*, alias, backend_alias, process_cutoff, required_process_version):
  problems = []
  live_process_metadata = (
    Process.objects.using(alias)
    .filter(
      backend_alias=backend_alias,
      last_heartbeat_at__gte=process_cutoff,
    )
    .values_list("metadata", flat=True)
  )
  incompatible_processes = sum(
    not isinstance(metadata, dict) or metadata.get("rollout_protocol") != ROLLOUT_PROTOCOL_VERSION
    for metadata in live_process_metadata
  )
  if incompatible_processes:
    problems.append(
      f"{incompatible_processes} live processes use an incompatible rollout protocol; "
      f"expected {ROLLOUT_PROTOCOL_VERSION}"
    )
  if required_process_version is not None:
    wrong_version_processes = sum(
      not isinstance(metadata, dict)
      or metadata.get("dj_queue_version") != required_process_version
      for metadata in live_process_metadata
    )
    if wrong_version_processes:
      problems.append(
        f"{wrong_version_processes} live processes do not run required dj_queue version "
        f"{required_process_version}"
      )
  return problems


def _job_health_problems(*, alias, backend_alias):
  problems = []
  invalid_jobs = (
    Job.objects.using(alias).filter(backend_alias=backend_alias).invalid_execution_state().count()
  )
  if invalid_jobs:
    problems.append(f"{invalid_jobs} jobs have invalid execution state")

  invalid_policies = (
    Job.objects.using(alias)
    .filter(backend_alias=backend_alias)
    .exclude(
      Q(
        concurrency_limit__isnull=True,
        concurrency_duration__isnull=True,
        concurrency_on_conflict__isnull=True,
      )
      | Q(
        concurrency_key__isnull=False,
        concurrency_limit__gte=1,
        concurrency_duration__gte=1,
        concurrency_on_conflict__in=("block", "discard"),
      )
      & ~Q(concurrency_key="")
    )
    .count()
  )
  if invalid_policies:
    problems.append(f"{invalid_policies} jobs have invalid concurrency policy")

  for label, model in _backend_owned_state_models():
    mismatched = _state_ownership_mismatch_counts(model, alias=alias, backend_alias=backend_alias)
    if mismatched["backend"]:
      problems.append(
        f"{mismatched['backend']} {label} execution rows have mismatched backend ownership"
      )
    if mismatched["queue"]:
      problems.append(
        f"{mismatched['queue']} {label} execution rows have mismatched queue ownership"
      )
  return problems


def _claim_health_problems(*, alias, backend_alias, process_cutoff):
  problems = []
  bad_claims = (
    ClaimedExecution.objects.using(alias)
    .filter(job__backend_alias=backend_alias)
    .filter(
      Q(process__isnull=True)
      | Q(process__backend_alias__isnull=True)
      | ~Q(process__backend_alias=backend_alias)
      | Q(process__last_heartbeat_at__lt=process_cutoff)
    )
    .count()
  )
  if bad_claims:
    problems.append(f"{bad_claims} claimed execution rows have missing or stale processes")
  return problems


def _recurring_health_problems(*, alias, backend_alias):
  problems = []
  recurring_without_jobs = (
    RecurringExecution.objects.using(alias)
    .filter(
      backend_alias=backend_alias,
      intended_job_id__isnull=False,
      job__isnull=True,
    )
    .count()
  )
  if recurring_without_jobs:
    problems.append(f"{recurring_without_jobs} recurring execution reservations have no job")

  recurring_mismatched = (
    RecurringExecution.objects.using(alias)
    .filter(
      Q(backend_alias=backend_alias) | Q(job__backend_alias=backend_alias), job__isnull=False
    )
    .exclude(backend_alias=F("job__backend_alias"))
    .count()
  )
  if recurring_mismatched:
    problems.append(
      f"{recurring_mismatched} recurring execution rows have mismatched backend ownership"
    )

  recurring_identity_mismatched = (
    RecurringExecution.objects.using(alias)
    .filter(backend_alias=backend_alias, intended_job_id__isnull=False, job__isnull=False)
    .exclude(intended_job_id=F("job_id"))
    .count()
  )
  if recurring_identity_mismatched:
    problems.append(
      f"{recurring_identity_mismatched} recurring execution rows have mismatched job identity"
    )
  return problems


def _semaphore_health_problems(*, alias):
  problems = []
  bad_semaphores = (
    Semaphore.objects.using(alias)
    .filter(Q(limit__lt=1) | Q(active_count__lt=0) | Q(value__lt=0) | Q(value__gt=F("limit")))
    .count()
  )
  if bad_semaphores:
    problems.append(f"{bad_semaphores} semaphores have impossible slot counts")
  return problems


def _retention_health_problems(*, backend_alias, now):
  problems = []
  failed_metrics = failed_job_metrics(
    backend_alias=backend_alias,
    now=now,
    retention_seconds=load_backend_config(backend_alias).clear_failed_jobs_after,
  )
  if failed_metrics["over_retention_count"]:
    problems.append(
      f"{failed_metrics['over_retention_count']} failed execution rows exceed "
      f"configured retention of {failed_metrics['retention_seconds']} seconds"
    )
  return problems


def postgres_health_problems(*, backend_alias, max_age=None, now=None):
  diagnostics = postgres_diagnostics_for_backend(
    backend_alias=backend_alias,
    max_age=max_age,
    now=now,
  )
  if not diagnostics or diagnostics.get("error"):
    return ()

  bloated_tables = [
    row
    for row in diagnostics["queue_tables"]
    if row["dead_tuples"] >= POSTGRES_DEAD_TUPLE_WARNING_COUNT
    and row["dead_tuple_ratio"] >= POSTGRES_DEAD_TUPLE_WARNING_RATIO
  ]
  if not bloated_tables:
    return ()

  table_names = ", ".join(row["table_name"] for row in bloated_tables[:5])
  problems = [
    f"{len(bloated_tables)} PostgreSQL queue tables have high dead tuples: {table_names}"
  ]
  xmin_blockers = postgres_xmin_blocker_rows(diagnostics)
  if xmin_blockers:
    problems.append(f"{len(xmin_blockers)} PostgreSQL sessions or slots may be pinning xmin")
  return tuple(problems)


def _backend_owned_state_models():
  return (
    ("ready", ReadyExecution),
    ("scheduled", ScheduledExecution),
    ("blocked", BlockedExecution),
  )


def _state_ownership_mismatch_counts(model, *, alias, backend_alias):
  return (
    model.objects.using(alias)
    .filter(Q(backend_alias=backend_alias) | Q(job__backend_alias=backend_alias))
    .aggregate(
      backend=Count("id", filter=~Q(backend_alias=F("job__backend_alias"))),
      queue=Count("id", filter=~Q(queue_name=F("job__queue_name"))),
    )
  )
