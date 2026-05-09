import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from django.db import connections
from django.tasks import TaskResultStatus
from django.utils import timezone

from dj_queue.models import (
  BlockedExecution,
  ClaimedExecution,
  FailedExecution,
  Job,
  Pause,
  Process,
  ReadyExecution,
  RecurringExecution,
  RecurringTask,
  ScheduledExecution,
  Semaphore,
)
from dj_queue.operations.concurrency import (
  cleanup_expired_semaphores,
  promote_expired_blocked_jobs,
  semaphore_acquire,
  semaphore_release,
)
from dj_queue.operations.recurring import fire_recurring_task
from dj_queue.operations.jobs import (
  claim_ready_jobs,
  complete_claimed_job,
  discard_ready_jobs,
  execute_claimed_job,
  fail_claimed_job,
  promote_scheduled_jobs,
)
from dj_queue.api import QueueInfo
from tests.tasks import echo, limited, other_queue


def make_job(task=echo, **overrides):
  payload = {
    "args": list(overrides.pop("args", [])),
    "kwargs": dict(overrides.pop("kwargs", {})),
  }
  payload.update(overrides.pop("payload", {}))

  return Job.objects.create(
    task_path=overrides.pop("task_path", task.module_path),
    queue_name=overrides.pop("queue_name", task.queue_name),
    priority=overrides.pop("priority", task.priority),
    payload=payload,
    backend_alias=overrides.pop("backend_alias", task.backend),
    scheduled_at=overrides.pop("scheduled_at", None),
    concurrency_key=overrides.pop("concurrency_key", None),
    finished_at=overrides.pop("finished_at", None),
    return_value=overrides.pop("return_value", None),
    **overrides,
  )


@pytest.mark.django_db
def test_semaphore_acquire_release_cycle():
  assert semaphore_acquire("account:1", limit=1, duration_seconds=60) is True
  assert semaphore_acquire("account:1", limit=1, duration_seconds=60) is False
  assert semaphore_release("account:1", duration_seconds=60) is True
  assert semaphore_acquire("account:1", limit=1, duration_seconds=60) is True


@pytest.mark.django_db
def test_semaphore_signal_caps_at_limit():
  semaphore_acquire("account:1", limit=2, duration_seconds=60)
  semaphore_release("account:1", duration_seconds=60)
  semaphore_release("account:1", duration_seconds=60)

  semaphore = Semaphore.objects.get(key="account:1")

  assert semaphore.value == semaphore.limit == 2


@pytest.mark.skipif(
  os.environ.get("DB_BACKEND", "sqlite") == "sqlite",
  reason="requires a shared test database across threads",
)
@pytest.mark.django_db(transaction=True)
def test_concurrent_acquire_allows_exactly_limit_successes():
  limit = 2
  attempts = 5
  barrier = threading.Barrier(attempts)

  def acquire_once():
    try:
      barrier.wait()
      return semaphore_acquire("account:concurrent", limit=limit, duration_seconds=60)
    finally:
      connections.close_all()

  with ThreadPoolExecutor(max_workers=attempts) as executor:
    results = list(executor.map(lambda _: acquire_once(), range(attempts)))

  assert results.count(True) == limit
  assert results.count(False) == attempts - limit
  assert Semaphore.objects.get(key="account:concurrent").value == 0


@pytest.mark.skipif(
  os.environ.get("DB_BACKEND", "sqlite") == "sqlite",
  reason="requires a shared test database across threads",
)
@pytest.mark.django_db(transaction=True)
def test_concurrent_first_acquire_creates_one_semaphore_row():
  attempts = 2
  barrier = threading.Barrier(attempts)

  def acquire_once():
    try:
      barrier.wait()
      return semaphore_acquire("account:first", limit=1, duration_seconds=60)
    finally:
      connections.close_all()

  with ThreadPoolExecutor(max_workers=attempts) as executor:
    results = list(executor.map(lambda _: acquire_once(), range(attempts)))

  assert results.count(True) == 1
  assert results.count(False) == 1
  assert Semaphore.objects.filter(key="account:first").count() == 1
  assert Semaphore.objects.get(key="account:first").value == 0


@pytest.mark.skipif(
  os.environ.get("DB_BACKEND", "sqlite") == "sqlite",
  reason="requires a shared test database across threads",
)
@pytest.mark.django_db(transaction=True)
def test_concurrent_recurring_fire_creates_one_reservation_and_job():
  attempts = 2
  barrier = threading.Barrier(attempts)
  recurring_task = RecurringTask.objects.create(
    backend_alias="default",
    key="every-minute",
    task_path=echo.module_path,
    payload={"args": ["hello"], "kwargs": {}},
    schedule="* * * * *",
    queue_name=echo.queue_name,
    priority=echo.priority,
  )
  run_at = timezone.now().replace(second=0, microsecond=0) + timedelta(minutes=1)

  def fire_once():
    try:
      barrier.wait()
      execution = fire_recurring_task(recurring_task, run_at)
      return execution is not None
    finally:
      connections.close_all()

  with ThreadPoolExecutor(max_workers=attempts) as executor:
    results = list(executor.map(lambda _: fire_once(), range(attempts)))

  assert results.count(True) == 1
  assert results.count(False) == 1
  assert (
    RecurringExecution.objects.filter(
      backend_alias="default",
      task_key=recurring_task.key,
      run_at=run_at,
    ).count()
    == 1
  )
  assert Job.objects.count() == 1


@pytest.mark.django_db
def test_enqueue_with_concurrency_slot_available_goes_ready():
  result = limited.enqueue(1, value="first")

  job = ReadyExecution.objects.get(job_id=result.id).job
  semaphore = Semaphore.objects.get(key="account:1")

  assert result.status.name == "READY"
  assert job.concurrency_key == "account:1"
  assert semaphore.value == 0
  assert semaphore.limit == 1


@pytest.mark.django_db
def test_enqueue_with_concurrency_limit_reached_goes_blocked():
  limited.enqueue(1, value="first")
  second = limited.enqueue(1, value="second")
  job = Job.objects.get(pk=second.id)

  assert ReadyExecution.objects.count() == 1
  assert job.blocked is True
  assert job.concurrency_key == "account:1"
  assert job.blocked_execution.concurrency_key == "account:1"
  assert limited.get_backend().get_result(second.id).status == TaskResultStatus.READY


@pytest.mark.django_db
def test_successful_completion_unblocks_next_waiter():
  first = limited.enqueue(1, value="first")
  second = limited.enqueue(1, value="second")

  claimed_jobs = claim_ready_jobs(limit=1)
  complete_claimed_job(first.id, "done")

  assert [str(job.id) for job in claimed_jobs] == [first.id]
  assert ReadyExecution.objects.filter(job_id=second.id).exists() is True
  assert Semaphore.objects.get(key="account:1").value == 0


@pytest.mark.django_db
def test_failed_completion_still_unblocks_next_waiter():
  first = limited.enqueue(1, value="first")
  second = limited.enqueue(1, value="second")

  claim_ready_jobs(limit=1)
  fail_claimed_job(first.id, ValueError("boom"), traceback_text="traceback")

  assert FailedExecution.objects.filter(job_id=first.id).exists() is True
  assert ReadyExecution.objects.filter(job_id=second.id).exists() is True
  assert Semaphore.objects.get(key="account:1").value == 0


@pytest.mark.django_db(transaction=True)
def test_missing_concurrency_task_path_releases_slot_and_unblocks_next_waiter():
  process = Process.objects.create(
    backend_alias="default",
    kind="Worker",
    pid=12345,
    hostname="localhost",
    name="worker-missing-concurrency-task",
    metadata={},
    last_heartbeat_at=timezone.now(),
  )
  first = make_job(
    task_path="tests.tasks.missing_concurrency_task",
    concurrency_key="account:missing-task",
  )
  second = make_job(concurrency_key="account:missing-task")
  Semaphore.objects.create(
    key="account:missing-task",
    value=0,
    limit=1,
    expires_at=timezone.now() + timedelta(seconds=60),
  )
  ClaimedExecution.objects.create(job=first, process=process)
  BlockedExecution.objects.create(
    job=second,
    queue_name=second.queue_name,
    priority=second.priority,
    concurrency_key="account:missing-task",
    expires_at=timezone.now() + timedelta(seconds=60),
  )

  execute_claimed_job(first)

  assert FailedExecution.objects.filter(job=first).exists() is True
  assert ReadyExecution.objects.filter(job=second).exists() is True
  assert BlockedExecution.objects.filter(job=second).exists() is False


@pytest.mark.django_db
def test_dispatcher_promotes_expired_blocked_jobs():
  job = make_job(task=limited, args=[1], kwargs={"value": "later"}, concurrency_key="account:1")
  BlockedExecution.objects.create(
    job=job,
    queue_name=job.queue_name,
    priority=job.priority,
    concurrency_key=job.concurrency_key,
    expires_at=timezone.now() - timedelta(seconds=1),
  )

  promoted = promote_expired_blocked_jobs(batch_size=10)

  assert [promoted_job.id for promoted_job in promoted] == [job.id]
  assert BlockedExecution.objects.filter(job=job).exists() is False
  assert ReadyExecution.objects.filter(job=job).exists() is True
  assert Semaphore.objects.get(key="account:1").value == 0


@pytest.mark.django_db
def test_expired_semaphore_cleanup_preserves_active_claimed_key():
  first = limited.enqueue(1, value="first")
  second = limited.enqueue(1, value="second")
  claim_ready_jobs(limit=1)
  Semaphore.objects.filter(key="account:1").update(
    expires_at=timezone.now() - timedelta(seconds=1)
  )
  BlockedExecution.objects.filter(job_id=second.id).update(
    expires_at=timezone.now() - timedelta(seconds=1)
  )

  assert cleanup_expired_semaphores() == 0
  assert promote_expired_blocked_jobs(batch_size=10) == []
  assert ClaimedExecution.objects.filter(job_id=first.id).exists() is True
  assert BlockedExecution.objects.filter(job_id=second.id).exists() is True
  assert ReadyExecution.objects.filter(job_id=second.id).exists() is False


@pytest.mark.django_db
def test_promote_expired_blocked_jobs_reuses_task_import_for_shared_task_path(monkeypatch):
  imported = []

  def fake_import_string(path):
    imported.append(path)
    from django.utils.module_loading import import_string

    return import_string(path)

  monkeypatch.setattr("dj_queue.operations.concurrency.import_string", fake_import_string)

  for account_id in (1, 2):
    job = make_job(
      task=limited,
      args=[account_id],
      kwargs={"value": f"later-{account_id}"},
      concurrency_key=f"account:{account_id}",
    )
    BlockedExecution.objects.create(
      job=job,
      queue_name=job.queue_name,
      priority=job.priority,
      concurrency_key=job.concurrency_key,
      expires_at=timezone.now() - timedelta(seconds=1),
    )

  promote_expired_blocked_jobs(batch_size=10)

  assert imported == [limited.module_path]


@pytest.mark.django_db
def test_promote_expired_blocked_jobs_uses_backend_default_concurrency_duration(
  monkeypatch, settings
):
  settings.TASKS = {
    **settings.TASKS,
    "default": {
      **settings.TASKS["default"],
      "OPTIONS": {
        **settings.TASKS["default"].get("OPTIONS", {}),
        "default_concurrency_duration": 240,
      },
    },
  }
  monkeypatch.delattr(limited.func, "concurrency_duration", raising=False)
  job = make_job(task=limited, args=[1], kwargs={"value": "later"}, concurrency_key="account:1")
  BlockedExecution.objects.create(
    job=job,
    queue_name=job.queue_name,
    priority=job.priority,
    concurrency_key=job.concurrency_key,
    expires_at=timezone.now() - timedelta(seconds=1),
  )
  Semaphore.objects.create(
    key="account:1",
    value=0,
    limit=1,
    expires_at=timezone.now() + timedelta(seconds=240),
  )
  before_promote = timezone.now()

  assert promote_expired_blocked_jobs(batch_size=10) == []

  blocked = BlockedExecution.objects.get(job=job)
  assert blocked.expires_at >= before_promote + timedelta(seconds=200)


@pytest.mark.django_db
def test_discarding_ready_job_releases_waiter():
  first = limited.enqueue(1, value="first")
  second = limited.enqueue(1, value="second")

  deleted = discard_ready_jobs(job_ids=[first.id], batch_size=1)

  assert deleted == 1
  assert ReadyExecution.objects.filter(job_id=second.id).exists() is True
  assert Semaphore.objects.get(key="account:1").value == 0


@pytest.mark.django_db
def test_queue_pause_blocks_claiming_not_enqueue():
  Pause.objects.create(backend_alias="default", queue_name="other")
  other_queue.enqueue("paused")

  claimed_jobs = claim_ready_jobs(limit=1, queues=("other",))

  assert ReadyExecution.objects.filter(queue_name="other").count() == 1
  assert claimed_jobs == []


@pytest.mark.django_db(transaction=True)
def test_claim_rechecks_pause_created_during_claim(monkeypatch):
  job = make_job(queue_name="critical")
  ReadyExecution.objects.create(job=job, queue_name="critical", priority=0)
  original_select_ready_rows = __import__(
    "dj_queue.operations.jobs", fromlist=["_select_ready_rows"]
  )._select_ready_rows

  def pause_during_selection(*args, **kwargs):
    rows = original_select_ready_rows(*args, **kwargs)
    QueueInfo("critical").pause()
    return rows

  monkeypatch.setattr("dj_queue.operations.jobs._select_ready_rows", pause_during_selection)

  claimed_jobs = claim_ready_jobs(limit=1, queues=("critical",))

  assert claimed_jobs == []
  assert ReadyExecution.objects.filter(job=job).exists() is True
  assert ClaimedExecution.objects.filter(job=job).exists() is False


@pytest.mark.skipif(
  os.environ.get("DB_BACKEND", "sqlite") != "sqlite",
  reason="simulates SQLite's lack of row-level locks",
)
@pytest.mark.django_db(transaction=True)
def test_sqlite_claim_skips_ready_row_consumed_after_selection(monkeypatch):
  job = make_job()
  ReadyExecution.objects.create(job=job, queue_name=job.queue_name, priority=job.priority)
  original_select_ready_rows = __import__(
    "dj_queue.operations.jobs", fromlist=["_select_ready_rows"]
  )._select_ready_rows

  def consume_during_selection(*args, **kwargs):
    rows = original_select_ready_rows(*args, **kwargs)
    ReadyExecution.objects.filter(pk__in=[row.pk for row in rows]).delete()
    return rows

  monkeypatch.setattr("dj_queue.operations.jobs._select_ready_rows", consume_during_selection)

  assert claim_ready_jobs(limit=1) == []
  assert ClaimedExecution.objects.filter(job=job).exists() is False


@pytest.mark.skipif(
  os.environ.get("DB_BACKEND", "sqlite") != "sqlite",
  reason="simulates SQLite's lack of row-level locks",
)
@pytest.mark.django_db(transaction=True)
def test_sqlite_promote_skips_scheduled_row_consumed_after_selection(monkeypatch):
  scheduled_at = timezone.now() - timedelta(seconds=1)
  job = make_job(scheduled_at=scheduled_at)
  ScheduledExecution.objects.create(
    job=job,
    queue_name=job.queue_name,
    priority=job.priority,
    scheduled_at=scheduled_at,
  )
  original_locked_queryset = __import__(
    "dj_queue.operations.jobs", fromlist=["locked_queryset"]
  ).locked_queryset

  class ConsumingQuerySet:
    def __init__(self, queryset):
      self.queryset = queryset

    def __getitem__(self, index):
      rows = list(self.queryset[index])
      ScheduledExecution.objects.filter(pk__in=[row.pk for row in rows]).delete()
      return rows

  def consume_during_selection(queryset, *, use_skip_locked=True):
    return ConsumingQuerySet(original_locked_queryset(queryset, use_skip_locked=use_skip_locked))

  monkeypatch.setattr("dj_queue.operations.jobs.locked_queryset", consume_during_selection)

  assert promote_scheduled_jobs(batch_size=10) == []
  assert ReadyExecution.objects.filter(job=job).exists() is False


@pytest.mark.skipif(
  os.environ.get("DB_BACKEND", "sqlite") != "sqlite",
  reason="simulates SQLite's lack of row-level locks",
)
@pytest.mark.django_db(transaction=True)
def test_sqlite_promote_skips_blocked_row_consumed_after_selection(monkeypatch):
  job = make_job(task=limited, args=[1], kwargs={"value": "later"}, concurrency_key="account:1")
  BlockedExecution.objects.create(
    job=job,
    queue_name=job.queue_name,
    priority=job.priority,
    concurrency_key=job.concurrency_key,
    expires_at=timezone.now() - timedelta(seconds=1),
  )
  original_locked_queryset = __import__(
    "dj_queue.operations.concurrency", fromlist=["locked_queryset"]
  ).locked_queryset

  class ConsumingQuerySet:
    def __init__(self, queryset):
      self.queryset = queryset

    def __getitem__(self, index):
      rows = list(self.queryset[index])
      BlockedExecution.objects.filter(pk__in=[row.pk for row in rows]).delete()
      return rows

  def consume_during_selection(queryset, *, use_skip_locked=True):
    return ConsumingQuerySet(original_locked_queryset(queryset, use_skip_locked=use_skip_locked))

  monkeypatch.setattr("dj_queue.operations.concurrency.locked_queryset", consume_during_selection)

  assert promote_expired_blocked_jobs(batch_size=10) == []
  assert ReadyExecution.objects.filter(job=job).exists() is False


@pytest.mark.django_db
def test_queue_resume_restores_claiming():
  pause = Pause.objects.create(backend_alias="default", queue_name="other")
  result = other_queue.enqueue("paused")

  assert claim_ready_jobs(limit=1, queues=("other",)) == []

  pause.delete()
  claimed_jobs = claim_ready_jobs(limit=1, queues=("other",))

  assert [str(job.id) for job in claimed_jobs] == [result.id]


@pytest.mark.django_db
def test_queue_selector_exact_prefix_and_star_ordering():
  alpha = echo.using(queue_name="alpha").enqueue("alpha")
  mail = echo.using(queue_name="mailers").enqueue("mail")
  default = echo.enqueue("default")

  claimed_jobs = claim_ready_jobs(limit=3, queues=("alpha", "mail*", "*"))

  assert [str(job.id) for job in claimed_jobs] == [alpha.id, mail.id, default.id]


@pytest.mark.django_db
def test_claim_ready_jobs_bulk_inserts_claimed_rows_for_full_batch():
  jobs = [make_job(args=[index]) for index in range(3)]
  for job in jobs:
    ReadyExecution.objects.create(job=job, queue_name=job.queue_name, priority=job.priority)

  claimed_jobs = claim_ready_jobs(limit=3)

  assert [job.id for job in claimed_jobs] == [jobs[0].id, jobs[1].id, jobs[2].id]
  assert ClaimedExecution.objects.count() == 3
  assert ReadyExecution.objects.count() == 0


@pytest.mark.django_db
def test_cleanup_expired_semaphores():
  Semaphore.objects.create(
    key="expired",
    value=0,
    limit=1,
    expires_at=timezone.now() - timedelta(seconds=1),
  )
  Semaphore.objects.create(
    key="fresh",
    value=0,
    limit=1,
    expires_at=timezone.now() + timedelta(minutes=1),
  )

  deleted = cleanup_expired_semaphores()

  assert deleted == 1
  assert Semaphore.objects.filter(key="expired").exists() is False
  assert Semaphore.objects.filter(key="fresh").exists() is True
