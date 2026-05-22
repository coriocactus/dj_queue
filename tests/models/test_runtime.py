import pytest
from django.db import IntegrityError, transaction
from django.utils import timezone

from dj_queue.models import Pause, Process, Semaphore


def make_process(**overrides):
  return Process.objects.create(
    backend_alias=overrides.pop("backend_alias", "default"),
    kind=overrides.pop("kind", "Worker"),
    pid=overrides.pop("pid", 12345),
    hostname=overrides.pop("hostname", "localhost"),
    name=overrides.pop("name", "worker-1"),
    metadata=overrides.pop("metadata", {}),
    supervisor=overrides.pop("supervisor", None),
    last_heartbeat_at=overrides.pop("last_heartbeat_at", timezone.now()),
    **overrides,
  )


@pytest.mark.django_db
def test_runtime_control_models_support_crud():
  semaphore = Semaphore.objects.create(
    key="account:1",
    value=1,
    limit=2,
    expires_at=timezone.now(),
  )
  pause = Pause.objects.create(backend_alias="default", queue_name="emails")
  process = make_process(metadata={"queues": ["default"], "threads": 3})

  semaphore.value = 0
  semaphore.save(update_fields=["value", "updated_at"])
  process.metadata = {"queues": ["emails"], "threads": 5}
  process.save(update_fields=["metadata"])

  assert Semaphore.objects.get(pk=semaphore.pk).value == 0
  assert Pause.objects.get(pk=pause.pk).queue_name == "emails"
  assert Process.objects.get(pk=process.pk).metadata == {
    "queues": ["emails"],
    "threads": 5,
  }

  semaphore.delete()
  pause.delete()
  process.delete()

  assert Semaphore.objects.exists() is False
  assert Pause.objects.exists() is False
  assert Process.objects.exists() is False


@pytest.mark.django_db
def test_pause_queue_name_unique():
  Pause.objects.create(backend_alias="default", queue_name="emails")

  with pytest.raises(IntegrityError), transaction.atomic():
    Pause.objects.create(backend_alias="default", queue_name="emails")


@pytest.mark.django_db
def test_pause_queue_name_is_unique_per_backend():
  Pause.objects.create(backend_alias="default", queue_name="emails")
  Pause.objects.create(backend_alias="secondary", queue_name="emails")

  assert Pause.objects.count() == 2


@pytest.mark.django_db
def test_semaphore_key_unique():
  Semaphore.objects.create(
    key="account:1",
    value=1,
    limit=1,
    expires_at=timezone.now(),
  )

  with pytest.raises(IntegrityError), transaction.atomic():
    Semaphore.objects.create(
      key="account:1",
      value=0,
      limit=1,
      expires_at=timezone.now(),
    )


@pytest.mark.django_db
def test_process_identity_unique_by_name_and_supervisor():
  supervisor = make_process(kind="Supervisor", name="supervisor-1", pid=100)
  make_process(name="worker-1", pid=101, supervisor=supervisor)
  make_process(name="worker-2", pid=101, supervisor=supervisor)

  with pytest.raises(IntegrityError), transaction.atomic():
    make_process(name="worker-1", pid=999, supervisor=supervisor)


@pytest.mark.django_db
def test_root_process_identity_unique_by_name():
  make_process(kind="Supervisor", name="supervisor-1", pid=100)

  with pytest.raises(IntegrityError), transaction.atomic():
    make_process(kind="Supervisor", name="supervisor-1", pid=999)
