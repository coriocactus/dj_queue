import pytest
from django.contrib.auth.models import Permission
from django.urls import reverse

from dj_queue.models import Job, Pause, RecurringTask
from tests.factories import make_failed_job, make_ready_job

pytestmark = pytest.mark.django_db


@pytest.fixture
def staff_client(client, django_user_model):
  def login(*permissions):
    user = django_user_model.objects.create_user(username="operator", is_staff=True)
    user.user_permissions.set(
      Permission.objects.filter(content_type__app_label="dj_queue", codename__in=permissions)
    )
    client.force_login(user)
    return client

  return login


@pytest.mark.parametrize("permissions", [(), ("view_job",), ("add_job",)])
def test_object_enqueue_requires_add_job(staff_client, permissions):
  client = staff_client(*permissions)
  job = make_ready_job(args=["copy"])
  response = client.post(
    reverse("admin:dj_queue_job_change", args=[job.pk]),
    {
      "_djq_object_action": "enqueue",
    },
  )
  allowed = "add_job" in permissions
  assert response.status_code == (302 if allowed else 403)
  assert Job.objects.count() == (2 if allowed else 1)


@pytest.mark.parametrize("surface", ["dashboard", "job", "failed"])
@pytest.mark.parametrize("action,permission", [("retry", "change_job"), ("discard", "delete_job")])
@pytest.mark.parametrize("allowed", [False, True])
def test_failed_job_actions_share_permissions(staff_client, surface, action, permission, allowed):
  permissions = ("view_job", "view_failedexecution", *((permission,) if allowed else ()))
  client = staff_client(*permissions)
  job = make_failed_job(args=["retry"])
  if surface == "dashboard":
    url = reverse("admin:dj_queue_dashboard_job_action", args=[job.queue_name])
    data = {
      "backend": "default",
      "state": "failed",
      "action": action,
      "_selected_action": [str(job.pk)],
    }
  else:
    model, pk = (
      ("job", job.pk) if surface == "job" else ("failedexecution", job.failed_execution.pk)
    )
    url = reverse(f"admin:dj_queue_{model}_change", args=[pk])
    data = {"_djq_object_action": action}

  response = client.post(url, data)

  assert response.status_code == (302 if allowed else 403)
  if allowed and action == "discard":
    assert not Job.objects.filter(pk=job.pk).exists()
  else:
    assert Job.objects.get(pk=job.pk).status == ("ready" if allowed else "failed")


@pytest.mark.parametrize(
  "action,permission",
  [("pause", "add_pause"), ("resume", "delete_pause"), ("clear", "delete_job")],
)
@pytest.mark.parametrize("allowed", [False, True])
def test_queue_actions_require_permission(staff_client, action, permission, allowed):
  client = staff_client(*((permission,) if allowed else ()))
  job = make_ready_job(args=["ready"])
  if action == "resume":
    Pause.objects.create(backend_alias="default", queue_name="default")

  response = client.post(
    reverse("admin:dj_queue_dashboard_queue_action", args=["default"]),
    {
      "backend": "default",
      "action": action,
    },
  )

  assert response.status_code == (302 if allowed else 403)
  assert Job.objects.filter(pk=job.pk).exists() is not (allowed and action == "clear")
  assert Pause.objects.exists() is (
    (action == "pause" and allowed) or (action == "resume" and not allowed)
  )


@pytest.mark.parametrize("action,permission", [("retry", "change_job"), ("discard", "delete_job")])
@pytest.mark.parametrize("allowed", [False, True])
def test_failed_bulk_actions_require_job_permission(staff_client, action, permission, allowed):
  client = staff_client("view_failedexecution", *((permission,) if allowed else ()))
  job = make_failed_job(args=["retry"])
  response = client.post(
    reverse("admin:dj_queue_failedexecution_changelist"),
    {
      "action": f"{action}_jobs",
      "_selected_action": [job.failed_execution.pk],
    },
  )
  assert response.status_code in (200, 302)
  if allowed and action == "discard":
    assert not Job.objects.filter(pk=job.pk).exists()
  else:
    assert Job.objects.get(pk=job.pk).status == ("ready" if allowed else "failed")


@pytest.mark.parametrize("model,action", [(Pause, "resume"), (RecurringTask, "unschedule")])
@pytest.mark.parametrize("allowed", [False, True])
def test_control_object_actions_require_delete_permission(staff_client, model, action, allowed):
  model_name = model._meta.model_name
  client = staff_client(f"view_{model_name}", *((f"delete_{model_name}",) if allowed else ()))
  fields = {"queue_name": "default", "backend_alias": "default"}
  if model is RecurringTask:
    fields.update(key="dynamic", task_path="tests.tasks.echo", schedule="* * * * *", priority=0)
  obj = model.objects.create(**fields)
  response = client.post(
    reverse(f"admin:dj_queue_{model_name}_change", args=[obj.pk]),
    {
      "_djq_object_action": action,
    },
  )
  assert response.status_code == (302 if allowed else 403)
  assert model.objects.filter(pk=obj.pk).exists() is not allowed


def test_dashboard_remains_visible_without_mutation_controls(staff_client):
  client = staff_client()
  make_ready_job(args=["visible"])

  response = client.get(reverse("admin:dj_queue_dashboard_queue", args=["default"]))

  assert response.status_code == 200
  assert response.context["job_actions"] == ()
  assert b'name="action" value="pause"' not in response.content
  assert b'name="action" value="clear"' not in response.content
