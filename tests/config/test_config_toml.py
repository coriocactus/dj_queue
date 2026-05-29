import pytest
from django.core.exceptions import ImproperlyConfigured

from dj_queue.config import load_backend_config


def test_toml_config_supports_flat_readme_shape(settings, tmp_path):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {},
    }
  }
  config_path = tmp_path / "dj_queue.toml"
  config_path.write_text(
    """
mode = "async"
database_alias = "queue"
preserve_finished_jobs = true
clear_finished_jobs_after = 86400
listen_notify = true # comments are ignored
silence_polling = true

[[workers]]
queues = ["default", "email*"]
threads = 8
processes = 1
polling_interval = 0.1

[[dispatchers]]
batch_size = 500
polling_interval = 1
concurrency_maintenance = true
concurrency_maintenance_interval = 600

[scheduler]
dynamic_tasks_enabled = true
polling_interval = 5

[recurring.nightly_cleanup]
task_path = "tests.tasks.echo"
schedule = "0 3 * * *"
args = ["hello"]
kwargs = { value = "world" }
queue_name = "maintenance"
priority = -5
description = "nightly cleanup"
""".lstrip(),
    encoding="utf-8",
  )

  config = load_backend_config(env={"DJ_QUEUE_CONFIG": str(config_path)})

  assert config.mode == "async"
  assert config.database_alias == "queue"
  assert config.workers[0].queues == ("default", "email*")
  assert config.workers[0].threads == 8
  assert config.dispatchers[0].concurrency_maintenance is True
  assert config.scheduler.dynamic_tasks_enabled is True
  assert config.recurring["nightly_cleanup"].args == ("hello",)
  assert config.recurring["nightly_cleanup"].kwargs == {"value": "world"}
  assert config.recurring["nightly_cleanup"].priority == -5


def test_toml_config_rejects_invalid_toml(settings, tmp_path):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {},
    }
  }
  config_path = tmp_path / "dj_queue.toml"
  config_path.write_text("mode =\n", encoding="utf-8")

  with pytest.raises(ImproperlyConfigured, match="TOML is invalid"):
    load_backend_config(env={"DJ_QUEUE_CONFIG": str(config_path)})


def test_toml_config_rejects_non_json_values(settings, tmp_path):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {},
    }
  }
  config_path = tmp_path / "dj_queue.toml"
  config_path.write_text(
    """
[recurring.daily]
task_path = "tests.tasks.echo"
schedule = "0 3 * * *"
args = [2026-05-29]
""".lstrip(),
    encoding="utf-8",
  )

  with pytest.raises(ImproperlyConfigured, match="JSON-serializable"):
    load_backend_config(env={"DJ_QUEUE_CONFIG": str(config_path)})


def test_toml_config_preserves_hashes_and_colons_inside_strings(settings, tmp_path):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {},
    }
  }
  config_path = tmp_path / "dj_queue.toml"
  config_path.write_text(
    """
[recurring.first_monday]
task_path = "tests.tasks.echo"
schedule = "0 5 * * mon#1" # first monday comment
args = ["https://example.com/report"]
description = "run at 05:00 report#not-a-comment"
""".lstrip(),
    encoding="utf-8",
  )

  config = load_backend_config(env={"DJ_QUEUE_CONFIG": str(config_path)})

  assert config.recurring["first_monday"].schedule == "0 5 * * mon#1"
  assert config.recurring["first_monday"].args == ("https://example.com/report",)
  assert config.recurring["first_monday"].description == "run at 05:00 report#not-a-comment"
