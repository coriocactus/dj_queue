import json

import pytest
from django.core.exceptions import ImproperlyConfigured

from dj_queue.config import DispatcherConfig, SchedulerConfig, WorkerConfig, load_backend_config


def test_config_defaults_resolve(settings):
  settings.TASKS = {}

  config = load_backend_config()

  assert config.mode == "fork"
  assert config.allowed_queues == ()
  assert config.workers == (WorkerConfig(),)
  assert config.dispatchers == (DispatcherConfig(),)
  assert config.scheduler == SchedulerConfig()
  assert config.database_alias == "default"
  assert config.use_skip_locked is True
  assert config.listen_notify is True
  assert json.dumps(config.as_dict())


def test_invalid_mode_is_rejected(settings):
  settings.TASKS = {
    "default": {
      "OPTIONS": {
        "mode": "threads",
      },
    },
  }

  with pytest.raises(ImproperlyConfigured, match="mode"):
    load_backend_config()


def test_on_thread_error_path_is_validated(settings):
  settings.TASKS = {
    "default": {
      "OPTIONS": {
        "on_thread_error": "missing.module.callback",
      },
    },
  }

  with pytest.raises(ImproperlyConfigured, match="on_thread_error"):
    load_backend_config()


def test_scheduler_omitted_when_no_scheduler_work_exists(settings):
  settings.TASKS = {
    "default": {
      "OPTIONS": {
        "preserve_finished_jobs": False,
        "clear_finished_jobs_after": None,
        "recurring": {},
      },
    },
  }

  config = load_backend_config()

  assert config.scheduler is None


def test_config_precedence_cli_over_env_over_yaml_over_settings(settings, tmp_path):
  settings.TASKS = {
    "default": {
      "OPTIONS": {
        "mode": "fork",
        "listen_notify": False,
        "silence_polling": False,
      },
    },
  }
  env_config_path = tmp_path / "env.yaml"
  env_config_path.write_text("mode: fork\nsilence_polling: false\n", encoding="utf-8")
  cli_config_path = tmp_path / "cli.yaml"
  cli_config_path.write_text(
    "mode: fork\nlisten_notify: true\nsilence_polling: true\n", encoding="utf-8"
  )

  env_config = load_backend_config(
    env={
      "DJ_QUEUE_MODE": "async",
      "DJ_QUEUE_CONFIG": str(env_config_path),
    }
  )
  cli_config = load_backend_config(
    cli_overrides={
      "mode": "fork",
      "config": str(cli_config_path),
    },
    env={
      "DJ_QUEUE_MODE": "async",
      "DJ_QUEUE_CONFIG": str(env_config_path),
    },
  )

  assert env_config.mode == "async"
  assert env_config.silence_polling is False
  assert cli_config.mode == "fork"
  assert cli_config.listen_notify is True
  assert cli_config.silence_polling is True


@pytest.mark.parametrize(
  ("value", "skip_recurring"),
  (
    ("1", True),
    ("true", True),
    ("yes", True),
    ("on", True),
    ("0", False),
    ("false", False),
    ("no", False),
    ("off", False),
  ),
)
def test_boolean_environment_values_parse_truthy_and_falsy_forms(settings, value, skip_recurring):
  settings.TASKS = {
    "default": {
      "OPTIONS": {
        "preserve_finished_jobs": True,
        "clear_finished_jobs_after": 10,
      },
    },
  }

  config = load_backend_config(env={"DJ_QUEUE_SKIP_RECURRING": value})

  assert (config.scheduler is None) is skip_recurring


def test_load_backend_config_caches_repeated_inputs(settings, monkeypatch):
  settings.TASKS = {
    "default": {
      "OPTIONS": {
        "mode": "fork",
      },
    },
  }
  calls = []

  def fake_load_yaml_options(path):
    calls.append(path)
    return {}

  monkeypatch.setattr("dj_queue.config._load_yaml_options", fake_load_yaml_options)

  first = load_backend_config(env={"DJ_QUEUE_CONFIG": "/tmp/dj-queue.yaml"})
  second = load_backend_config(env={"DJ_QUEUE_CONFIG": "/tmp/dj-queue.yaml"})

  assert first == second
  assert calls == ["/tmp/dj-queue.yaml"]


def test_load_backend_config_cache_invalidates_when_settings_change(settings):
  settings.TASKS = {
    "default": {
      "OPTIONS": {
        "database_alias": "queue_a",
      },
    },
  }

  first = load_backend_config()

  settings.TASKS = {
    "default": {
      "OPTIONS": {
        "database_alias": "queue_b",
      },
    },
  }

  second = load_backend_config()

  assert first.database_alias == "queue_a"
  assert second.database_alias == "queue_b"
