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
  assert config.clear_failed_jobs_after is None
  assert config.clear_recurring_executions_after is None
  assert json.dumps(config.as_dict())


def test_missing_alias_is_rejected_when_tasks_is_non_empty(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {},
    }
  }

  with pytest.raises(ImproperlyConfigured, match="is not configured"):
    load_backend_config("missing")


def test_invalid_mode_is_rejected(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {
        "mode": "threads",
      },
    },
  }

  with pytest.raises(ImproperlyConfigured, match="mode"):
    load_backend_config()


def test_non_dj_queue_backend_alias_is_rejected(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "other.backend.Backend",
      "OPTIONS": {},
    },
  }

  with pytest.raises(ImproperlyConfigured, match="not configured for DjQueueBackend"):
    load_backend_config()


def test_missing_backend_alias_is_rejected_when_tasks_is_non_empty(settings):
  settings.TASKS = {
    "default": {
      "OPTIONS": {},
    },
  }

  with pytest.raises(ImproperlyConfigured, match="not configured for DjQueueBackend"):
    load_backend_config()


def test_on_thread_error_path_is_validated(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
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
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {
        "preserve_finished_jobs": False,
        "clear_finished_jobs_after": None,
        "clear_failed_jobs_after": None,
        "clear_recurring_executions_after": None,
        "recurring": {},
      },
    },
  }

  config = load_backend_config()

  assert config.scheduler is None


def test_failed_or_recurring_cleanup_keeps_scheduler_enabled(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {
        "preserve_finished_jobs": False,
        "clear_finished_jobs_after": None,
        "clear_failed_jobs_after": 60,
        "clear_recurring_executions_after": 120,
        "recurring": {},
      },
    },
  }

  config = load_backend_config()

  assert config.scheduler == SchedulerConfig()


def test_config_precedence_cli_over_env_over_yaml_over_settings(settings, tmp_path):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
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


def test_multi_backend_yaml_selects_requested_alias(settings, tmp_path):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {
        "mode": "fork",
        "database_alias": "default",
      },
    },
    "critical": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {
        "mode": "fork",
        "database_alias": "default",
      },
    },
  }
  config_path = tmp_path / "dj_queue.yaml"
  config_path.write_text(
    "\n".join(
      (
        "backends:",
        "  default:",
        "    mode: async",
        "    database_alias: queue_default",
        "  critical:",
        "    mode: fork",
        "    database_alias: queue_critical",
      )
    ),
    encoding="utf-8",
  )

  default_config = load_backend_config(
    env={
      "DJ_QUEUE_CONFIG": str(config_path),
    }
  )
  critical_config = load_backend_config(
    "critical",
    env={
      "DJ_QUEUE_CONFIG": str(config_path),
    },
  )

  assert default_config.mode == "async"
  assert default_config.database_alias == "queue_default"
  assert critical_config.mode == "fork"
  assert critical_config.database_alias == "queue_critical"


def test_multi_backend_yaml_missing_alias_falls_back_to_tasks(settings, tmp_path):
  settings.TASKS = {
    "secondary": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {
        "mode": "async",
        "database_alias": "queue_secondary",
      },
    }
  }
  config_path = tmp_path / "dj_queue.yaml"
  config_path.write_text(
    "\n".join(
      (
        "backends:",
        "  default:",
        "    mode: fork",
      )
    ),
    encoding="utf-8",
  )

  config = load_backend_config(
    "secondary",
    env={
      "DJ_QUEUE_CONFIG": str(config_path),
    },
  )

  assert config.mode == "async"
  assert config.database_alias == "queue_secondary"


def test_multi_backend_yaml_rejects_non_mapping_backends(settings, tmp_path):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {},
    }
  }
  config_path = tmp_path / "dj_queue.yaml"
  config_path.write_text("backends: []\n", encoding="utf-8")

  with pytest.raises(ImproperlyConfigured, match="'backends' must be a mapping"):
    load_backend_config(
      env={
        "DJ_QUEUE_CONFIG": str(config_path),
      }
    )


def test_multi_backend_yaml_rejects_non_mapping_backend_entry(settings, tmp_path):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {},
    }
  }
  config_path = tmp_path / "dj_queue.yaml"
  config_path.write_text(
    "\n".join(
      (
        "backends:",
        "  default: true",
      )
    ),
    encoding="utf-8",
  )

  with pytest.raises(ImproperlyConfigured, match=r"backends\['default'\] must be a mapping"):
    load_backend_config(
      env={
        "DJ_QUEUE_CONFIG": str(config_path),
      }
    )


def test_multi_backend_yaml_rejects_mixed_shapes(settings, tmp_path):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {},
    }
  }
  config_path = tmp_path / "dj_queue.yaml"
  config_path.write_text(
    "\n".join(
      (
        "mode: async",
        "backends:",
        "  default:",
        "    database_alias: queue",
      )
    ),
    encoding="utf-8",
  )

  with pytest.raises(ImproperlyConfigured, match="must use either a flat options mapping"):
    load_backend_config(
      env={
        "DJ_QUEUE_CONFIG": str(config_path),
      }
    )


def test_workers_single_mapping_is_normalized_to_one_item_list(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {
        "workers": {
          "queues": ["default", "email*"],
          "threads": 8,
          "processes": 2,
          "polling_interval": 0.1,
        },
      },
    }
  }

  config = load_backend_config()

  assert config.workers == (
    WorkerConfig(
      queues=("default", "email*"),
      threads=8,
      processes=2,
      polling_interval=0.1,
    ),
  )


def test_dispatchers_single_mapping_is_normalized_to_one_item_list(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {
        "dispatchers": {
          "batch_size": 250,
          "polling_interval": 2,
          "concurrency_maintenance": False,
          "concurrency_maintenance_interval": 900,
        },
      },
    }
  }

  config = load_backend_config()

  assert config.dispatchers == (
    DispatcherConfig(
      batch_size=250,
      polling_interval=2,
      concurrency_maintenance=False,
      concurrency_maintenance_interval=900,
    ),
  )


@pytest.mark.parametrize(
  ("options", "setting_name"),
  (
    ({"workers": {"polling_interval": 0}}, r"workers\[0\]\.polling_interval"),
    ({"workers": {"polling_interval": -0.1}}, r"workers\[0\]\.polling_interval"),
    ({"workers": {"polling_interval": "fast"}}, r"workers\[0\]\.polling_interval"),
    ({"dispatchers": {"polling_interval": 0}}, r"dispatchers\[0\]\.polling_interval"),
    ({"dispatchers": {"polling_interval": -1}}, r"dispatchers\[0\]\.polling_interval"),
    ({"dispatchers": {"polling_interval": "fast"}}, r"dispatchers\[0\]\.polling_interval"),
    ({"scheduler": {"polling_interval": 0}}, r"scheduler\.polling_interval"),
    ({"scheduler": {"polling_interval": -5}}, r"scheduler\.polling_interval"),
    ({"scheduler": {"polling_interval": "fast"}}, r"scheduler\.polling_interval"),
  ),
)
def test_runner_polling_interval_must_be_positive_number(settings, options, setting_name):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": options,
    }
  }

  with pytest.raises(ImproperlyConfigured, match=setting_name):
    load_backend_config()


def test_missing_runner_polling_interval_uses_positive_defaults(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {
        "workers": {"queues": "default"},
        "dispatchers": {"batch_size": 250},
        "scheduler": {"dynamic_tasks_enabled": False},
      },
    }
  }

  config = load_backend_config()

  assert config.workers[0].polling_interval == 0.1
  assert config.dispatchers[0].polling_interval == 1
  assert config.scheduler.polling_interval == 5


def test_workers_single_mapping_still_normalizes_async_processes_to_one(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {
        "mode": "async",
        "workers": {
          "queues": "default",
          "threads": 4,
          "processes": 3,
        },
      },
    }
  }

  with pytest.warns(UserWarning, match="normalizing to 1"):
    config = load_backend_config()

  assert config.workers == (
    WorkerConfig(
      queues=("default",),
      threads=4,
      processes=1,
      polling_interval=0.1,
    ),
  )


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
      "BACKEND": "dj_queue.backend.DjQueueBackend",
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
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {
        "mode": "fork",
      },
    },
  }
  calls = []

  def fake_load_yaml_options(path, *, backend_alias):
    calls.append((path, backend_alias))
    return {}

  monkeypatch.setattr("dj_queue.config._load_yaml_options", fake_load_yaml_options)

  first = load_backend_config(env={"DJ_QUEUE_CONFIG": "/tmp/dj-queue.yaml"})
  second = load_backend_config(env={"DJ_QUEUE_CONFIG": "/tmp/dj-queue.yaml"})

  assert first == second
  assert calls == [("/tmp/dj-queue.yaml", "default")]


def test_load_backend_config_cache_invalidates_when_settings_change(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {
        "database_alias": "queue_a",
      },
    },
  }

  first = load_backend_config()

  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {
        "database_alias": "queue_b",
      },
    },
  }

  second = load_backend_config()

  assert first.database_alias == "queue_a"
  assert second.database_alias == "queue_b"
