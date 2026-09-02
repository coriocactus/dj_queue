import json
import sys

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
  assert config.async_thread_sensitive is False
  assert config.async_close_connections is False
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


@pytest.mark.parametrize(
  ("options", "setting_path", "unknown_option"),
  (
    ({"workerz": []}, "TASKS backend OPTIONS", "workerz"),
    ({"workers": {"threadz": 3}}, "workers[0]", "threadz"),
    ({"dispatchers": {"batch_sizes": 500}}, "dispatchers[0]", "batch_sizes"),
    ({"scheduler": {"dynamic_tasks": True}}, "scheduler", "dynamic_tasks"),
    (
      {
        "recurring": {
          "daily": {
            "task_path": "tests.tasks.echo",
            "schedule": "* * * * *",
            "queues": "default",
          }
        }
      },
      "recurring task 'daily'",
      "queues",
    ),
  ),
)
def test_config_rejects_unknown_options(settings, options, setting_path, unknown_option):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": options,
    },
  }

  with pytest.raises(ImproperlyConfigured) as exc_info:
    load_backend_config()

  message = str(exc_info.value)
  assert setting_path in message
  assert unknown_option in message


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


def test_static_recurring_priority_must_be_in_range(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {
        "recurring": {
          "too-important": {
            "task_path": "tests.tasks.echo",
            "schedule": "* * * * *",
            "priority": 101,
          }
        }
      },
    }
  }

  with pytest.raises(ImproperlyConfigured, match="priority"):
    load_backend_config()


def test_static_recurring_task_path_must_reference_a_django_task(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {
        "recurring": {
          "not-a-task": {
            "task_path": "dj_queue.config.load_backend_config",
            "schedule": "* * * * *",
          }
        }
      },
    }
  }

  with pytest.raises(ImproperlyConfigured, match="must reference a Django task"):
    load_backend_config()


def test_static_recurring_task_import_tolerates_task_decorators_before_target(
  settings, tmp_path, monkeypatch
):
  module_name = "reentrant_recurring_tasks"
  monkeypatch.delitem(sys.modules, module_name, raising=False)
  (tmp_path / f"{module_name}.py").write_text(
    (
      "from django.tasks import task\n"
      "\n"
      "@task\n"
      "def earlier_task():\n"
      "  return 'earlier'\n"
      "\n"
      "@task\n"
      "def cleanup_unactivated_users():\n"
      "  return 'cleanup'"
    ),
    encoding="utf-8",
  )
  monkeypatch.syspath_prepend(str(tmp_path))
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {
        "recurring": {
          "users_cleanup_unactivated_users": {
            "task_path": f"{module_name}.cleanup_unactivated_users",
            "schedule": "* * * * *",
          }
        }
      },
    }
  }

  try:
    config = load_backend_config()
  finally:
    sys.modules.pop(module_name, None)

  assert "users_cleanup_unactivated_users" in config.recurring


def test_static_recurring_queue_must_be_allowed_for_backend(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "QUEUES": ["default"],
      "OPTIONS": {
        "recurring": {
          "other-queue": {
            "task_path": "tests.tasks.echo",
            "schedule": "* * * * *",
            "queue_name": "other",
          }
        }
      },
    }
  }

  with pytest.raises(ImproperlyConfigured, match="queue 'other' is not allowed"):
    load_backend_config()


def test_config_precedence_cli_over_env_over_toml_over_settings(settings, tmp_path):
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
  env_config_path = tmp_path / "env.toml"
  env_config_path.write_text('mode = "fork"\nsilence_polling = false\n', encoding="utf-8")
  cli_config_path = tmp_path / "cli.toml"
  cli_config_path.write_text(
    'mode = "fork"\nlisten_notify = true\nsilence_polling = true\n', encoding="utf-8"
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


def test_multi_backend_toml_selects_requested_alias(settings, tmp_path):
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
  config_path = tmp_path / "dj_queue.toml"
  config_path.write_text(
    (
      "[backends.default]\n"
      'mode = "async"\n'
      'database_alias = "queue_default"\n'
      "[backends.critical]\n"
      'mode = "fork"\n'
      'database_alias = "queue_critical"'
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


def test_multi_backend_toml_missing_alias_falls_back_to_tasks(settings, tmp_path):
  settings.TASKS = {
    "secondary": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {
        "mode": "async",
        "database_alias": "queue_secondary",
      },
    }
  }
  config_path = tmp_path / "dj_queue.toml"
  config_path.write_text(
    '[backends.default]\nmode = "fork"',
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


def test_multi_backend_toml_rejects_non_mapping_backends(settings, tmp_path):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {},
    }
  }
  config_path = tmp_path / "dj_queue.toml"
  config_path.write_text("backends = []\n", encoding="utf-8")

  with pytest.raises(ImproperlyConfigured, match="'backends' must be a mapping"):
    load_backend_config(
      env={
        "DJ_QUEUE_CONFIG": str(config_path),
      }
    )


def test_multi_backend_toml_rejects_non_mapping_backend_entry(settings, tmp_path):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {},
    }
  }
  config_path = tmp_path / "dj_queue.toml"
  config_path.write_text("backends = { default = true }\n", encoding="utf-8")

  with pytest.raises(ImproperlyConfigured, match=r"backends\['default'\] must be a mapping"):
    load_backend_config(
      env={
        "DJ_QUEUE_CONFIG": str(config_path),
      }
    )


def test_multi_backend_toml_rejects_mixed_shapes(settings, tmp_path):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {},
    }
  }
  config_path = tmp_path / "dj_queue.toml"
  config_path.write_text(
    'mode = "async"\n[backends.default]\ndatabase_alias = "queue"',
    encoding="utf-8",
  )

  with pytest.raises(ImproperlyConfigured, match="must use either a flat options mapping"):
    load_backend_config(
      env={
        "DJ_QUEUE_CONFIG": str(config_path),
      }
    )


def test_config_file_read_errors_are_wrapped(settings, tmp_path):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {},
    }
  }
  missing_config_path = tmp_path / "missing.toml"

  with pytest.raises(ImproperlyConfigured, match="DJ_QUEUE_CONFIG could not be read"):
    load_backend_config(env={"DJ_QUEUE_CONFIG": str(missing_config_path)})


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
          "prefetch_multiplier": 3,
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
      prefetch_multiplier=3,
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
    ({"workers": {"polling_interval": True}}, r"workers\[0\]\.polling_interval"),
    ({"workers": {"polling_interval": "fast"}}, r"workers\[0\]\.polling_interval"),
    ({"dispatchers": {"polling_interval": 0}}, r"dispatchers\[0\]\.polling_interval"),
    ({"dispatchers": {"polling_interval": -1}}, r"dispatchers\[0\]\.polling_interval"),
    ({"dispatchers": {"polling_interval": True}}, r"dispatchers\[0\]\.polling_interval"),
    ({"dispatchers": {"polling_interval": "fast"}}, r"dispatchers\[0\]\.polling_interval"),
    ({"scheduler": {"polling_interval": 0}}, r"scheduler\.polling_interval"),
    ({"scheduler": {"polling_interval": -5}}, r"scheduler\.polling_interval"),
    ({"scheduler": {"polling_interval": True}}, r"scheduler\.polling_interval"),
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


@pytest.mark.parametrize(
  ("value", "setting_name"),
  (
    (0, "default_concurrency_duration"),
    (-1, "default_concurrency_duration"),
    (True, "default_concurrency_duration"),
    (1.9, "default_concurrency_duration"),
    ("soon", "default_concurrency_duration"),
  ),
)
def test_default_concurrency_duration_must_be_positive_integer(settings, value, setting_name):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {
        "default_concurrency_duration": value,
      },
    }
  }

  with pytest.raises(ImproperlyConfigured, match=setting_name):
    load_backend_config()


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
      prefetch_multiplier=2,
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


@pytest.mark.parametrize(
  "setting_name",
  (
    "preserve_finished_jobs",
    "use_skip_locked",
    "listen_notify",
    "silence_polling",
  ),
)
def test_boolean_options_parse_truthy_and_falsy_strings(settings, setting_name):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {
        setting_name: "false",
        "clear_finished_jobs_after": 10,
      },
    },
  }

  config = load_backend_config()

  assert getattr(config, setting_name) is False


@pytest.mark.parametrize(
  ("section", "setting_name"),
  (
    ("dispatchers", "concurrency_maintenance"),
    ("scheduler", "dynamic_tasks_enabled"),
  ),
)
def test_nested_boolean_options_parse_truthy_and_falsy_strings(settings, section, setting_name):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {
        section: {setting_name: "false"},
      },
    },
  }

  config = load_backend_config()

  if section == "dispatchers":
    assert getattr(config.dispatchers[0], setting_name) is False
  else:
    assert getattr(config.scheduler, setting_name) is False


@pytest.mark.parametrize(
  ("setting_name", "value"),
  (
    ("preserve_finished_jobs", "sometimes"),
    ("use_skip_locked", "sometimes"),
    ("listen_notify", "sometimes"),
    ("silence_polling", "sometimes"),
  ),
)
def test_boolean_options_reject_unknown_strings(settings, setting_name, value):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {setting_name: value},
    },
  }

  with pytest.raises(ImproperlyConfigured) as exc_info:
    load_backend_config()
  assert setting_name in str(exc_info.value)


@pytest.mark.parametrize(
  ("setting_name", "value"),
  (
    ("process_heartbeat_interval", -1),
    ("process_heartbeat_interval", "soon"),
    ("process_alive_threshold", 0),
    ("process_alive_threshold", "soon"),
    ("shutdown_timeout", -1),
    ("shutdown_timeout", "soon"),
  ),
)
def test_runtime_numeric_options_validate(settings, setting_name, value):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {setting_name: value},
    },
  }

  with pytest.raises(ImproperlyConfigured, match=setting_name):
    load_backend_config()


def test_runtime_numeric_options_accept_fractional_seconds(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {
        "process_heartbeat_interval": 0.25,
        "process_alive_threshold": 1.5,
        "shutdown_timeout": 0.5,
        "dispatchers": {"concurrency_maintenance_interval": 0.5},
      },
    },
  }

  config = load_backend_config()

  assert config.process_heartbeat_interval == 0.25
  assert config.process_alive_threshold == 1.5
  assert config.shutdown_timeout == 0.5
  assert config.dispatchers[0].concurrency_maintenance_interval == 0.5


@pytest.mark.parametrize(
  "setting_name",
  (
    "clear_finished_jobs_after",
    "clear_failed_jobs_after",
    "clear_recurring_executions_after",
  ),
)
def test_retention_options_must_be_nonnegative_integers(settings, setting_name):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {setting_name: -1},
    },
  }

  with pytest.raises(ImproperlyConfigured, match=setting_name):
    load_backend_config()


@pytest.mark.parametrize(
  ("options", "setting_name"),
  (
    ({"workers": {"threads": 0}}, "workers[0].threads"),
    ({"workers": {"threads": True}}, "workers[0].threads"),
    ({"workers": {"threads": 1.9}}, "workers[0].threads"),
    ({"workers": {"processes": 0}}, "workers[0].processes"),
    ({"workers": {"prefetch_multiplier": 0}}, "workers[0].prefetch_multiplier"),
    ({"workers": {"prefetch_multiplier": True}}, "workers[0].prefetch_multiplier"),
    ({"workers": {"prefetch_multiplier": 1.9}}, "workers[0].prefetch_multiplier"),
    ({"dispatchers": {"batch_size": 0}}, "dispatchers[0].batch_size"),
    (
      {"dispatchers": {"concurrency_maintenance_interval": -1}},
      "dispatchers[0].concurrency_maintenance_interval",
    ),
    (
      {"dispatchers": {"concurrency_maintenance_interval": True}},
      "dispatchers[0].concurrency_maintenance_interval",
    ),
  ),
)
def test_nested_runtime_numeric_options_validate(settings, options, setting_name):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": options,
    },
  }

  with pytest.raises(ImproperlyConfigured) as exc_info:
    load_backend_config()
  assert setting_name in str(exc_info.value)


@pytest.mark.parametrize(
  ("option_name", "value"),
  (
    ("database_alias", 1),
    ("supervisor_pidfile", 1),
    ("on_thread_error", 1),
  ),
)
def test_string_options_reject_non_strings(settings, option_name, value):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {option_name: value},
    },
  }

  with pytest.raises(ImproperlyConfigured, match=option_name):
    load_backend_config()


@pytest.mark.parametrize(
  "tasks_settings",
  (
    {
      "default": {
        "BACKEND": "dj_queue.backend.DjQueueBackend",
        "QUEUES": [1],
        "OPTIONS": {},
      }
    },
    {
      "default": {
        "BACKEND": "dj_queue.backend.DjQueueBackend",
        "OPTIONS": {"workers": {"queues": [1]}},
      }
    },
  ),
)
def test_queue_selectors_must_be_strings(settings, tasks_settings):
  settings.TASKS = tasks_settings

  with pytest.raises(ImproperlyConfigured, match="sequence of strings"):
    load_backend_config()


@pytest.mark.parametrize(
  ("recurring", "setting_name"),
  (
    ({1: {"task_path": "tests.tasks.echo", "schedule": "* * * * *"}}, "recurring task key"),
    ({"": {"task_path": "tests.tasks.echo", "schedule": "* * * * *"}}, "recurring task key"),
    ({"daily": {"task_path": 1, "schedule": "* * * * *"}}, "task_path"),
    ({"daily": {"task_path": "tests.tasks.echo", "schedule": 1}}, "schedule"),
    (
      {"daily": {"task_path": "tests.tasks.echo", "schedule": "* * * * *", "queue_name": ""}},
      "queue_name",
    ),
    (
      {"daily": {"task_path": "tests.tasks.echo", "schedule": "* * * * *", "args": "x"}},
      "args",
    ),
    (
      {"daily": {"task_path": "tests.tasks.echo", "schedule": "* * * * *", "kwargs": []}},
      "kwargs",
    ),
  ),
)
def test_static_recurring_entries_reject_sloppy_shapes(settings, recurring, setting_name):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {"recurring": recurring},
    },
  }

  with pytest.raises(ImproperlyConfigured, match=setting_name):
    load_backend_config()


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

  def fake_load_toml_options(path, *, backend_alias):
    calls.append((path, backend_alias))
    return {}

  monkeypatch.setattr("dj_queue.config._load_toml_options", fake_load_toml_options)

  first = load_backend_config(env={"DJ_QUEUE_CONFIG": "/tmp/dj-queue.toml"})
  second = load_backend_config(env={"DJ_QUEUE_CONFIG": "/tmp/dj-queue.toml"})

  assert first == second
  assert calls == [("/tmp/dj-queue.toml", "default")]


def test_load_backend_config_rejects_non_json_config_values(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {
        "database_alias": object(),
      },
    },
  }

  with pytest.raises(ImproperlyConfigured, match="JSON-serializable"):
    load_backend_config()


def test_load_backend_config_ignores_non_json_values_on_other_backends(settings):
  settings.TASKS = {
    "default": {
      "BACKEND": "dj_queue.backend.DjQueueBackend",
      "OPTIONS": {
        "database_alias": "default",
      },
    },
    "other": {
      "BACKEND": "other.backend.Backend",
      "OPTIONS": {
        "callback": object(),
      },
    },
  }

  config = load_backend_config()

  assert config.database_alias == "default"


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
