import warnings
from copy import deepcopy

import pytest
from django.core.exceptions import ImproperlyConfigured

from dj_queue.config import DEFAULT_OPTIONS, WorkerConfig, load_backend_config


def backend_settings(options):
  return {"default": {"BACKEND": "dj_queue.backend.DjQueueBackend", "OPTIONS": options}}


@pytest.mark.parametrize(
  ("flags", "options", "message"),
  (
    ({"only_work": True}, {"dispatchers": {"batch_size": 0}}, r"dispatchers\[0\].batch_size"),
    ({"only_dispatch": True}, {"workers": {"threads": 0}}, r"workers\[0\].threads"),
    (
      {"skip_recurring": True},
      {"scheduler": {"polling_interval": 0}},
      "scheduler.polling_interval",
    ),
    (
      {"only_work": True},
      {"recurring": {"bad": {"task_path": "missing.task", "schedule": "* * * * *"}}},
      "recurring task 'bad'",
    ),
    ({"skip_recurring": True}, {"clear_failed_jobs_after": -1}, "clear_failed_jobs_after"),
  ),
)
def test_disabled_sections_still_validate(flags, options, message):
  with pytest.raises(ImproperlyConfigured, match=message):
    load_backend_config(tasks_settings=backend_settings(options), cli_overrides=flags, env={})


@pytest.mark.parametrize(
  ("flags", "workers", "dispatchers", "scheduler"),
  (
    ({}, True, True, True),
    ({"only_work": True}, True, False, False),
    ({"only_dispatch": True}, False, True, False),
    ({"skip_recurring": True}, True, True, False),
  ),
)
def test_topology_selection_preserves_typed_config(flags, workers, dispatchers, scheduler):
  config = load_backend_config(tasks_settings={}, cli_overrides=flags, env={})

  assert bool(config.workers) is workers
  assert bool(config.dispatchers) is dispatchers
  assert (config.scheduler is not None) is scheduler
  assert config.only_work is flags.get("only_work", False)
  assert config.only_dispatch is flags.get("only_dispatch", False)
  assert config.skip_recurring is flags.get("skip_recurring", False)


def test_async_warning_survives_disabled_workers_without_mutating_inputs():
  defaults = deepcopy(DEFAULT_OPTIONS)
  tasks = backend_settings({"mode": "async", "workers": {"processes": 3}})
  original = deepcopy(tasks)
  with pytest.warns(UserWarning, match="normalizing to 1"):
    config = load_backend_config(
      tasks_settings=tasks, cli_overrides={"only_dispatch": True}, env={}
    )
  with warnings.catch_warnings(record=True) as caught:
    cached = load_backend_config(
      tasks_settings=tasks, cli_overrides={"only_dispatch": True}, env={}
    )

  assert cached is config
  assert not caught
  assert config.workers == ()
  assert tasks == original
  assert DEFAULT_OPTIONS == defaults
  assert load_backend_config(tasks_settings={}, env={}).workers == (WorkerConfig(),)


def test_failed_validation_is_not_cached(tmp_path):
  path = tmp_path / "options.toml"
  path.write_text("shutdown_timeout = -1\n")
  env = {"DJ_QUEUE_CONFIG": str(path)}
  with pytest.raises(ImproperlyConfigured, match="shutdown_timeout"):
    load_backend_config(tasks_settings={}, env=env)
  path.write_text("shutdown_timeout = 2\n")

  assert load_backend_config(tasks_settings={}, env=env).shutdown_timeout == 2


def test_invalid_scalar_is_reported_before_empty_topology():
  tasks = backend_settings({"workers": [], "dispatchers": [], "shutdown_timeout": -1})

  with pytest.raises(ImproperlyConfigured, match="shutdown_timeout"):
    load_backend_config(tasks_settings=tasks, cli_overrides={"skip_recurring": True}, env={})


def test_scheduler_only_and_empty_topology():
  tasks = backend_settings({"workers": [], "dispatchers": [], "clear_failed_jobs_after": 0})
  config = load_backend_config(tasks_settings=tasks, env={})

  assert config.scheduler is not None
  assert config.clear_failed_jobs_after == 0
  with pytest.raises(ImproperlyConfigured, match="at least one"):
    load_backend_config(tasks_settings=tasks, cli_overrides={"skip_recurring": True}, env={})
