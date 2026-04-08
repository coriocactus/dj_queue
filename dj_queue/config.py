import json
import os
import warnings
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field, replace
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from croniter import croniter
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string

DEFAULT_WORKER = {
  "queues": "*",
  "threads": 3,
  "processes": 1,
  "polling_interval": 0.1,
}

DEFAULT_DISPATCHER = {
  "batch_size": 500,
  "polling_interval": 1,
  "concurrency_maintenance": True,
  "concurrency_maintenance_interval": 600,
}

DEFAULT_SCHEDULER = {
  "dynamic_tasks_enabled": False,
  "polling_interval": 5,
}

DEFAULT_OPTIONS = {
  "mode": "fork",
  "workers": [DEFAULT_WORKER],
  "dispatchers": [DEFAULT_DISPATCHER],
  "scheduler": DEFAULT_SCHEDULER,
  "recurring": {},
  "process_heartbeat_interval": 60,
  "process_alive_threshold": 300,
  "shutdown_timeout": 5,
  "supervisor_pidfile": None,
  "preserve_finished_jobs": True,
  "clear_finished_jobs_after": 86400,
  "default_concurrency_duration": 180,
  "database_alias": "default",
  "use_skip_locked": True,
  "listen_notify": True,
  "silence_polling": True,
  "on_thread_error": None,
}

TRUTHY_ENV_VALUES = {"1", "true", "yes", "on"}
FALSY_ENV_VALUES = {"0", "false", "no", "off"}
CONFIG_ENV_KEYS = ("DJ_QUEUE_CONFIG", "DJ_QUEUE_MODE", "DJ_QUEUE_SKIP_RECURRING")


@dataclass(frozen=True, slots=True)
class ConfigValue:
  def as_dict(self) -> dict[str, Any]:
    return asdict(self)


@dataclass(frozen=True, slots=True)
class WorkerConfig(ConfigValue):
  queues: tuple[str, ...] = ("*",)
  threads: int = 3
  processes: int = 1
  polling_interval: float = 0.1


@dataclass(frozen=True, slots=True)
class DispatcherConfig(ConfigValue):
  batch_size: int = 500
  polling_interval: float = 1
  concurrency_maintenance: bool = True
  concurrency_maintenance_interval: int = 600


@dataclass(frozen=True, slots=True)
class SchedulerConfig(ConfigValue):
  dynamic_tasks_enabled: bool = False
  polling_interval: float = 5


@dataclass(frozen=True, slots=True)
class RecurringTaskConfig(ConfigValue):
  key: str
  task_path: str
  schedule: str
  args: tuple[Any, ...] = ()
  kwargs: dict[str, Any] = field(default_factory=dict)
  queue_name: str = "default"
  priority: int = 0
  description: str = ""


@dataclass(frozen=True, slots=True)
class BackendConfig(ConfigValue):
  backend_alias: str = "default"
  allowed_queues: tuple[str, ...] = ()
  mode: str = "fork"
  workers: tuple[WorkerConfig, ...] = (WorkerConfig(),)
  dispatchers: tuple[DispatcherConfig, ...] = (DispatcherConfig(),)
  scheduler: SchedulerConfig | None = field(default_factory=SchedulerConfig)
  recurring: dict[str, RecurringTaskConfig] = field(default_factory=dict)
  process_heartbeat_interval: int = 60
  process_alive_threshold: int = 300
  shutdown_timeout: int = 5
  supervisor_pidfile: str | None = None
  preserve_finished_jobs: bool = True
  clear_finished_jobs_after: int | None = 86400
  default_concurrency_duration: int = 180
  database_alias: str = "default"
  use_skip_locked: bool = True
  listen_notify: bool = True
  silence_polling: bool = True
  on_thread_error: str | None = None
  skip_recurring: bool = False
  only_work: bool = False
  only_dispatch: bool = False

  @property
  def has_scheduler_work(self) -> bool:
    return self.scheduler is not None


def load_backend_config(
  backend_alias: str = "default",
  *,
  cli_overrides: Mapping[str, Any] | None = None,
  env: Mapping[str, str] | None = None,
  tasks_settings: Mapping[str, Any] | None = None,
) -> BackendConfig:
  if cli_overrides is None:
    cli_overrides = {}
  if env is None:
    env = os.environ
  if tasks_settings is None:
    tasks_settings = getattr(settings, "TASKS", {})

  return _load_backend_config_cached(
    backend_alias,
    _cache_key(cli_overrides),
    _cache_key({key: env.get(key) for key in CONFIG_ENV_KEYS if env.get(key) is not None}),
    _cache_key(tasks_settings),
  )


@lru_cache(maxsize=None)
def _load_backend_config_cached(
  backend_alias: str,
  cli_overrides_key: str,
  env_key: str,
  tasks_settings_key: str,
) -> BackendConfig:
  cli_overrides = json.loads(cli_overrides_key)
  env = json.loads(env_key)
  tasks_settings = json.loads(tasks_settings_key)
  backend_block = _backend_block(tasks_settings, backend_alias)
  resolved_options = _resolved_options(backend_block, cli_overrides, env)

  mode = resolved_options["mode"]
  if mode not in {"fork", "async"}:
    raise ImproperlyConfigured(f"dj_queue mode must be 'fork' or 'async', got {mode!r}")

  only_work = bool(cli_overrides.get("only_work", False))
  only_dispatch = bool(cli_overrides.get("only_dispatch", False))
  if only_work and only_dispatch:
    raise ImproperlyConfigured("--only-work and --only-dispatch cannot be combined")

  skip_recurring = _resolve_skip_recurring(cli_overrides, env)
  on_thread_error = _validated_callback_path(resolved_options.get("on_thread_error"))
  recurring = _build_recurring_config(resolved_options.get("recurring", {}))
  scheduler = _build_scheduler_config(resolved_options.get("scheduler", DEFAULT_SCHEDULER))
  workers = _build_worker_configs(resolved_options.get("workers", []), mode)
  dispatchers = _build_dispatcher_configs(resolved_options.get("dispatchers", []))

  if only_work:
    dispatchers = ()
    scheduler = None
  elif only_dispatch:
    workers = ()
    scheduler = None
  elif skip_recurring or not _scheduler_has_work(
    scheduler,
    recurring,
    preserve_finished_jobs=bool(resolved_options["preserve_finished_jobs"]),
    clear_finished_jobs_after=resolved_options["clear_finished_jobs_after"],
  ):
    scheduler = None

  if not workers and not dispatchers and scheduler is None:
    raise ImproperlyConfigured(
      "dj_queue requires at least one worker, dispatcher, or scheduler workload"
    )

  return BackendConfig(
    backend_alias=backend_alias,
    allowed_queues=_as_string_tuple(backend_block.get("QUEUES", [])),
    mode=mode,
    workers=workers,
    dispatchers=dispatchers,
    scheduler=scheduler,
    recurring=recurring,
    process_heartbeat_interval=int(resolved_options["process_heartbeat_interval"]),
    process_alive_threshold=int(resolved_options["process_alive_threshold"]),
    shutdown_timeout=int(resolved_options["shutdown_timeout"]),
    supervisor_pidfile=resolved_options["supervisor_pidfile"],
    preserve_finished_jobs=bool(resolved_options["preserve_finished_jobs"]),
    clear_finished_jobs_after=_optional_int(resolved_options["clear_finished_jobs_after"]),
    default_concurrency_duration=int(resolved_options["default_concurrency_duration"]),
    database_alias=str(resolved_options["database_alias"]),
    use_skip_locked=bool(resolved_options["use_skip_locked"]),
    listen_notify=bool(resolved_options["listen_notify"]),
    silence_polling=bool(resolved_options["silence_polling"]),
    on_thread_error=on_thread_error,
    skip_recurring=skip_recurring,
    only_work=only_work,
    only_dispatch=only_dispatch,
  )


def _backend_block(
  tasks_settings: Mapping[str, Any] | None,
  backend_alias: str,
) -> Mapping[str, Any]:
  resolved_tasks_settings = tasks_settings
  if resolved_tasks_settings is None:
    resolved_tasks_settings = getattr(settings, "TASKS", {})

  backend_block = resolved_tasks_settings.get(backend_alias, {})
  if not isinstance(backend_block, Mapping):
    raise ImproperlyConfigured(f"TASKS[{backend_alias!r}] must be a mapping")
  return backend_block


def _resolved_options(
  backend_block: Mapping[str, Any],
  cli_overrides: Mapping[str, Any],
  env: Mapping[str, str],
) -> dict[str, Any]:
  settings_options = backend_block.get("OPTIONS", {})
  if not isinstance(settings_options, Mapping):
    raise ImproperlyConfigured("TASKS backend OPTIONS must be a mapping")

  resolved_options = dict(DEFAULT_OPTIONS)
  resolved_options.update(settings_options)

  config_path = cli_overrides.get("config") or env.get("DJ_QUEUE_CONFIG")
  resolved_options.update(_load_yaml_options(config_path))

  env_mode = env.get("DJ_QUEUE_MODE")
  if env_mode is not None:
    resolved_options["mode"] = env_mode

  cli_mode = cli_overrides.get("mode")
  if cli_mode is not None:
    resolved_options["mode"] = cli_mode

  return resolved_options


def _load_yaml_options(config_path: Any) -> dict[str, Any]:
  if not config_path:
    return {}

  config_payload = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
  if config_payload is None:
    return {}
  if not isinstance(config_payload, dict):
    raise ImproperlyConfigured("DJ_QUEUE_CONFIG must point to a YAML mapping")
  return config_payload


def _resolve_skip_recurring(
  cli_overrides: Mapping[str, Any],
  env: Mapping[str, str],
) -> bool:
  if "skip_recurring" in cli_overrides:
    return bool(cli_overrides["skip_recurring"])

  value = env.get("DJ_QUEUE_SKIP_RECURRING")
  if value is None:
    return False
  return _parse_bool(value, "DJ_QUEUE_SKIP_RECURRING")


def _parse_bool(value: str, setting_name: str) -> bool:
  normalized = value.strip().lower()
  if normalized in TRUTHY_ENV_VALUES:
    return True
  if normalized in FALSY_ENV_VALUES:
    return False
  raise ImproperlyConfigured(
    f"{setting_name} must be one of {sorted(TRUTHY_ENV_VALUES | FALSY_ENV_VALUES)}"
  )


def _validated_callback_path(callback_path: Any) -> str | None:
  if callback_path in (None, ""):
    return None

  callback_path = str(callback_path)
  try:
    import_string(callback_path)
  except ImportError as exc:
    raise ImproperlyConfigured(
      f"dj_queue on_thread_error must be importable: {callback_path}"
    ) from exc
  return callback_path


def _build_worker_configs(raw_workers: Any, mode: str) -> tuple[WorkerConfig, ...]:
  workers: list[WorkerConfig] = []
  for raw_worker in raw_workers or []:
    if not isinstance(raw_worker, Mapping):
      raise ImproperlyConfigured("worker entries must be mappings")

    worker = WorkerConfig(
      queues=_as_queue_selectors(raw_worker.get("queues", DEFAULT_WORKER["queues"])),
      threads=int(raw_worker.get("threads", DEFAULT_WORKER["threads"])),
      processes=int(raw_worker.get("processes", DEFAULT_WORKER["processes"])),
      polling_interval=float(
        raw_worker.get("polling_interval", DEFAULT_WORKER["polling_interval"])
      ),
    )

    if mode == "async" and worker.processes > 1:
      warnings.warn(
        "dj_queue async mode ignores worker processes > 1; normalizing to 1",
        UserWarning,
        stacklevel=3,
      )
      worker = replace(worker, processes=1)

    workers.append(worker)
  return tuple(workers)


def _build_dispatcher_configs(raw_dispatchers: Any) -> tuple[DispatcherConfig, ...]:
  dispatchers: list[DispatcherConfig] = []
  for raw_dispatcher in raw_dispatchers or []:
    if not isinstance(raw_dispatcher, Mapping):
      raise ImproperlyConfigured("dispatcher entries must be mappings")

    dispatchers.append(
      DispatcherConfig(
        batch_size=int(raw_dispatcher.get("batch_size", DEFAULT_DISPATCHER["batch_size"])),
        polling_interval=float(
          raw_dispatcher.get("polling_interval", DEFAULT_DISPATCHER["polling_interval"])
        ),
        concurrency_maintenance=bool(
          raw_dispatcher.get(
            "concurrency_maintenance",
            DEFAULT_DISPATCHER["concurrency_maintenance"],
          )
        ),
        concurrency_maintenance_interval=int(
          raw_dispatcher.get(
            "concurrency_maintenance_interval",
            DEFAULT_DISPATCHER["concurrency_maintenance_interval"],
          )
        ),
      )
    )
  return tuple(dispatchers)


def _build_scheduler_config(raw_scheduler: Any) -> SchedulerConfig:
  if raw_scheduler is None:
    raw_scheduler = {}
  if not isinstance(raw_scheduler, Mapping):
    raise ImproperlyConfigured("scheduler config must be a mapping")

  return SchedulerConfig(
    dynamic_tasks_enabled=bool(
      raw_scheduler.get(
        "dynamic_tasks_enabled",
        DEFAULT_SCHEDULER["dynamic_tasks_enabled"],
      )
    ),
    polling_interval=float(
      raw_scheduler.get("polling_interval", DEFAULT_SCHEDULER["polling_interval"])
    ),
  )


def _build_recurring_config(raw_recurring: Any) -> dict[str, RecurringTaskConfig]:
  if raw_recurring is None:
    return {}
  if not isinstance(raw_recurring, Mapping):
    raise ImproperlyConfigured("recurring config must be a mapping")

  recurring: dict[str, RecurringTaskConfig] = {}
  for key, raw_entry in raw_recurring.items():
    if not isinstance(raw_entry, Mapping):
      raise ImproperlyConfigured("recurring entries must be mappings")

    task_path = raw_entry.get("task_path")
    schedule = raw_entry.get("schedule")
    if not task_path or not schedule:
      raise ImproperlyConfigured(f"recurring task {key!r} requires task_path and schedule")
    if not croniter.is_valid(str(schedule)):
      raise ImproperlyConfigured(f"recurring task {key!r} has an invalid cron schedule")

    recurring[str(key)] = RecurringTaskConfig(
      key=str(key),
      task_path=str(task_path),
      schedule=str(schedule),
      args=tuple(raw_entry.get("args", [])),
      kwargs=dict(raw_entry.get("kwargs", {})),
      queue_name=str(raw_entry.get("queue_name", "default")),
      priority=int(raw_entry.get("priority", 0)),
      description=str(raw_entry.get("description", "")),
    )
  return recurring


def _scheduler_has_work(
  scheduler: SchedulerConfig,
  recurring: Mapping[str, RecurringTaskConfig],
  *,
  preserve_finished_jobs: bool,
  clear_finished_jobs_after: Any,
) -> bool:
  has_cleanup = preserve_finished_jobs and clear_finished_jobs_after is not None
  return scheduler.dynamic_tasks_enabled or bool(recurring) or has_cleanup


def _as_queue_selectors(value: Any) -> tuple[str, ...]:
  if isinstance(value, str):
    return (value,)
  return _as_string_tuple(value)


def _as_string_tuple(value: Any) -> tuple[str, ...]:
  if value in (None, []):
    return ()
  if isinstance(value, str):
    return (value,)
  if not isinstance(value, Sequence):
    raise ImproperlyConfigured("expected a string or a sequence of strings")
  return tuple(str(item) for item in value)


def _optional_int(value: Any) -> int | None:
  if value is None:
    return None
  return int(value)


def _cache_key(value: Any) -> str:
  return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
