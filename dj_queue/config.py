import json
import math
import os
import tomllib
import warnings
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string

from dj_queue.cron import is_valid_cron

DEFAULT_WORKER = {
  "queues": "*",
  "threads": 3,
  "processes": 1,
  "polling_interval": 0.1,
  "prefetch_multiplier": 2,
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
  "clear_failed_jobs_after": None,
  "clear_recurring_executions_after": None,
  "default_concurrency_duration": 180,
  "database_alias": "default",
  "use_skip_locked": True,
  "listen_notify": True,
  "silence_polling": True,
  "async_thread_sensitive": False,
  "async_close_connections": False,
  "on_thread_error": None,
}

DJ_QUEUE_BACKEND_PATH = "dj_queue.backend.DjQueueBackend"

TRUTHY_ENV_VALUES = {"1", "true", "yes", "on"}
FALSY_ENV_VALUES = {"0", "false", "no", "off"}
CONFIG_ENV_KEYS = ("DJ_QUEUE_CONFIG", "DJ_QUEUE_MODE", "DJ_QUEUE_SKIP_RECURRING")
_BACKEND_CONFIG_CACHE = {}


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
  prefetch_multiplier: int = 2


@dataclass(frozen=True, slots=True)
class DispatcherConfig(ConfigValue):
  batch_size: int = 500
  polling_interval: float = 1
  concurrency_maintenance: bool = True
  concurrency_maintenance_interval: float = 600


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
  process_heartbeat_interval: float = 60
  process_alive_threshold: float = 300
  shutdown_timeout: float = 5
  supervisor_pidfile: str | None = None
  preserve_finished_jobs: bool = True
  clear_finished_jobs_after: int | None = 86400
  clear_failed_jobs_after: int | None = None
  clear_recurring_executions_after: int | None = None
  default_concurrency_duration: int = 180
  database_alias: str = "default"
  use_skip_locked: bool = True
  listen_notify: bool = True
  silence_polling: bool = True
  async_thread_sensitive: bool = False
  async_close_connections: bool = False
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

  ensure_dj_queue_backend_alias(tasks_settings, backend_alias)
  backend_block = _backend_block(tasks_settings, backend_alias)
  env_values = {key: env.get(key) for key in CONFIG_ENV_KEYS if env.get(key) is not None}
  cache_key = (
    backend_alias,
    _cache_key(cli_overrides),
    _cache_key(env_values),
    _cache_key(backend_block),
  )
  if cache_key not in _BACKEND_CONFIG_CACHE:
    _BACKEND_CONFIG_CACHE[cache_key] = _load_backend_config_uncached(
      backend_alias,
      cli_overrides,
      env_values,
      tasks_settings,
    )
  return _BACKEND_CONFIG_CACHE[cache_key]


def load_allowed_queues(
  backend_alias: str = "default",
  *,
  tasks_settings: Mapping[str, Any] | None = None,
) -> tuple[str, ...]:
  if tasks_settings is None:
    tasks_settings = getattr(settings, "TASKS", {})
  ensure_dj_queue_backend_alias(tasks_settings, backend_alias)
  backend_block = _backend_block(tasks_settings, backend_alias)
  return _as_string_tuple(backend_block.get("QUEUES", []))


def _load_backend_config_uncached(
  backend_alias: str,
  cli_overrides: Mapping[str, Any],
  env: Mapping[str, str],
  tasks_settings: Mapping[str, Any],
) -> BackendConfig:
  ensure_dj_queue_backend_alias(tasks_settings, backend_alias)
  backend_block = _backend_block(tasks_settings, backend_alias)
  resolved_options = _resolved_options(backend_alias, backend_block, cli_overrides, env)

  mode = resolved_options["mode"]
  if mode not in {"fork", "async"}:
    raise ImproperlyConfigured(f"dj_queue mode must be 'fork' or 'async', got {mode!r}")

  only_work = _bool_option(cli_overrides.get("only_work", False), "--only-work")
  only_dispatch = _bool_option(cli_overrides.get("only_dispatch", False), "--only-dispatch")
  if only_work and only_dispatch:
    raise ImproperlyConfigured("--only-work and --only-dispatch cannot be combined")

  skip_recurring = _resolve_skip_recurring(cli_overrides, env)
  preserve_finished_jobs = _bool_option(
    resolved_options["preserve_finished_jobs"], "preserve_finished_jobs"
  )
  allowed_queues = _as_string_tuple(backend_block.get("QUEUES", []))
  on_thread_error = _validated_callback_path(resolved_options.get("on_thread_error"))
  recurring = _build_recurring_config(
    resolved_options.get("recurring", {}),
    allowed_queues=allowed_queues,
    backend_alias=backend_alias,
  )
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
    preserve_finished_jobs=preserve_finished_jobs,
    clear_finished_jobs_after=resolved_options["clear_finished_jobs_after"],
    clear_failed_jobs_after=resolved_options["clear_failed_jobs_after"],
    clear_recurring_executions_after=resolved_options["clear_recurring_executions_after"],
  ):
    scheduler = None

  if not workers and not dispatchers and scheduler is None:
    raise ImproperlyConfigured(
      "dj_queue requires at least one worker, dispatcher, or scheduler workload"
    )

  return BackendConfig(
    backend_alias=backend_alias,
    allowed_queues=allowed_queues,
    mode=mode,
    workers=workers,
    dispatchers=dispatchers,
    scheduler=scheduler,
    recurring=recurring,
    process_heartbeat_interval=_nonnegative_float(
      resolved_options["process_heartbeat_interval"], "process_heartbeat_interval"
    ),
    process_alive_threshold=_positive_float(
      resolved_options["process_alive_threshold"], "process_alive_threshold"
    ),
    shutdown_timeout=_nonnegative_float(resolved_options["shutdown_timeout"], "shutdown_timeout"),
    supervisor_pidfile=_optional_string_option(
      resolved_options["supervisor_pidfile"], "supervisor_pidfile"
    ),
    preserve_finished_jobs=preserve_finished_jobs,
    clear_finished_jobs_after=_optional_nonnegative_int(
      resolved_options["clear_finished_jobs_after"], "clear_finished_jobs_after"
    ),
    clear_failed_jobs_after=_optional_nonnegative_int(
      resolved_options["clear_failed_jobs_after"], "clear_failed_jobs_after"
    ),
    clear_recurring_executions_after=_optional_nonnegative_int(
      resolved_options["clear_recurring_executions_after"],
      "clear_recurring_executions_after",
    ),
    default_concurrency_duration=_positive_int(
      resolved_options["default_concurrency_duration"],
      "default_concurrency_duration",
    ),
    database_alias=_string_option(resolved_options["database_alias"], "database_alias"),
    use_skip_locked=_bool_option(resolved_options["use_skip_locked"], "use_skip_locked"),
    listen_notify=_bool_option(resolved_options["listen_notify"], "listen_notify"),
    silence_polling=_bool_option(resolved_options["silence_polling"], "silence_polling"),
    async_thread_sensitive=_bool_option(
      resolved_options["async_thread_sensitive"], "async_thread_sensitive"
    ),
    async_close_connections=_bool_option(
      resolved_options["async_close_connections"], "async_close_connections"
    ),
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

  if not resolved_tasks_settings:
    if backend_alias == "default":
      return {}
    raise ImproperlyConfigured(f"dj_queue backend alias {backend_alias!r} is not configured")

  if backend_alias not in resolved_tasks_settings:
    raise ImproperlyConfigured(f"dj_queue backend alias {backend_alias!r} is not configured")

  backend_block = resolved_tasks_settings[backend_alias]
  if not isinstance(backend_block, Mapping):
    raise ImproperlyConfigured(f"TASKS[{backend_alias!r}] must be a mapping")
  return backend_block


def configured_backend_aliases(
  tasks_settings: Mapping[str, Any] | None = None,
) -> tuple[str, ...]:
  resolved_tasks_settings = tasks_settings
  if resolved_tasks_settings is None:
    resolved_tasks_settings = getattr(settings, "TASKS", {})

  if not resolved_tasks_settings:
    return ("default",)

  aliases = []
  for alias, backend_block in resolved_tasks_settings.items():
    if not isinstance(backend_block, Mapping):
      raise ImproperlyConfigured(f"TASKS[{alias!r}] must be a mapping")
    if is_dj_queue_backend_alias(backend_block):
      aliases.append(alias)
  return tuple(aliases)


def ensure_dj_queue_backend_alias(
  tasks_settings: Mapping[str, Any] | None,
  backend_alias: str,
) -> None:
  aliases = configured_backend_aliases(tasks_settings)
  if backend_alias in aliases:
    return
  raise ImproperlyConfigured(
    f"dj_queue backend alias {backend_alias!r} is not configured for DjQueueBackend"
  )


def is_dj_queue_backend_alias(backend_block: Mapping[str, Any]) -> bool:
  backend_path = backend_block.get("BACKEND")
  return backend_path == DJ_QUEUE_BACKEND_PATH


def _resolved_options(
  backend_alias: str,
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
  resolved_options.update(_load_toml_options(config_path, backend_alias=backend_alias))

  env_mode = env.get("DJ_QUEUE_MODE")
  if env_mode is not None:
    resolved_options["mode"] = env_mode

  cli_mode = cli_overrides.get("mode")
  if cli_mode is not None:
    resolved_options["mode"] = cli_mode

  return resolved_options


def _load_toml_options(config_path: Any, *, backend_alias: str) -> dict[str, Any]:
  if not config_path:
    return {}

  try:
    config_payload = tomllib.loads(Path(config_path).read_text(encoding="utf-8"))
  except OSError as exc:
    raise ImproperlyConfigured(f"DJ_QUEUE_CONFIG could not be read: {exc}") from exc
  except tomllib.TOMLDecodeError as exc:
    raise ImproperlyConfigured(f"DJ_QUEUE_CONFIG TOML is invalid: {exc}") from exc
  if not isinstance(config_payload, dict):
    raise ImproperlyConfigured("DJ_QUEUE_CONFIG must point to a TOML mapping")

  raw_backends = config_payload.get("backends")
  if raw_backends is None:
    return _json_serializable_options(config_payload, "DJ_QUEUE_CONFIG")

  if len(config_payload) != 1:
    raise ImproperlyConfigured(
      "DJ_QUEUE_CONFIG must use either a flat options mapping or a top-level 'backends' mapping"
    )
  if not isinstance(raw_backends, Mapping):
    raise ImproperlyConfigured("DJ_QUEUE_CONFIG 'backends' must be a mapping")

  backend_options = raw_backends.get(backend_alias)
  if backend_options is None:
    return {}
  if not isinstance(backend_options, Mapping):
    raise ImproperlyConfigured(f"DJ_QUEUE_CONFIG backends[{backend_alias!r}] must be a mapping")
  return _json_serializable_options(
    dict(backend_options),
    f"DJ_QUEUE_CONFIG backends[{backend_alias!r}]",
  )


def _json_serializable_options(options: Mapping[str, Any], setting_name: str) -> dict[str, Any]:
  try:
    json.dumps(options, sort_keys=True, separators=(",", ":"), allow_nan=False)
  except (TypeError, ValueError) as exc:
    raise ImproperlyConfigured(f"{setting_name} values must be JSON-serializable") from exc
  return dict(options)


def _resolve_skip_recurring(
  cli_overrides: Mapping[str, Any],
  env: Mapping[str, str],
) -> bool:
  if "skip_recurring" in cli_overrides:
    return _bool_option(cli_overrides["skip_recurring"], "skip_recurring")

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


def _bool_option(value: Any, setting_name: str) -> bool:
  if isinstance(value, bool):
    return value
  if isinstance(value, str):
    return _parse_bool(value, setting_name)
  if isinstance(value, int) and value in (0, 1):
    return bool(value)
  raise ImproperlyConfigured(f"dj_queue {setting_name} must be a boolean")


def _validated_callback_path(callback_path: Any) -> str | None:
  if callback_path in (None, ""):
    return None

  callback_path = _string_option(callback_path, "on_thread_error")
  try:
    import_string(callback_path)
  except ImportError as exc:
    raise ImproperlyConfigured(
      f"dj_queue on_thread_error must be importable: {callback_path}"
    ) from exc
  return callback_path


def _build_worker_configs(raw_workers: Any, mode: str) -> tuple[WorkerConfig, ...]:
  if isinstance(raw_workers, Mapping):
    raw_workers = [raw_workers]

  workers: list[WorkerConfig] = []
  for index, raw_worker in enumerate(raw_workers or []):
    if not isinstance(raw_worker, Mapping):
      raise ImproperlyConfigured("worker entries must be mappings")

    worker = WorkerConfig(
      queues=_as_queue_selectors(raw_worker.get("queues", DEFAULT_WORKER["queues"])),
      threads=_positive_int(
        raw_worker.get("threads", DEFAULT_WORKER["threads"]), f"workers[{index}].threads"
      ),
      processes=_positive_int(
        raw_worker.get("processes", DEFAULT_WORKER["processes"]),
        f"workers[{index}].processes",
      ),
      polling_interval=_positive_float(
        raw_worker.get("polling_interval", DEFAULT_WORKER["polling_interval"]),
        f"workers[{index}].polling_interval",
      ),
      prefetch_multiplier=_positive_int(
        raw_worker.get("prefetch_multiplier", DEFAULT_WORKER["prefetch_multiplier"]),
        f"workers[{index}].prefetch_multiplier",
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
  if isinstance(raw_dispatchers, Mapping):
    raw_dispatchers = [raw_dispatchers]

  dispatchers: list[DispatcherConfig] = []
  for index, raw_dispatcher in enumerate(raw_dispatchers or []):
    if not isinstance(raw_dispatcher, Mapping):
      raise ImproperlyConfigured("dispatcher entries must be mappings")

    dispatchers.append(
      DispatcherConfig(
        batch_size=_positive_int(
          raw_dispatcher.get("batch_size", DEFAULT_DISPATCHER["batch_size"]),
          f"dispatchers[{index}].batch_size",
        ),
        polling_interval=_positive_float(
          raw_dispatcher.get("polling_interval", DEFAULT_DISPATCHER["polling_interval"]),
          f"dispatchers[{index}].polling_interval",
        ),
        concurrency_maintenance=_bool_option(
          raw_dispatcher.get(
            "concurrency_maintenance",
            DEFAULT_DISPATCHER["concurrency_maintenance"],
          ),
          f"dispatchers[{index}].concurrency_maintenance",
        ),
        concurrency_maintenance_interval=_nonnegative_float(
          raw_dispatcher.get(
            "concurrency_maintenance_interval",
            DEFAULT_DISPATCHER["concurrency_maintenance_interval"],
          ),
          f"dispatchers[{index}].concurrency_maintenance_interval",
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
    dynamic_tasks_enabled=_bool_option(
      raw_scheduler.get(
        "dynamic_tasks_enabled",
        DEFAULT_SCHEDULER["dynamic_tasks_enabled"],
      ),
      "scheduler.dynamic_tasks_enabled",
    ),
    polling_interval=_positive_float(
      raw_scheduler.get("polling_interval", DEFAULT_SCHEDULER["polling_interval"]),
      "scheduler.polling_interval",
    ),
  )


def _build_recurring_config(
  raw_recurring: Any,
  *,
  allowed_queues: tuple[str, ...],
  backend_alias: str,
) -> dict[str, RecurringTaskConfig]:
  if raw_recurring is None:
    return {}
  if not isinstance(raw_recurring, Mapping):
    raise ImproperlyConfigured("recurring config must be a mapping")

  recurring: dict[str, RecurringTaskConfig] = {}
  for key, raw_entry in raw_recurring.items():
    key = _nonempty_string_option(key, "recurring task key")
    if not isinstance(raw_entry, Mapping):
      raise ImproperlyConfigured("recurring entries must be mappings")

    raw_task_path = raw_entry.get("task_path")
    raw_schedule = raw_entry.get("schedule")
    if raw_task_path in (None, "") or raw_schedule in (None, ""):
      raise ImproperlyConfigured(f"recurring task {key!r} requires task_path and schedule")
    task_path = _nonempty_string_option(raw_task_path, f"recurring task {key!r} task_path")
    schedule = _nonempty_string_option(raw_schedule, f"recurring task {key!r} schedule")
    if not is_valid_cron(schedule):
      raise ImproperlyConfigured(f"recurring task {key!r} has an invalid cron schedule")

    queue_name = _nonempty_string_option(
      raw_entry.get("queue_name", "default"), f"recurring task {key!r} queue_name"
    )
    priority = _priority_int(raw_entry.get("priority", 0), f"recurring task {key!r} priority")
    if allowed_queues and queue_name not in allowed_queues:
      raise ImproperlyConfigured(
        f"recurring task {key!r} is invalid: queue {queue_name!r} is not allowed for backend {backend_alias!r}"
      )
    args = _tuple_option(raw_entry.get("args", []), f"recurring task {key!r} args")
    kwargs = _dict_option(raw_entry.get("kwargs", {}), f"recurring task {key!r} kwargs")
    try:
      task = import_string(task_path)
    except ImportError as exc:
      raise ImproperlyConfigured(f"recurring task {key!r} is invalid: {exc}") from exc
    if not hasattr(task, "using"):
      raise ImproperlyConfigured(
        f"recurring task {key!r} is invalid: task_path must reference a Django task"
      )

    recurring[key] = RecurringTaskConfig(
      key=key,
      task_path=task_path,
      schedule=schedule,
      args=args,
      kwargs=kwargs,
      queue_name=queue_name,
      priority=priority,
      description=_string_option(
        raw_entry.get("description", ""), f"recurring task {key!r} description"
      ),
    )
  return recurring


def _scheduler_has_work(
  scheduler: SchedulerConfig,
  recurring: Mapping[str, RecurringTaskConfig],
  *,
  preserve_finished_jobs: bool,
  clear_finished_jobs_after: Any,
  clear_failed_jobs_after: Any,
  clear_recurring_executions_after: Any,
) -> bool:
  has_cleanup = (
    (preserve_finished_jobs and clear_finished_jobs_after is not None)
    or clear_failed_jobs_after is not None
    or clear_recurring_executions_after is not None
  )
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
  if not isinstance(value, Sequence) or isinstance(value, (bytes, bytearray)):
    raise ImproperlyConfigured("expected a string or a sequence of strings")
  values = tuple(value)
  if not all(isinstance(item, str) for item in values):
    raise ImproperlyConfigured("expected a string or a sequence of strings")
  return values


def _string_option(value: Any, setting_name: str) -> str:
  if not isinstance(value, str):
    raise ImproperlyConfigured(f"dj_queue {setting_name} must be a string")
  return value


def _nonempty_string_option(value: Any, setting_name: str) -> str:
  value = _string_option(value, setting_name)
  if value == "":
    raise ImproperlyConfigured(f"dj_queue {setting_name} must be a non-empty string")
  return value


def _optional_string_option(value: Any, setting_name: str) -> str | None:
  if value is None:
    return None
  return _string_option(value, setting_name)


def _tuple_option(value: Any, setting_name: str) -> tuple[Any, ...]:
  if isinstance(value, str) or not isinstance(value, Sequence):
    raise ImproperlyConfigured(f"dj_queue {setting_name} must be a sequence")
  return tuple(value)


def _dict_option(value: Any, setting_name: str) -> dict[str, Any]:
  if not isinstance(value, Mapping):
    raise ImproperlyConfigured(f"dj_queue {setting_name} must be a mapping")
  return dict(value)


def _optional_nonnegative_int(value: Any, setting_name: str) -> int | None:
  if value is None:
    return None
  number = _integer(value, setting_name, "a non-negative integer")
  if number < 0:
    raise ImproperlyConfigured(
      f"dj_queue {setting_name} must be a non-negative integer, got {value!r}"
    )
  return number


def _positive_float(value: Any, setting_name: str) -> float:
  if isinstance(value, bool):
    raise ImproperlyConfigured(f"dj_queue {setting_name} must be a positive number, got {value!r}")
  try:
    number = float(value)
  except (TypeError, ValueError) as exc:
    raise ImproperlyConfigured(
      f"dj_queue {setting_name} must be a positive number, got {value!r}"
    ) from exc

  if not math.isfinite(number) or number <= 0:
    raise ImproperlyConfigured(f"dj_queue {setting_name} must be a positive number, got {value!r}")
  return number


def _nonnegative_float(value: Any, setting_name: str) -> float:
  if isinstance(value, bool):
    raise ImproperlyConfigured(
      f"dj_queue {setting_name} must be a non-negative number, got {value!r}"
    )
  try:
    number = float(value)
  except (TypeError, ValueError) as exc:
    raise ImproperlyConfigured(
      f"dj_queue {setting_name} must be a non-negative number, got {value!r}"
    ) from exc

  if not math.isfinite(number) or number < 0:
    raise ImproperlyConfigured(
      f"dj_queue {setting_name} must be a non-negative number, got {value!r}"
    )
  return number


def _positive_int(value: Any, setting_name: str) -> int:
  number = _integer(value, setting_name, "a positive integer")

  if number <= 0:
    raise ImproperlyConfigured(
      f"dj_queue {setting_name} must be a positive integer, got {value!r}"
    )
  return number


def _priority_int(value: Any, setting_name: str) -> int:
  number = _integer(value, setting_name, "an integer from -100 to 100")

  if number < -100 or number > 100:
    raise ImproperlyConfigured(
      f"dj_queue {setting_name} must be an integer from -100 to 100, got {value!r}"
    )
  return number


def _integer(value: Any, setting_name: str, expectation: str) -> int:
  if isinstance(value, bool):
    raise ImproperlyConfigured(f"dj_queue {setting_name} must be {expectation}, got {value!r}")
  if isinstance(value, int):
    return value
  if isinstance(value, str):
    normalized = value.strip()
    unsigned = normalized[1:] if normalized[:1] in {"+", "-"} else normalized
    if unsigned.isdecimal():
      return int(normalized)
  raise ImproperlyConfigured(f"dj_queue {setting_name} must be {expectation}, got {value!r}")


def _cache_key(value: Any) -> str:
  try:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
  except (TypeError, ValueError) as exc:
    raise ImproperlyConfigured("dj_queue config values must be JSON-serializable") from exc
