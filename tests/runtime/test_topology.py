from dj_queue.config import BackendConfig, DispatcherConfig, SchedulerConfig, WorkerConfig
from dj_queue.runtime.dispatcher import Dispatcher
from dj_queue.runtime.scheduler import Scheduler
from dj_queue.runtime.topology import runner_definitions
from dj_queue.runtime.worker import Worker


def test_runner_definitions_expand_configured_topology():
  worker_config = WorkerConfig(processes=2)
  dispatcher_config = DispatcherConfig()
  config = BackendConfig(
    workers=(worker_config,),
    dispatchers=(dispatcher_config,),
    scheduler=SchedulerConfig(),
  )

  definitions = runner_definitions(config)

  assert [
    (definition.kind, definition.runner_class, definition.name) for definition in definitions
  ] == [
    ("worker", Worker, "worker-1-1"),
    ("worker", Worker, "worker-1-2"),
    ("dispatcher", Dispatcher, "dispatcher-1"),
    ("scheduler", Scheduler, "scheduler-1"),
  ]
  assert definitions[0].config is worker_config
  assert definitions[2].config is dispatcher_config
  assert definitions[3].config is config
