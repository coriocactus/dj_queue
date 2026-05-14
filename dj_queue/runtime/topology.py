from dataclasses import dataclass

from dj_queue.runtime.dispatcher import Dispatcher
from dj_queue.runtime.scheduler import Scheduler
from dj_queue.runtime.worker import Worker


@dataclass(frozen=True, slots=True)
class RunnerDefinition:
  kind: str
  runner_class: type
  config: object
  name: str


def runner_definitions(config):
  definitions = []

  for index, worker_config in enumerate(config.workers, start=1):
    for process_index in range(worker_config.processes):
      suffix = index if worker_config.processes == 1 else f"{index}-{process_index + 1}"
      definitions.append(
        RunnerDefinition(
          kind="worker",
          runner_class=Worker,
          config=worker_config,
          name=f"worker-{suffix}",
        )
      )

  for index, dispatcher_config in enumerate(config.dispatchers, start=1):
    definitions.append(
      RunnerDefinition(
        kind="dispatcher",
        runner_class=Dispatcher,
        config=dispatcher_config,
        name=f"dispatcher-{index}",
      )
    )

  if config.scheduler is not None:
    definitions.append(
      RunnerDefinition(
        kind="scheduler",
        runner_class=Scheduler,
        config=config,
        name="scheduler-1",
      )
    )

  return definitions
