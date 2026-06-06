from benchmarks.scenarios.enqueue import bulk_enqueue, single_enqueue
from benchmarks.scenarios.runtime import (
  concurrency_contention,
  held_xmin_worker_drain,
  ordered_selector_claim,
  runtime_hot_key_contention,
  worker_drain,
)
from benchmarks.scenarios.scheduling import recurring_scale, scheduled_promotion

SCENARIOS = {
  "single-enqueue": single_enqueue,
  "bulk-enqueue": bulk_enqueue,
  "scheduled-promotion": scheduled_promotion,
  "recurring-scale": recurring_scale,
  "worker-drain": worker_drain,
  "held-xmin-worker-drain": held_xmin_worker_drain,
  "concurrency-contention": concurrency_contention,
  "runtime-hot-key-contention": runtime_hot_key_contention,
  "ordered-selector-claim": ordered_selector_claim,
}

QUICK_SCENARIOS = (
  "single-enqueue",
  "bulk-enqueue",
  "scheduled-promotion",
  "recurring-scale",
  "worker-drain",
  "concurrency-contention",
  "runtime-hot-key-contention",
  "ordered-selector-claim",
)
