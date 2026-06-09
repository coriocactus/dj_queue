# dj_queue mariadb Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-06-09T14:21:28.899649+00:00

## Environment

- backend: `mariadb`
- database: `mysql` `dj_queue_benchmark`
- database version: `10.6.25-MariaDB-ubu2204`
- Python: `3.14.5`
- Django: `6.0.5`
- dj_queue: `0.12.0`
- platform: `macOS-26.5.1-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `cef37cdd23be`
- benchmark worker count: `4`
- benchmark worker threads: `8`
- preserve finished jobs: `True`
- database CONN_MAX_AGE: `60`

## Results

### `single-enqueue`: one-by-one immediate enqueue latency and throughput

- key metric: **`latency_p95_ms`** - enqueue tail latency for individual task submissions; lower is better
- healthy local baseline: `<= 15 ms` p95 for request-path enqueue on the 10k local benchmark
- use case: web requests, admin actions, and small fan-out paths that submit tasks one at a time
- mechanics: calls the public `Task.enqueue()` path once per job, including validation, job insert, ready-row insert, result mapping, and ready wakeup registration

| size | run | duration_seconds | jobs_per_second | query_count_sample | **latency_p95_ms** | ready_count | job_count | latency_mean_ms | latency_p50_ms | latency_p99_ms |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 4.565 | 219.074 | 5 | **7.218** | 1000 | 1000 | 4.562 | 4.254 | 10.491 |
| 1000 | 1 | 3.212 | 311.351 | 5 | **4.994** | 1000 | 1000 | 3.210 | 3.205 | 6.692 |
| 1000 | 2 | 2.346 | 426.285 | 5 | **4.018** | 1000 | 1000 | 2.345 | 2.019 | 5.829 |
| 10000 | 0 | 24.858 | 402.277 | 5 | **3.968** | 10000 | 10000 | 2.485 | 2.402 | 5.346 |
| 10000 | 1 | 24.963 | 400.598 | 5 | **3.935** | 10000 | 10000 | 2.495 | 2.360 | 5.264 |
| 10000 | 2 | 21.911 | 456.395 | 5 | **3.114** | 10000 | 10000 | 2.190 | 2.015 | 3.813 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.115 | **8693.693** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.205 | **4877.236** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.154 | **6477.203** | 5 | 1000 | 1000 |
| 10000 | 0 | 1.252 | **7988.947** | 5 | 10000 | 10000 |
| 10000 | 1 | 1.165 | **8581.041** | 5 | 10000 | 10000 |
| 10000 | 2 | 1.297 | **7708.352** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.122 | **8214.770** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.109 | **9191.722** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.113 | **8853.024** | 1000 | 1000 | 1000 |
| 10000 | 0 | 0.946 | **10573.745** | 10000 | 10000 | 10000 |
| 10000 | 1 | 0.937 | **10667.487** | 10000 | 10000 | 10000 |
| 10000 | 2 | 0.936 | **10686.133** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | query_count | fired_count | recurring_task_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | **0.001** | 1591089.864 | 1 | 0 | 1000 |
| 1000 | 1 | **0.001** | 1559352.011 | 1 | 0 | 1000 |
| 1000 | 2 | **0.001** | 1329419.423 | 1 | 0 | 1000 |
| 10000 | 0 | **0.001** | 14090281.818 | 1 | 0 | 10000 |
| 10000 | 1 | **0.001** | 16274479.219 | 1 | 0 | 10000 |
| 10000 | 2 | **0.001** | 9771588.897 | 1 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.022 | **978.025** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.084 | **922.476** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.096 | **912.696** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 10.961 | **912.359** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 9.983 | **1001.751** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 9.529 | **1049.461** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 146.813 | 3001 | **50.574** | 11998 | 5000 | 6998 | 1000 | 19.773 | 6.811 |
| 1000 | 1 | 191.002 | 3001 | **48.844** | 11998 | 5000 | 6998 | 1000 | 20.473 | 5.236 |
| 1000 | 2 | 177.365 | 3001 | **51.085** | 11998 | 5000 | 6998 | 1000 | 19.575 | 5.638 |
| 10000 | 0 | 178.170 | 30001 | **40.254** | 119998 | 50000 | 69998 | 10000 | 248.420 | 56.126 |
| 10000 | 1 | 169.788 | 30001 | **40.172** | 119998 | 50000 | 69998 | 10000 | 248.930 | 58.897 |
| 10000 | 2 | 165.839 | 30001 | **54.984** | 119998 | 50000 | 69998 | 10000 | 181.871 | 60.300 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 453.518 | **138.973** | 1000 | 0 | 0 | 0 | 1000 | 7.196 | 2.205 | 1000 | True | 4 |
| 1000 | 1 | 462.598 | **136.001** | 1000 | 0 | 0 | 0 | 1000 | 7.353 | 2.162 | 1000 | True | 4 |
| 1000 | 2 | 555.180 | **142.880** | 1000 | 0 | 0 | 0 | 1000 | 6.999 | 1.801 | 1000 | True | 4 |
| 10000 | 0 | 376.651 | **85.533** | 10000 | 0 | 0 | 0 | 10000 | 116.914 | 26.550 | 10000 | True | 4 |
| 10000 | 1 | 194.401 | **87.096** | 10000 | 0 | 0 | 0 | 10000 | 114.816 | 51.440 | 10000 | True | 4 |
| 10000 | 2 | 199.229 | **91.027** | 10000 | 0 | 0 | 0 | 10000 | 109.858 | 50.194 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 2.984 | **335.075** | 1.453 | 2005 | 1.526 | 2000 | 1000 |
| 1000 | 1 | 3.401 | **294.047** | 1.604 | 2005 | 1.791 | 2000 | 1000 |
| 1000 | 2 | 3.653 | **273.728** | 1.677 | 2005 | 1.970 | 2000 | 1000 |
| 10000 | 0 | 41.134 | **243.109** | 21.724 | 20005 | 19.343 | 20000 | 10000 |
| 10000 | 1 | 35.984 | **277.898** | 19.675 | 20005 | 16.252 | 20000 | 10000 |
| 10000 | 2 | 39.840 | **251.004** | 21.493 | 20005 | 18.282 | 20000 | 10000 |

## Reproduce

```bash
docker compose up mariadb -d
bin/benchmark.py all --backend mariadb --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mariadb-20260609T134341Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mariadb-20260609T134341Z.jsonl --output docs/benchmarks/mariadb.md
```
