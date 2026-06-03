# dj_queue mariadb Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-06-03T06:57:47.506502+00:00

## Environment

- backend: `mariadb`
- database: `mysql` `dj_queue_benchmark`
- database version: `10.6.25-MariaDB-ubu2204`
- Python: `3.14.5`
- Django: `6.0.5`
- dj_queue: `0.11.0`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `cb4d0997597c`
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
| 1000 | 0 | 6.920 | 144.515 | 5 | **11.690** | 1000 | 1000 | 6.916 | 5.973 | 14.392 |
| 1000 | 1 | 5.922 | 168.874 | 5 | **10.640** | 1000 | 1000 | 5.919 | 5.311 | 12.554 |
| 1000 | 2 | 7.279 | 137.384 | 5 | **11.494** | 1000 | 1000 | 7.275 | 6.618 | 15.574 |
| 10000 | 0 | 75.979 | 131.615 | 5 | **12.081** | 10000 | 10000 | 7.594 | 7.030 | 14.169 |
| 10000 | 1 | 78.156 | 127.949 | 5 | **12.012** | 10000 | 10000 | 7.812 | 7.290 | 14.162 |
| 10000 | 2 | 76.942 | 129.968 | 5 | **11.988** | 10000 | 10000 | 7.691 | 7.095 | 14.666 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.259 | **3862.558** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.268 | **3737.428** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.279 | **3578.537** | 5 | 1000 | 1000 |
| 10000 | 0 | 1.468 | **6812.770** | 5 | 10000 | 10000 |
| 10000 | 1 | 1.235 | **8098.552** | 5 | 10000 | 10000 |
| 10000 | 2 | 1.273 | **7854.203** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.143 | **6976.010** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.130 | **7688.402** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.141 | **7098.361** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.142 | **8753.352** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.063 | **9407.601** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.017 | **9836.253** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.006** | 165916.586 | 0 | 1000 |
| 1000 | 1 | **0.008** | 120704.300 | 0 | 1000 |
| 1000 | 2 | **0.007** | 143344.350 | 0 | 1000 |
| 10000 | 0 | **0.011** | 883496.342 | 0 | 10000 |
| 10000 | 1 | **0.035** | 282022.482 | 0 | 10000 |
| 10000 | 2 | **0.024** | 419445.493 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.328 | **752.949** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.380 | **724.450** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.272 | **785.869** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 12.705 | **787.091** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 12.756 | **783.939** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 12.668 | **789.395** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 167.533 | 3001 | **42.682** | 11998 | 5000 | 6998 | 1000 | 23.429 | 5.969 |
| 1000 | 1 | 160.500 | 3001 | **42.460** | 11998 | 5000 | 6998 | 1000 | 23.552 | 6.231 |
| 1000 | 2 | 171.219 | 3001 | **41.379** | 11998 | 5000 | 6998 | 1000 | 24.167 | 5.840 |
| 10000 | 0 | 134.151 | 30001 | **49.312** | 119998 | 50000 | 69998 | 10000 | 202.790 | 74.543 |
| 10000 | 1 | 145.357 | 30001 | **48.699** | 119998 | 50000 | 69998 | 10000 | 205.344 | 68.796 |
| 10000 | 2 | 138.654 | 30001 | **48.684** | 119998 | 50000 | 69998 | 10000 | 205.404 | 72.122 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 151.771 | **78.995** | 1000 | 0 | 0 | 0 | 1000 | 12.659 | 6.589 | 1000 | True | 4 |
| 1000 | 1 | 156.531 | **77.261** | 1000 | 0 | 0 | 0 | 1000 | 12.943 | 6.389 | 1000 | True | 4 |
| 1000 | 2 | 149.370 | **84.308** | 1000 | 0 | 0 | 0 | 1000 | 11.861 | 6.695 | 1000 | True | 4 |
| 10000 | 0 | 151.685 | **74.395** | 10000 | 0 | 0 | 0 | 10000 | 134.418 | 65.926 | 10000 | True | 4 |
| 10000 | 1 | 147.495 | **74.419** | 10000 | 0 | 0 | 0 | 10000 | 134.374 | 67.799 | 10000 | True | 4 |
| 10000 | 2 | 151.696 | **71.547** | 10000 | 0 | 0 | 0 | 10000 | 139.768 | 65.921 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 9.078 | **110.160** | 4.109 | 1670 | 4.950 | 2000 | 1000 |
| 1000 | 1 | 9.399 | **106.394** | 4.221 | 1670 | 5.158 | 2000 | 1000 |
| 1000 | 2 | 9.262 | **107.972** | 4.312 | 1670 | 4.929 | 2000 | 1000 |
| 10000 | 0 | 85.116 | **117.487** | 49.489 | 16670 | 35.475 | 20000 | 10000 |
| 10000 | 1 | 86.275 | **115.909** | 49.676 | 16670 | 36.446 | 20000 | 10000 |
| 10000 | 2 | 84.620 | **118.175** | 49.039 | 16670 | 35.430 | 20000 | 10000 |

## Reproduce

```bash
docker compose up mariadb -d
bin/benchmark.py all --backend mariadb --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mariadb-20260603T060847Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mariadb-20260603T060847Z.jsonl --output docs/benchmarks/mariadb.md
```
