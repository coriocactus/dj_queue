# dj_queue mysql Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-25T15:47:50.678455+00:00

## Environment

- backend: `mysql`
- database: `mysql` `dj_queue_benchmark`
- database version: `8.4.8`
- Python: `3.14.4`
- Django: `6.0.5`
- dj_queue: `0.10.3`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `1a65bc8ef066`
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
| 1000 | 0 | 6.806 | 146.920 | 5 | **12.686** | 1000 | 1000 | 6.804 | 5.704 | 17.058 |
| 1000 | 1 | 4.827 | 207.168 | 5 | **6.377** | 1000 | 1000 | 4.825 | 4.615 | 8.443 |
| 1000 | 2 | 5.289 | 189.083 | 5 | **7.263** | 1000 | 1000 | 5.287 | 5.022 | 10.674 |
| 10000 | 0 | 56.592 | 176.705 | 5 | **10.985** | 10000 | 10000 | 5.657 | 4.746 | 17.160 |
| 10000 | 1 | 55.130 | 181.388 | 5 | **9.956** | 10000 | 10000 | 5.511 | 4.766 | 15.438 |
| 10000 | 2 | 55.305 | 180.815 | 5 | **10.161** | 10000 | 10000 | 5.528 | 4.685 | 15.655 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.381 | **2624.311** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.575 | **1740.210** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.517 | **1935.977** | 5 | 1000 | 1000 |
| 10000 | 0 | 1.644 | **6083.816** | 5 | 10000 | 10000 |
| 10000 | 1 | 1.737 | **5756.557** | 5 | 10000 | 10000 |
| 10000 | 2 | 1.441 | **6941.792** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.213 | **4701.848** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.263 | **3797.000** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.277 | **3607.418** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.261 | **7928.796** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.299 | **7699.604** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.286 | **7773.286** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.011** | 89594.696 | 0 | 1000 |
| 1000 | 1 | **0.011** | 88343.125 | 0 | 1000 |
| 1000 | 2 | **0.020** | 51190.937 | 0 | 1000 |
| 10000 | 0 | **0.019** | 538699.963 | 0 | 10000 |
| 10000 | 1 | **0.018** | 541853.671 | 0 | 10000 |
| 10000 | 2 | **0.018** | 554020.457 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.485 | **673.410** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.523 | **656.669** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.437 | **696.039** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 14.431 | **692.959** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 13.876 | **720.683** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 15.766 | **634.265** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 127.341 | 3001 | **66.925** | 11998 | 5000 | 6998 | 1000 | 14.942 | 7.853 |
| 1000 | 1 | 164.961 | 3001 | **59.771** | 11998 | 5000 | 6998 | 1000 | 16.731 | 6.062 |
| 1000 | 2 | 208.450 | 3001 | **65.464** | 11998 | 5000 | 6998 | 1000 | 15.276 | 4.797 |
| 10000 | 0 | 190.519 | 30001 | **75.910** | 119998 | 50000 | 69998 | 10000 | 131.735 | 52.488 |
| 10000 | 1 | 160.923 | 30001 | **66.950** | 119998 | 50000 | 69998 | 10000 | 149.365 | 62.141 |
| 10000 | 2 | 198.306 | 30001 | **68.941** | 119998 | 50000 | 69998 | 10000 | 145.051 | 50.427 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 235.745 | **71.245** | 1000 | 0 | 0 | 0 | 1000 | 14.036 | 4.242 | 1000 | True | 4 |
| 1000 | 1 | 225.237 | **82.420** | 1000 | 0 | 0 | 0 | 1000 | 12.133 | 4.440 | 1000 | True | 4 |
| 1000 | 2 | 203.123 | **74.329** | 1000 | 0 | 0 | 0 | 1000 | 13.454 | 4.923 | 1000 | True | 4 |
| 10000 | 0 | 179.719 | **74.973** | 10000 | 0 | 0 | 0 | 10000 | 133.381 | 55.642 | 10000 | True | 4 |
| 10000 | 1 | 172.014 | **72.143** | 10000 | 0 | 0 | 0 | 10000 | 138.613 | 58.135 | 10000 | True | 4 |
| 10000 | 2 | 210.014 | **73.337** | 10000 | 0 | 0 | 0 | 10000 | 136.358 | 47.616 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 5.832 | **171.472** | 2.682 | 2005 | 3.140 | 2000 | 1000 |
| 1000 | 1 | 5.935 | **168.503** | 2.717 | 2005 | 3.208 | 2000 | 1000 |
| 1000 | 2 | 7.139 | **140.076** | 3.161 | 2005 | 3.967 | 2000 | 1000 |
| 10000 | 0 | 84.402 | **118.481** | 41.293 | 20005 | 42.980 | 20000 | 10000 |
| 10000 | 1 | 72.627 | **137.690** | 36.621 | 20005 | 35.893 | 20000 | 10000 |
| 10000 | 2 | 80.480 | **124.254** | 39.996 | 20005 | 40.359 | 20000 | 10000 |

## Reproduce

```bash
docker compose up mysql -d
bin/benchmark.py all --backend mysql --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mysql-20260525T150718Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mysql-20260525T150718Z.jsonl --output docs/benchmarks/mysql.md
```
