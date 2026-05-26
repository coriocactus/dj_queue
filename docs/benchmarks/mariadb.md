# dj_queue mariadb Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-26T19:54:44.831793+00:00

## Environment

- backend: `mariadb`
- database: `mysql` `dj_queue_benchmark`
- database version: `10.6.25-MariaDB-ubu2204`
- Python: `3.14.5`
- Django: `6.0.5`
- dj_queue: `0.10.5`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `e3d51861cac1`
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
| 1000 | 0 | 8.452 | 118.320 | 5 | **11.240** | 1000 | 1000 | 8.447 | 9.654 | 15.100 |
| 1000 | 1 | 8.116 | 123.211 | 5 | **11.262** | 1000 | 1000 | 8.112 | 7.523 | 14.654 |
| 1000 | 2 | 7.357 | 135.917 | 5 | **11.139** | 1000 | 1000 | 7.354 | 6.876 | 15.598 |
| 10000 | 0 | 66.255 | 150.932 | 5 | **11.235** | 10000 | 10000 | 6.622 | 6.709 | 12.988 |
| 10000 | 1 | 64.323 | 155.465 | 5 | **10.871** | 10000 | 10000 | 6.429 | 6.493 | 13.043 |
| 10000 | 2 | 79.693 | 125.482 | 5 | **11.135** | 10000 | 10000 | 7.965 | 7.530 | 13.290 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.193 | **5179.099** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.247 | **4043.787** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.236 | **4229.599** | 5 | 1000 | 1000 |
| 10000 | 0 | 1.177 | **8493.062** | 5 | 10000 | 10000 |
| 10000 | 1 | 1.138 | **8785.128** | 5 | 10000 | 10000 |
| 10000 | 2 | 1.206 | **8292.264** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.116 | **8606.756** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.126 | **7938.608** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.128 | **7797.790** | 1000 | 1000 | 1000 |
| 10000 | 0 | 0.924 | **10825.776** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.010 | **9904.697** | 10000 | 10000 | 10000 |
| 10000 | 2 | 0.974 | **10262.025** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.007** | 148933.280 | 0 | 1000 |
| 1000 | 1 | **0.007** | 138774.053 | 0 | 1000 |
| 1000 | 2 | **0.013** | 77459.334 | 0 | 1000 |
| 10000 | 0 | **0.032** | 307777.936 | 0 | 10000 |
| 10000 | 1 | **0.031** | 324049.761 | 0 | 10000 |
| 10000 | 2 | **0.035** | 283610.504 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.310 | **763.251** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.312 | **762.164** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.368 | **731.123** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 13.032 | **767.330** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 13.061 | **765.659** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 13.550 | **737.991** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 166.686 | 3001 | **37.533** | 11998 | 5000 | 6998 | 1000 | 26.643 | 5.999 |
| 1000 | 1 | 170.834 | 3001 | **36.250** | 11998 | 5000 | 6998 | 1000 | 27.586 | 5.854 |
| 1000 | 2 | 176.563 | 3001 | **33.869** | 11998 | 5000 | 6998 | 1000 | 29.525 | 5.664 |
| 10000 | 0 | 169.086 | 30001 | **41.568** | 119998 | 50000 | 69998 | 10000 | 240.569 | 59.141 |
| 10000 | 1 | 130.525 | 30001 | **31.956** | 119998 | 50000 | 69998 | 10000 | 312.929 | 76.613 |
| 10000 | 2 | 125.259 | 30001 | **31.312** | 119998 | 50000 | 69998 | 10000 | 319.366 | 79.835 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 113.593 | **91.462** | 1000 | 0 | 0 | 0 | 1000 | 10.934 | 8.803 | 1000 | True | 4 |
| 1000 | 1 | 123.770 | **112.969** | 1000 | 0 | 0 | 0 | 1000 | 8.852 | 8.079 | 1000 | True | 4 |
| 1000 | 2 | 238.761 | **113.501** | 1000 | 0 | 0 | 0 | 1000 | 8.810 | 4.188 | 1000 | True | 4 |
| 10000 | 0 | 138.749 | **68.348** | 10000 | 0 | 0 | 0 | 10000 | 146.311 | 72.073 | 10000 | True | 4 |
| 10000 | 1 | 124.777 | **66.194** | 10000 | 0 | 0 | 0 | 10000 | 151.070 | 80.143 | 10000 | True | 4 |
| 10000 | 2 | 134.808 | **67.675** | 10000 | 0 | 0 | 0 | 10000 | 147.765 | 74.180 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 12.094 | **82.686** | 6.006 | 2005 | 6.062 | 2000 | 1000 |
| 1000 | 1 | 13.460 | **74.296** | 6.700 | 2005 | 6.730 | 2000 | 1000 |
| 1000 | 2 | 13.239 | **75.534** | 6.568 | 2005 | 6.643 | 2000 | 1000 |
| 10000 | 0 | 126.935 | **78.780** | 68.739 | 20005 | 57.941 | 20000 | 10000 |
| 10000 | 1 | 125.541 | **79.656** | 68.023 | 20005 | 57.263 | 20000 | 10000 |
| 10000 | 2 | 125.222 | **79.858** | 67.813 | 20005 | 57.157 | 20000 | 10000 |

## Reproduce

```bash
docker compose up mariadb -d
bin/benchmark.py all --backend mariadb --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mariadb-20260526T185629Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mariadb-20260526T185629Z.jsonl --output docs/benchmarks/mariadb.md
```
