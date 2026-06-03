# dj_queue mysql Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-06-03T15:35:01.140093+00:00

## Environment

- backend: `mysql`
- database: `mysql` `dj_queue_benchmark`
- database version: `8.4.8`
- Python: `3.14.5`
- Django: `6.0.5`
- dj_queue: `0.12.0`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `411646c33337`
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
| 1000 | 0 | 3.778 | 264.660 | 5 | **4.951** | 1000 | 1000 | 3.777 | 3.662 | 6.461 |
| 1000 | 1 | 3.708 | 269.718 | 5 | **4.820** | 1000 | 1000 | 3.706 | 3.604 | 6.082 |
| 1000 | 2 | 3.945 | 253.472 | 5 | **5.224** | 1000 | 1000 | 3.943 | 3.760 | 6.815 |
| 10000 | 0 | 46.836 | 213.509 | 5 | **9.081** | 10000 | 10000 | 4.682 | 3.746 | 19.032 |
| 10000 | 1 | 42.049 | 237.817 | 5 | **7.569** | 10000 | 10000 | 4.203 | 3.627 | 13.902 |
| 10000 | 2 | 40.572 | 246.478 | 5 | **6.464** | 10000 | 10000 | 4.056 | 3.591 | 13.411 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.178 | **5604.741** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.194 | **5163.378** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.208 | **4818.623** | 5 | 1000 | 1000 |
| 10000 | 0 | 1.379 | **7250.862** | 5 | 10000 | 10000 |
| 10000 | 1 | 1.283 | **7795.284** | 5 | 10000 | 10000 |
| 10000 | 2 | 1.292 | **7742.913** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.168 | **5968.878** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.113 | **8831.842** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.143 | **6977.338** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.130 | **8848.919** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.094 | **9141.256** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.078 | **9275.557** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.011** | 91987.156 | 0 | 1000 |
| 1000 | 1 | **0.007** | 140058.474 | 0 | 1000 |
| 1000 | 2 | **0.006** | 158172.587 | 0 | 1000 |
| 10000 | 0 | **0.023** | 438609.322 | 0 | 10000 |
| 10000 | 1 | **0.026** | 384833.097 | 0 | 10000 |
| 10000 | 2 | **0.025** | 396286.149 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.260 | **793.407** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.201 | **832.481** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.343 | **744.555** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 9.675 | **1033.623** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 9.265 | **1079.349** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 9.289 | **1076.580** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 230.918 | 3001 | **74.880** | 11998 | 5000 | 6998 | 1000 | 13.355 | 4.331 |
| 1000 | 1 | 238.132 | 3001 | **80.590** | 11998 | 5000 | 6998 | 1000 | 12.408 | 4.199 |
| 1000 | 2 | 251.382 | 3001 | **76.268** | 11998 | 5000 | 6998 | 1000 | 13.112 | 3.978 |
| 10000 | 0 | 233.546 | 30001 | **83.646** | 119998 | 50000 | 69998 | 10000 | 119.551 | 42.818 |
| 10000 | 1 | 226.162 | 30001 | **85.289** | 119998 | 50000 | 69998 | 10000 | 117.248 | 44.216 |
| 10000 | 2 | 232.200 | 30001 | **80.106** | 119998 | 50000 | 69998 | 10000 | 124.834 | 43.066 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 256.810 | **127.894** | 1000 | 0 | 0 | 0 | 1000 | 7.819 | 3.894 | 1000 | True | 4 |
| 1000 | 1 | 159.187 | **118.348** | 1000 | 0 | 0 | 0 | 1000 | 8.450 | 6.282 | 1000 | True | 4 |
| 1000 | 2 | 243.068 | **130.201** | 1000 | 0 | 0 | 0 | 1000 | 7.680 | 4.114 | 1000 | True | 4 |
| 10000 | 0 | 210.327 | **108.846** | 10000 | 0 | 0 | 0 | 10000 | 91.873 | 47.545 | 10000 | True | 4 |
| 10000 | 1 | 240.374 | **120.190** | 10000 | 0 | 0 | 0 | 10000 | 83.202 | 41.602 | 10000 | True | 4 |
| 10000 | 2 | 233.908 | **112.642** | 10000 | 0 | 0 | 0 | 10000 | 88.777 | 42.752 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 5.926 | **168.751** | 2.571 | 2005 | 3.344 | 2000 | 1000 |
| 1000 | 1 | 5.440 | **183.830** | 2.400 | 2005 | 3.029 | 2000 | 1000 |
| 1000 | 2 | 5.218 | **191.648** | 2.292 | 2005 | 2.916 | 2000 | 1000 |
| 10000 | 0 | 60.928 | **164.127** | 26.304 | 20005 | 34.514 | 20000 | 10000 |
| 10000 | 1 | 55.725 | **179.453** | 24.687 | 20005 | 30.935 | 20000 | 10000 |
| 10000 | 2 | 59.666 | **167.601** | 26.451 | 20005 | 33.106 | 20000 | 10000 |

## Reproduce

```bash
docker compose up mysql -d
bin/benchmark.py all --backend mysql --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mysql-20260603T150421Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mysql-20260603T150421Z.jsonl --output docs/benchmarks/mysql.md
```
