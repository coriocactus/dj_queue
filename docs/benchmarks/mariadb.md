# dj_queue mariadb Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-25T15:07:18.784735+00:00

## Environment

- backend: `mariadb`
- database: `mysql` `dj_queue_benchmark`
- database version: `10.6.25-MariaDB-ubu2204`
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
| 1000 | 0 | 7.031 | 142.229 | 5 | **13.366** | 1000 | 1000 | 7.027 | 6.346 | 17.697 |
| 1000 | 1 | 2.550 | 392.091 | 5 | **5.209** | 1000 | 1000 | 2.549 | 1.905 | 8.367 |
| 1000 | 2 | 2.012 | 496.966 | 5 | **3.250** | 1000 | 1000 | 2.011 | 1.773 | 4.504 |
| 10000 | 0 | 24.148 | 414.114 | 5 | **3.815** | 10000 | 10000 | 2.414 | 2.101 | 5.839 |
| 10000 | 1 | 23.998 | 416.710 | 5 | **3.747** | 10000 | 10000 | 2.398 | 2.070 | 5.561 |
| 10000 | 2 | 27.512 | 363.482 | 5 | **5.700** | 10000 | 10000 | 2.750 | 2.159 | 10.573 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.193 | **5182.714** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.169 | **5905.518** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.266 | **3757.768** | 5 | 1000 | 1000 |
| 10000 | 0 | 1.308 | **7644.447** | 5 | 10000 | 10000 |
| 10000 | 1 | 1.295 | **7721.364** | 5 | 10000 | 10000 |
| 10000 | 2 | 1.279 | **7817.762** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.127 | **7882.584** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.161 | **6227.464** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.167 | **5988.880** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.237 | **8083.526** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.192 | **8392.781** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.179 | **8481.545** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.011** | 94574.945 | 0 | 1000 |
| 1000 | 1 | **0.013** | 74294.437 | 0 | 1000 |
| 1000 | 2 | **0.014** | 73947.181 | 0 | 1000 |
| 10000 | 0 | **0.025** | 393147.440 | 0 | 10000 |
| 10000 | 1 | **0.011** | 950205.253 | 0 | 10000 |
| 10000 | 2 | **0.028** | 362289.857 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.413 | **707.926** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.357 | **736.869** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.367 | **731.662** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 12.957 | **771.795** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 12.803 | **781.059** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 12.761 | **783.628** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 171.336 | 3001 | **43.848** | 11998 | 5000 | 6998 | 1000 | 22.806 | 5.836 |
| 1000 | 1 | 159.823 | 3001 | **41.813** | 11998 | 5000 | 6998 | 1000 | 23.916 | 6.257 |
| 1000 | 2 | 145.630 | 3001 | **43.470** | 11998 | 5000 | 6998 | 1000 | 23.005 | 6.867 |
| 10000 | 0 | 130.610 | 30001 | **48.694** | 119998 | 50000 | 69998 | 10000 | 205.363 | 76.564 |
| 10000 | 1 | 160.739 | 30001 | **45.456** | 119998 | 50000 | 69998 | 10000 | 219.991 | 62.213 |
| 10000 | 2 | 139.409 | 30001 | **46.806** | 119998 | 50000 | 69998 | 10000 | 213.647 | 71.732 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 133.588 | **73.189** | 1000 | 0 | 0 | 0 | 1000 | 13.663 | 7.486 | 1000 | True | 4 |
| 1000 | 1 | 118.688 | **76.495** | 1000 | 0 | 0 | 0 | 1000 | 13.073 | 8.425 | 1000 | True | 4 |
| 1000 | 2 | 131.369 | **67.380** | 1000 | 0 | 0 | 0 | 1000 | 14.841 | 7.612 | 1000 | True | 4 |
| 10000 | 0 | 152.767 | **69.525** | 10000 | 0 | 0 | 0 | 10000 | 143.834 | 65.459 | 10000 | True | 4 |
| 10000 | 1 | 184.212 | **66.717** | 10000 | 0 | 0 | 0 | 10000 | 149.886 | 54.285 | 10000 | True | 4 |
| 10000 | 2 | 193.988 | **69.382** | 10000 | 0 | 0 | 0 | 10000 | 144.129 | 51.550 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 9.935 | **100.650** | 4.677 | 2005 | 5.235 | 2000 | 1000 |
| 1000 | 1 | 11.150 | **89.689** | 5.232 | 2005 | 5.892 | 2000 | 1000 |
| 1000 | 2 | 11.812 | **84.656** | 5.310 | 2005 | 6.477 | 2000 | 1000 |
| 10000 | 0 | 109.868 | **91.018** | 56.724 | 20005 | 52.913 | 20000 | 10000 |
| 10000 | 1 | 112.558 | **88.843** | 57.731 | 20005 | 54.590 | 20000 | 10000 |
| 10000 | 2 | 120.663 | **82.875** | 60.315 | 20005 | 60.100 | 20000 | 10000 |

## Reproduce

```bash
docker compose up mariadb -d
bin/benchmark.py all --backend mariadb --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mariadb-20260525T141948Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mariadb-20260525T141948Z.jsonl --output docs/benchmarks/mariadb.md
```
