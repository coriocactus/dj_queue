# dj_queue mariadb Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-11-15T10:33:25.436882+00:00

## Environment

- backend: `mariadb`
- database: `mysql` `dj_queue_benchmark`
- database version: `12.3.3-MariaDB-ubu2404`
- Python: `3.14.10`
- Django: `6.1.3`
- psycopg implementation: `None`
- dj_queue: `0.18.0`
- platform: `macOS-27.2.1-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `4c5277727759`
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
| 1000 | 0 | 2.466 | 405.566 | 4 | **3.013** | 1000 | 1000 | 2.464 | 2.473 | 3.454 |
| 1000 | 1 | 2.386 | 419.178 | 4 | **2.958** | 1000 | 1000 | 2.384 | 2.392 | 3.564 |
| 1000 | 2 | 2.253 | 443.824 | 4 | **2.655** | 1000 | 1000 | 2.251 | 2.277 | 3.297 |
| 10000 | 0 | 25.229 | 396.367 | 4 | **3.063** | 10000 | 10000 | 2.521 | 2.533 | 3.413 |
| 10000 | 1 | 67.219 | 148.767 | 4 | **6.969** | 10000 | 10000 | 6.720 | 3.725 | 20.919 |
| 10000 | 2 | 26.824 | 372.798 | 4 | **4.082** | 10000 | 10000 | 2.681 | 2.506 | 5.100 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.156 | **6391.207** | 4 | 1000 | 1000 |
| 1000 | 1 | 0.158 | **6347.843** | 4 | 1000 | 1000 |
| 1000 | 2 | 0.145 | **6893.357** | 4 | 1000 | 1000 |
| 10000 | 0 | 0.904 | **11057.201** | 4 | 10000 | 10000 |
| 10000 | 1 | 0.982 | **10184.725** | 4 | 10000 | 10000 |
| 10000 | 2 | 0.989 | **10108.886** | 4 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.067 | **14910.805** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.085 | **11822.433** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.062 | **16043.532** | 1000 | 1000 | 1000 |
| 10000 | 0 | 0.709 | **14109.479** | 10000 | 10000 | 10000 |
| 10000 | 1 | 0.717 | **13939.582** | 10000 | 10000 | 10000 |
| 10000 | 2 | 0.750 | **13326.511** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | query_count | fired_count | recurring_task_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | **0.002** | 561587.318 | 1 | 0 | 1000 |
| 1000 | 1 | **0.003** | 382995.021 | 1 | 0 | 1000 |
| 1000 | 2 | **0.002** | 417834.692 | 1 | 0 | 1000 |
| 10000 | 0 | **0.010** | 994872.230 | 1 | 0 | 10000 |
| 10000 | 1 | **0.010** | 1021285.219 | 1 | 0 | 10000 |
| 10000 | 2 | **0.011** | 903645.131 | 1 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 0.486 | **2057.188** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 0.506 | **1975.837** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 0.507 | **1971.169** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 4.618 | **2165.601** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 4.590 | **2178.543** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 4.613 | **2167.584** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 344.108 | 3000 | **50.302** | 11999 | 6000 | 5999 | 1000 | 19.880 | 2.906 |
| 1000 | 1 | 222.309 | 3000 | **47.852** | 11999 | 6000 | 5999 | 1000 | 20.898 | 4.498 |
| 1000 | 2 | 259.130 | 3000 | **48.045** | 11999 | 6000 | 5999 | 1000 | 20.814 | 3.859 |
| 10000 | 0 | 258.508 | 30000 | **47.659** | 119999 | 60000 | 59999 | 10000 | 209.825 | 38.683 |
| 10000 | 1 | 249.136 | 30000 | **73.545** | 119999 | 60000 | 59999 | 10000 | 135.972 | 40.139 |
| 10000 | 2 | 518.957 | 30000 | **99.615** | 119999 | 60000 | 59999 | 10000 | 100.386 | 19.269 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 396.236 | **85.759** | 1000 | 0 | 0 | 0 | 1000 | 11.661 | 2.524 | 1000 | True | 4 |
| 1000 | 1 | 353.369 | **141.207** | 1000 | 0 | 0 | 0 | 1000 | 7.082 | 2.830 | 1000 | True | 4 |
| 1000 | 2 | 539.852 | **112.582** | 1000 | 0 | 0 | 0 | 1000 | 8.882 | 1.852 | 1000 | True | 4 |
| 10000 | 0 | 320.019 | **81.410** | 10000 | 0 | 0 | 0 | 10000 | 122.835 | 31.248 | 10000 | True | 4 |
| 10000 | 1 | 335.346 | **76.880** | 10000 | 0 | 0 | 0 | 10000 | 130.072 | 29.820 | 10000 | True | 4 |
| 10000 | 2 | 380.491 | **73.350** | 10000 | 0 | 0 | 0 | 10000 | 136.333 | 26.282 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 3.834 | **260.830** | 2.078 | 2005 | 1.749 | 2000 | 1000 |
| 1000 | 1 | 5.686 | **175.875** | 2.883 | 2005 | 2.792 | 2000 | 1000 |
| 1000 | 2 | 4.969 | **201.237** | 2.592 | 2005 | 2.368 | 2000 | 1000 |
| 10000 | 0 | 66.501 | **150.373** | 33.677 | 20005 | 32.701 | 20000 | 10000 |
| 10000 | 1 | 68.281 | **146.454** | 34.659 | 20005 | 33.494 | 20000 | 10000 |
| 10000 | 2 | 68.032 | **146.990** | 33.620 | 20005 | 34.290 | 20000 | 10000 |

## Reproduce

```bash
docker compose up mariadb -d
bin/benchmark.py all --backend mariadb --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mariadb-20261115T100031Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mariadb-20261115T100031Z.jsonl --output docs/benchmarks/mariadb.md
```
