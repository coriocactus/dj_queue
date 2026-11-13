# dj_queue mariadb Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-11-13T14:47:40.690272+00:00

## Environment

- backend: `mariadb`
- database: `mysql` `dj_queue_benchmark`
- database version: `12.3.3-MariaDB-ubu2404`
- Python: `3.14.10`
- Django: `6.1.2`
- psycopg implementation: `None`
- dj_queue: `0.15.0`
- platform: `macOS-27.2.1-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `0e72b86317b1`
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
| 1000 | 0 | 1.214 | 823.523 | 4 | **1.859** | 1000 | 1000 | 1.214 | 1.032 | 2.181 |
| 1000 | 1 | 1.254 | 797.508 | 4 | **2.201** | 1000 | 1000 | 1.253 | 1.138 | 2.508 |
| 1000 | 2 | 0.958 | 1043.393 | 4 | **1.130** | 1000 | 1000 | 0.958 | 0.900 | 1.754 |
| 10000 | 0 | 21.799 | 458.740 | 4 | **4.209** | 10000 | 10000 | 2.179 | 1.674 | 6.270 |
| 10000 | 1 | 23.484 | 425.819 | 4 | **2.870** | 10000 | 10000 | 2.347 | 2.397 | 3.560 |
| 10000 | 2 | 16.426 | 608.808 | 4 | **2.680** | 10000 | 10000 | 1.642 | 1.453 | 3.731 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.099 | **10083.602** | 4 | 1000 | 1000 |
| 1000 | 1 | 0.085 | **11709.362** | 4 | 1000 | 1000 |
| 1000 | 2 | 0.087 | **11511.962** | 4 | 1000 | 1000 |
| 10000 | 0 | 0.866 | **11541.660** | 4 | 10000 | 10000 |
| 10000 | 1 | 0.949 | **10535.280** | 4 | 10000 | 10000 |
| 10000 | 2 | 0.802 | **12470.023** | 4 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.068 | **14780.691** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.070 | **14206.025** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.090 | **11160.424** | 1000 | 1000 | 1000 |
| 10000 | 0 | 0.730 | **13696.248** | 10000 | 10000 | 10000 |
| 10000 | 1 | 0.727 | **13757.779** | 10000 | 10000 | 10000 |
| 10000 | 2 | 0.712 | **14042.204** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | query_count | fired_count | recurring_task_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | **0.001** | 757241.307 | 1 | 0 | 1000 |
| 1000 | 1 | **0.001** | 780690.708 | 1 | 0 | 1000 |
| 1000 | 2 | **0.001** | 812182.748 | 1 | 0 | 1000 |
| 10000 | 0 | **0.012** | 832264.082 | 1 | 0 | 10000 |
| 10000 | 1 | **0.011** | 873248.046 | 1 | 0 | 10000 |
| 10000 | 2 | **0.010** | 987768.169 | 1 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 0.506 | **1975.910** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 0.503 | **1989.815** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 0.498 | **2006.728** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 4.530 | **2207.285** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 4.821 | **2074.453** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 4.479 | **2232.491** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 268.639 | 3000 | **57.688** | 11999 | 6000 | 5999 | 1000 | 17.335 | 3.722 |
| 1000 | 1 | 268.049 | 3000 | **56.162** | 11999 | 6000 | 5999 | 1000 | 17.806 | 3.731 |
| 1000 | 2 | 408.086 | 3000 | **54.846** | 11999 | 6000 | 5999 | 1000 | 18.233 | 2.450 |
| 10000 | 0 | 295.757 | 30000 | **54.749** | 119999 | 60000 | 59999 | 10000 | 182.652 | 33.811 |
| 10000 | 1 | 419.828 | 30000 | **50.591** | 119999 | 60000 | 59999 | 10000 | 197.663 | 23.819 |
| 10000 | 2 | 268.655 | 30000 | **50.465** | 119999 | 60000 | 59999 | 10000 | 198.157 | 37.222 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 403.687 | **88.481** | 1000 | 0 | 0 | 0 | 1000 | 11.302 | 2.477 | 1000 | True | 4 |
| 1000 | 1 | 411.941 | **87.455** | 1000 | 0 | 0 | 0 | 1000 | 11.435 | 2.428 | 1000 | True | 4 |
| 1000 | 2 | 383.832 | **84.578** | 1000 | 0 | 0 | 0 | 1000 | 11.823 | 2.605 | 1000 | True | 4 |
| 10000 | 0 | 394.730 | **99.863** | 10000 | 0 | 0 | 0 | 10000 | 100.137 | 25.334 | 10000 | True | 4 |
| 10000 | 1 | 428.787 | **108.793** | 10000 | 0 | 0 | 0 | 10000 | 91.918 | 23.322 | 10000 | True | 4 |
| 10000 | 2 | 464.982 | **131.282** | 10000 | 0 | 0 | 0 | 10000 | 76.172 | 21.506 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 3.312 | **301.976** | 1.750 | 2005 | 1.554 | 2000 | 1000 |
| 1000 | 1 | 3.937 | **254.019** | 2.074 | 2005 | 1.854 | 2000 | 1000 |
| 1000 | 2 | 2.388 | **418.705** | 1.206 | 2005 | 1.178 | 2000 | 1000 |
| 10000 | 0 | 27.588 | **362.479** | 13.217 | 20005 | 14.322 | 20000 | 10000 |
| 10000 | 1 | 24.576 | **406.907** | 12.160 | 20005 | 12.368 | 20000 | 10000 |
| 10000 | 2 | 25.400 | **393.708** | 11.907 | 20005 | 13.444 | 20000 | 10000 |

## Reproduce

```bash
docker compose up mariadb -d
bin/benchmark.py all --backend mariadb --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mariadb-20261113T141829Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mariadb-20261113T141829Z.jsonl --output docs/benchmarks/mariadb.md
```
