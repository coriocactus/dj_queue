# dj_queue mysql Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-26T20:40:24.034742+00:00

## Environment

- backend: `mysql`
- database: `mysql` `dj_queue_benchmark`
- database version: `8.4.8`
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
| 1000 | 0 | 4.789 | 208.828 | 5 | **7.119** | 1000 | 1000 | 4.787 | 4.649 | 9.000 |
| 1000 | 1 | 4.875 | 205.143 | 5 | **6.623** | 1000 | 1000 | 4.873 | 4.799 | 8.408 |
| 1000 | 2 | 4.828 | 207.146 | 5 | **6.590** | 1000 | 1000 | 4.826 | 4.719 | 9.252 |
| 10000 | 0 | 53.596 | 186.581 | 5 | **9.128** | 10000 | 10000 | 5.358 | 4.876 | 14.361 |
| 10000 | 1 | 53.871 | 185.627 | 5 | **8.505** | 10000 | 10000 | 5.385 | 4.946 | 14.981 |
| 10000 | 2 | 53.418 | 187.204 | 5 | **9.273** | 10000 | 10000 | 5.340 | 4.816 | 13.562 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.420 | **2378.735** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.419 | **2384.939** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.551 | **1813.737** | 5 | 1000 | 1000 |
| 10000 | 0 | 1.871 | **5345.494** | 5 | 10000 | 10000 |
| 10000 | 1 | 1.703 | **5873.485** | 5 | 10000 | 10000 |
| 10000 | 2 | 1.578 | **6337.949** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.255 | **3921.362** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.248 | **4038.216** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.218 | **4590.767** | 1000 | 1000 | 1000 |
| 10000 | 0 | 0.999 | **10006.059** | 10000 | 10000 | 10000 |
| 10000 | 1 | 0.996 | **10041.176** | 10000 | 10000 | 10000 |
| 10000 | 2 | 0.987 | **10128.781** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.006** | 156173.743 | 0 | 1000 |
| 1000 | 1 | **0.007** | 144846.191 | 0 | 1000 |
| 1000 | 2 | **0.006** | 171702.058 | 0 | 1000 |
| 10000 | 0 | **0.024** | 424319.895 | 0 | 10000 |
| 10000 | 1 | **0.025** | 400179.424 | 0 | 10000 |
| 10000 | 2 | **0.022** | 456320.604 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.471 | **679.688** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.464 | **683.210** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.465 | **682.550** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 14.102 | **709.113** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 14.705 | **680.049** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 14.325 | **698.084** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 217.330 | 3001 | **45.897** | 11998 | 5000 | 6998 | 1000 | 21.788 | 4.601 |
| 1000 | 1 | 153.122 | 3001 | **38.985** | 11998 | 5000 | 6998 | 1000 | 25.651 | 6.531 |
| 1000 | 2 | 219.199 | 3001 | **38.940** | 11998 | 5000 | 6998 | 1000 | 25.680 | 4.562 |
| 10000 | 0 | 198.603 | 30001 | **48.574** | 119998 | 50000 | 69998 | 10000 | 205.874 | 50.352 |
| 10000 | 1 | 198.049 | 30001 | **46.402** | 119998 | 50000 | 69998 | 10000 | 215.509 | 50.493 |
| 10000 | 2 | 187.530 | 30001 | **47.818** | 119998 | 50000 | 69998 | 10000 | 209.127 | 53.325 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 219.292 | **81.264** | 1000 | 0 | 0 | 0 | 1000 | 12.306 | 4.560 | 1000 | True | 4 |
| 1000 | 1 | 178.921 | **75.888** | 1000 | 0 | 0 | 0 | 1000 | 13.177 | 5.589 | 1000 | True | 4 |
| 1000 | 2 | 218.603 | **81.357** | 1000 | 0 | 0 | 0 | 1000 | 12.291 | 4.575 | 1000 | True | 4 |
| 10000 | 0 | 193.017 | **74.568** | 10000 | 0 | 0 | 0 | 10000 | 134.106 | 51.809 | 10000 | True | 4 |
| 10000 | 1 | 199.598 | **74.619** | 10000 | 0 | 0 | 0 | 10000 | 134.014 | 50.101 | 10000 | True | 4 |
| 10000 | 2 | 189.633 | **74.887** | 10000 | 0 | 0 | 0 | 10000 | 133.534 | 52.733 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 8.504 | **117.589** | 4.125 | 2005 | 4.366 | 2000 | 1000 |
| 1000 | 1 | 7.637 | **130.933** | 3.739 | 2005 | 3.886 | 2000 | 1000 |
| 1000 | 2 | 9.086 | **110.065** | 4.008 | 2005 | 5.064 | 2000 | 1000 |
| 10000 | 0 | 85.506 | **116.950** | 43.529 | 20005 | 41.852 | 20000 | 10000 |
| 10000 | 1 | 89.240 | **112.058** | 43.905 | 20005 | 45.206 | 20000 | 10000 |
| 10000 | 2 | 88.788 | **112.628** | 44.941 | 20005 | 43.715 | 20000 | 10000 |

## Reproduce

```bash
docker compose up mysql -d
bin/benchmark.py all --backend mysql --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mysql-20260526T195444Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mysql-20260526T195444Z.jsonl --output docs/benchmarks/mysql.md
```
