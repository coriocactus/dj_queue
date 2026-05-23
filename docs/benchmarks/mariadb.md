# dj_queue mariadb Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-23T11:43:11.688960+00:00

## Environment

- backend: `mariadb`
- database: `mysql` `dj_queue_benchmark`
- database version: `10.6.25-MariaDB-ubu2204`
- Python: `3.14.4`
- Django: `6.0.5`
- dj_queue: `0.9.2`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `b0af38279ead`
- benchmark worker count: `4`
- benchmark worker threads: `8`
- preserve finished jobs: `True`
- database CONN_MAX_AGE: `60`

## Results

### `single-enqueue`: one-by-one immediate enqueue latency and throughput

- key metric: **`latency_p95_ms`** - enqueue tail latency for individual task submissions; lower is better
- good number: `<= 20 ms` for request-path enqueue on the 10k local benchmark
- use case: web requests, admin actions, and small fan-out paths that submit tasks one at a time
- mechanics: calls the public `Task.enqueue()` path once per job, including validation, job insert, ready-row insert, result mapping, and ready wakeup registration

| size | run | duration_seconds | jobs_per_second | **latency_p95_ms** | ready_count | job_count | latency_mean_ms | latency_p50_ms | latency_p99_ms |
|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 4.545 | 219.999 | **9.535** | 1000 | 1000 | 4.544 | 3.530 | 12.417 |
| 1000 | 1 | 2.330 | 429.230 | **3.672** | 1000 | 1000 | 2.329 | 2.089 | 4.748 |
| 1000 | 2 | 2.493 | 401.097 | **3.840** | 1000 | 1000 | 2.492 | 2.213 | 4.282 |
| 10000 | 0 | 24.707 | 404.743 | **3.843** | 10000 | 10000 | 2.470 | 2.237 | 4.482 |
| 10000 | 1 | 85.818 | 116.525 | **14.650** | 10000 | 10000 | 8.578 | 8.412 | 17.295 |
| 10000 | 2 | 77.340 | 129.299 | **13.337** | 10000 | 10000 | 7.731 | 7.887 | 16.228 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- good number: `>= 5,000 jobs/sec` for 10k independent immediate jobs
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.389 | **2571.337** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.275 | **3642.258** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.292 | **3424.572** | 5 | 1000 | 1000 |
| 10000 | 0 | 1.544 | **6478.515** | 5 | 10000 | 10000 |
| 10000 | 1 | 1.528 | **6543.691** | 5 | 10000 | 10000 |
| 10000 | 2 | 1.666 | **6001.275** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- good number: `>= 5,000 rows/sec` for a 10k due-row promotion burst
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.158 | **6348.372** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.165 | **6078.365** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.159 | **6294.597** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.137 | **8794.538** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.157 | **8642.662** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.134 | **8816.523** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- good number: `<= 0.050 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.011** | 87032.520 | 0 | 1000 |
| 1000 | 1 | **0.011** | 86979.212 | 0 | 1000 |
| 1000 | 2 | **0.011** | 91375.663 | 0 | 1000 |
| 10000 | 0 | **0.024** | 411093.354 | 0 | 10000 |
| 10000 | 1 | **0.023** | 432460.484 | 0 | 10000 |
| 10000 | 2 | **0.028** | 351542.756 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- good number: `>= 250 jobs/sec` for draining 10k no-op ready jobs
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 2.188 | **457.012** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 2.193 | **456.039** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 2.212 | **452.066** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 19.028 | **525.530** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 19.285 | **518.542** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 18.647 | **536.275** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- good number: `>= 25 jobs/sec` for a 10k serialized hot-key drain
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 112.547 | **26.102** | 15995 | 4000 | 11995 | 1000 | 38.311 | 8.885 |
| 1000 | 1 | 93.833 | **28.242** | 15995 | 4000 | 11995 | 1000 | 35.408 | 10.657 |
| 1000 | 2 | 105.415 | **29.830** | 15995 | 4000 | 11995 | 1000 | 33.524 | 9.486 |
| 10000 | 0 | 105.350 | **28.566** | 159995 | 40000 | 119995 | 10000 | 350.069 | 94.922 |
| 10000 | 1 | 93.631 | **32.597** | 159995 | 40000 | 119995 | 10000 | 306.776 | 106.802 |
| 10000 | 2 | 112.542 | **36.890** | 159995 | 40000 | 119995 | 10000 | 271.078 | 88.856 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- good number: `>= 50 jobs/sec` for a 10k exact-selector drain
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_query_count | finished_count |
|---|---|---|---|---|---|
| 1000 | 0 | 16.272 | **61.456** | 1336 | 1000 |
| 1000 | 1 | 16.273 | **61.450** | 1336 | 1000 |
| 1000 | 2 | 16.343 | **61.189** | 1336 | 1000 |
| 10000 | 0 | 120.413 | **83.047** | 13336 | 10000 |
| 10000 | 1 | 126.075 | **79.318** | 13336 | 10000 |
| 10000 | 2 | 118.435 | **84.434** | 13336 | 10000 |

## Reproduce

```bash
docker compose up mariadb -d
bin/benchmark.py all --backend mariadb --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mariadb-20260523T075830Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mariadb-20260523T075830Z.jsonl --output docs/benchmarks/mariadb.md
```
