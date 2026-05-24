# dj_queue mariadb Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-24T21:55:12.019271+00:00

## Environment

- backend: `mariadb`
- database: `mysql` `dj_queue_benchmark`
- database version: `10.6.25-MariaDB-ubu2204`
- Python: `3.14.4`
- Django: `6.0.5`
- dj_queue: `0.10.3`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `5dbf01bdd08f`
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

| size | run | duration_seconds | jobs_per_second | **latency_p95_ms** | ready_count | job_count | latency_mean_ms | latency_p50_ms | latency_p99_ms |
|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 7.269 | 137.572 | **11.549** | 1000 | 1000 | 7.266 | 6.562 | 15.053 |
| 1000 | 1 | 8.292 | 120.603 | **12.537** | 1000 | 1000 | 8.288 | 7.716 | 16.503 |
| 1000 | 2 | 8.260 | 121.067 | **13.345** | 1000 | 1000 | 8.257 | 7.509 | 15.636 |
| 10000 | 0 | 77.410 | 129.182 | **11.938** | 10000 | 10000 | 7.738 | 7.070 | 14.463 |
| 10000 | 1 | 74.722 | 133.830 | **11.799** | 10000 | 10000 | 7.469 | 6.744 | 14.141 |
| 10000 | 2 | 74.365 | 134.472 | **11.538** | 10000 | 10000 | 7.433 | 6.749 | 14.000 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.242 | **4127.036** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.230 | **4353.586** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.266 | **3763.512** | 5 | 1000 | 1000 |
| 10000 | 0 | 1.340 | **7463.255** | 5 | 10000 | 10000 |
| 10000 | 1 | 1.316 | **7601.088** | 5 | 10000 | 10000 |
| 10000 | 2 | 1.446 | **6914.349** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.180 | **5560.883** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.209 | **4787.805** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.139 | **7198.839** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.155 | **8660.611** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.183 | **8450.061** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.145 | **8735.552** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.007** | 136563.833 | 0 | 1000 |
| 1000 | 1 | **0.008** | 126023.277 | 0 | 1000 |
| 1000 | 2 | **0.009** | 114865.516 | 0 | 1000 |
| 10000 | 0 | **0.034** | 297685.123 | 0 | 10000 |
| 10000 | 1 | **0.010** | 971215.598 | 0 | 10000 |
| 10000 | 2 | **0.031** | 323749.014 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.404 | **712.034** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.369 | **730.614** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.511 | **661.839** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 12.698 | **787.525** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 12.764 | **783.429** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 12.772 | **782.934** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 154.112 | **45.451** | 11998 | 5000 | 6998 | 1000 | 22.002 | 6.489 |
| 1000 | 1 | 129.562 | **42.912** | 11998 | 5000 | 6998 | 1000 | 23.304 | 7.718 |
| 1000 | 2 | 144.005 | **43.623** | 11998 | 5000 | 6998 | 1000 | 22.924 | 6.944 |
| 10000 | 0 | 139.565 | **48.583** | 119998 | 50000 | 69998 | 10000 | 205.831 | 71.651 |
| 10000 | 1 | 141.441 | **47.705** | 119998 | 50000 | 69998 | 10000 | 209.621 | 70.701 |
| 10000 | 2 | 133.413 | **48.670** | 119998 | 50000 | 69998 | 10000 | 205.467 | 74.955 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_query_count | finished_count |
|---|---|---|---|---|---|
| 1000 | 0 | 10.300 | **97.083** | 2005 | 1000 |
| 1000 | 1 | 9.187 | **108.853** | 2005 | 1000 |
| 1000 | 2 | 10.620 | **94.163** | 2005 | 1000 |
| 10000 | 0 | 105.729 | **94.581** | 20005 | 10000 |
| 10000 | 1 | 106.518 | **93.881** | 20005 | 10000 |
| 10000 | 2 | 105.104 | **95.144** | 20005 | 10000 |

## Reproduce

```bash
docker compose up mariadb -d
bin/benchmark.py all --backend mariadb --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mariadb-20260524T211942Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mariadb-20260524T211942Z.jsonl --output docs/benchmarks/mariadb.md
```
