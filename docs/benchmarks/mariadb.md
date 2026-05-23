# dj_queue mariadb Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-23T18:46:01.527363+00:00

## Environment

- backend: `mariadb`
- database: `mysql` `dj_queue_benchmark`
- database version: `10.6.25-MariaDB-ubu2204`
- Python: `3.14.4`
- Django: `6.0.5`
- dj_queue: `0.10.2`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `c939c210c215`
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
| 1000 | 0 | 8.195 | 122.023 | **12.688** | 1000 | 1000 | 8.191 | 7.498 | 16.637 |
| 1000 | 1 | 7.717 | 129.589 | **12.558** | 1000 | 1000 | 7.713 | 7.092 | 16.083 |
| 1000 | 2 | 7.286 | 137.250 | **12.331** | 1000 | 1000 | 7.282 | 6.819 | 17.247 |
| 10000 | 0 | 30.915 | 323.464 | **7.353** | 10000 | 10000 | 3.090 | 2.440 | 11.664 |
| 10000 | 1 | 23.414 | 427.098 | **4.609** | 10000 | 10000 | 2.340 | 1.883 | 8.976 |
| 10000 | 2 | 30.978 | 322.806 | **10.563** | 10000 | 10000 | 3.096 | 2.019 | 11.974 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.147 | **6792.696** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.207 | **4833.184** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.252 | **3971.910** | 5 | 1000 | 1000 |
| 10000 | 0 | 1.214 | **8239.915** | 5 | 10000 | 10000 |
| 10000 | 1 | 1.219 | **8203.681** | 5 | 10000 | 10000 |
| 10000 | 2 | 1.268 | **7885.896** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.136 | **7345.506** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.148 | **6738.456** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.114 | **8764.556** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.136 | **8800.477** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.173 | **8528.409** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.184 | **8446.783** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.006** | 178038.901 | 0 | 1000 |
| 1000 | 1 | **0.010** | 105060.413 | 0 | 1000 |
| 1000 | 2 | **0.012** | 81521.464 | 0 | 1000 |
| 10000 | 0 | **0.035** | 288607.577 | 0 | 10000 |
| 10000 | 1 | **0.031** | 327533.265 | 0 | 10000 |
| 10000 | 2 | **0.024** | 420699.085 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.414 | **707.314** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.453 | **688.109** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.487 | **672.527** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 13.372 | **747.812** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 13.629 | **733.738** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 13.231 | **755.825** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 130.345 | **40.528** | 10998 | 4000 | 6998 | 1000 | 24.674 | 7.672 |
| 1000 | 1 | 131.320 | **51.416** | 10998 | 4000 | 6998 | 1000 | 19.449 | 7.615 |
| 1000 | 2 | 370.987 | **50.810** | 10998 | 4000 | 6998 | 1000 | 19.681 | 2.696 |
| 10000 | 0 | 112.870 | **45.172** | 109998 | 40000 | 69998 | 10000 | 221.378 | 88.597 |
| 10000 | 1 | 244.219 | **43.189** | 109998 | 40000 | 69998 | 10000 | 231.542 | 40.947 |
| 10000 | 2 | 100.582 | **44.460** | 109998 | 40000 | 69998 | 10000 | 224.920 | 99.421 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_query_count | finished_count |
|---|---|---|---|---|---|
| 1000 | 0 | 5.843 | **171.141** | 1671 | 1000 |
| 1000 | 1 | 12.740 | **78.491** | 1671 | 1000 |
| 1000 | 2 | 10.559 | **94.703** | 1671 | 1000 |
| 10000 | 0 | 90.822 | **110.105** | 16671 | 10000 |
| 10000 | 1 | 94.005 | **106.377** | 16671 | 10000 |
| 10000 | 2 | 102.502 | **97.559** | 16671 | 10000 |

## Reproduce

```bash
docker compose up mariadb -d
bin/benchmark.py all --backend mariadb --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mariadb-20260523T181303Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mariadb-20260523T181303Z.jsonl --output docs/benchmarks/mariadb.md
```
