# dj_queue PostgreSQL Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-11-15T10:00:31.008597+00:00

## Environment

- backend: `postgres`
- database: `postgresql` `dj_queue_benchmark`
- database version: `PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit`
- Python: `3.14.10`
- Django: `6.1.3`
- psycopg implementation: `binary`
- dj_queue: `0.18.0`
- platform: `macOS-27.2.1-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `3149a1ffd19e`
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
| 1000 | 0 | 1.708 | 585.312 | 4 | **2.230** | 1000 | 1000 | 1.707 | 1.652 | 3.011 |
| 1000 | 1 | 2.634 | 379.648 | 4 | **3.665** | 1000 | 1000 | 2.632 | 2.548 | 4.001 |
| 1000 | 2 | 2.499 | 400.160 | 4 | **3.623** | 1000 | 1000 | 2.497 | 2.115 | 4.066 |
| 10000 | 0 | 25.645 | 389.938 | 4 | **3.848** | 10000 | 10000 | 2.563 | 2.280 | 5.076 |
| 10000 | 1 | 17.719 | 564.362 | 4 | **3.473** | 10000 | 10000 | 1.771 | 1.541 | 4.328 |
| 10000 | 2 | 21.676 | 461.335 | 4 | **3.629** | 10000 | 10000 | 2.166 | 1.962 | 4.404 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.065 | **15386.085** | 4 | 1000 | 1000 |
| 1000 | 1 | 0.059 | **16841.467** | 4 | 1000 | 1000 |
| 1000 | 2 | 0.061 | **16376.585** | 4 | 1000 | 1000 |
| 10000 | 0 | 0.544 | **18378.333** | 4 | 10000 | 10000 |
| 10000 | 1 | 0.550 | **18194.855** | 4 | 10000 | 10000 |
| 10000 | 2 | 0.549 | **18205.680** | 4 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.048 | **20947.636** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.052 | **19368.990** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.046 | **21923.214** | 1000 | 1000 | 1000 |
| 10000 | 0 | 0.442 | **22647.197** | 10000 | 10000 | 10000 |
| 10000 | 1 | 0.468 | **21357.139** | 10000 | 10000 | 10000 |
| 10000 | 2 | 0.474 | **21101.070** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | query_count | fired_count | recurring_task_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | **0.001** | 1173306.806 | 1 | 0 | 1000 |
| 1000 | 1 | **0.001** | 1230517.833 | 1 | 0 | 1000 |
| 1000 | 2 | **0.001** | 1205122.764 | 1 | 0 | 1000 |
| 10000 | 0 | **0.002** | 5123386.485 | 1 | 0 | 10000 |
| 10000 | 1 | **0.001** | 6864397.934 | 1 | 0 | 10000 |
| 10000 | 2 | **0.001** | 6751054.913 | 1 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 0.324 | **3088.976** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 0.320 | **3122.907** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 0.298 | **3354.348** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 2.423 | **4126.414** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 2.557 | **3911.440** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 2.412 | **4145.140** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `held-xmin-worker-drain`: PostgreSQL worker drain under a held repeatable-read snapshot

- key metric: **`jobs_per_second`** - end-to-end worker-drain throughput while a second connection pins xmin; higher is better
- healthy local baseline: compare with `worker-drain` and watch dead tuples and relation bytes during the hold
- use case: PostgreSQL operations where long transactions, replication slots, or prepared transactions can delay vacuum cleanup of queue churn
- mechanics: opens a second PostgreSQL connection, begins a repeatable-read transaction to pin xmin, drains ready jobs through `worker-drain`, then samples queue-table dead tuples and relation bytes before, during, and after releasing the snapshot

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | dead_tuples_before | dead_tuples_during | dead_tuples_after_release | relation_bytes_before | relation_bytes_during | relation_bytes_after_release | claimed_count | completed_count | held_xmin | job_count | live_tuples_after_release | live_tuples_before | live_tuples_during | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 0.309 | **3235.343** | 1000 | 0 | 98408 | 89408 | 89408 | 442368 | 2121728 | 2121728 | 0 | 1000 | True | 1000 | 45602 | 43597 | 45602 | True | 4 |
| 1000 | 1 | 0.304 | **3288.922** | 1000 | 0 | 89408 | 91408 | 91408 | 442368 | 2129920 | 2129920 | 0 | 1000 | True | 1000 | 45602 | 45602 | 45602 | True | 4 |
| 1000 | 2 | 0.341 | **2932.059** | 1000 | 0 | 91408 | 96016 | 96016 | 442368 | 2088960 | 2088960 | 0 | 1000 | True | 1000 | 45005 | 45602 | 45005 | True | 4 |
| 10000 | 0 | 2.497 | **4005.417** | 10000 | 0 | 124328 | 39398 | 39398 | 442368 | 13484032 | 13484032 | 0 | 10000 | True | 10000 | 10027 | 55698 | 10027 | True | 4 |
| 10000 | 1 | 2.501 | **3999.096** | 10000 | 0 | 39398 | 55514 | 55514 | 442368 | 13582336 | 13582336 | 0 | 10000 | True | 10000 | 20052 | 10027 | 20052 | True | 4 |
| 10000 | 2 | 2.506 | **3990.645** | 10000 | 0 | 55514 | 75470 | 75470 | 442368 | 13557760 | 13557760 | 0 | 10000 | True | 10000 | 31937 | 20052 | 31937 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 741.440 | 3000 | **204.223** | 8001 | 5000 | 3001 | 1000 | 4.897 | 1.349 |
| 1000 | 1 | 726.714 | 3000 | **156.717** | 8001 | 5000 | 3001 | 1000 | 6.381 | 1.376 |
| 1000 | 2 | 303.109 | 3000 | **133.311** | 8001 | 5000 | 3001 | 1000 | 7.501 | 3.299 |
| 10000 | 0 | 324.871 | 30000 | **137.680** | 80001 | 50000 | 30001 | 10000 | 72.632 | 30.781 |
| 10000 | 1 | 531.897 | 30000 | **168.373** | 80001 | 50000 | 30001 | 10000 | 59.392 | 18.801 |
| 10000 | 2 | 311.384 | 30000 | **113.984** | 80001 | 50000 | 30001 | 10000 | 87.731 | 32.115 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 497.578 | **201.509** | 1000 | 0 | 0 | 0 | 1000 | 4.963 | 2.010 | 1000 | True | 4 |
| 1000 | 1 | 301.140 | **121.705** | 1000 | 0 | 0 | 0 | 1000 | 8.217 | 3.321 | 1000 | True | 4 |
| 1000 | 2 | 346.429 | **104.907** | 1000 | 0 | 0 | 0 | 1000 | 9.532 | 2.887 | 1000 | True | 4 |
| 10000 | 0 | 340.362 | **97.997** | 10000 | 0 | 0 | 0 | 10000 | 102.044 | 29.380 | 10000 | True | 4 |
| 10000 | 1 | 305.659 | **108.439** | 10000 | 0 | 0 | 0 | 10000 | 92.218 | 32.716 | 10000 | True | 4 |
| 10000 | 2 | 310.295 | **106.078** | 10000 | 0 | 0 | 0 | 10000 | 94.270 | 32.227 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 3.726 | **268.353** | 1.787 | 1670 | 1.929 | 1000 | 1000 |
| 1000 | 1 | 3.983 | **251.039** | 1.920 | 1670 | 2.051 | 1000 | 1000 |
| 1000 | 2 | 3.939 | **253.866** | 1.876 | 1670 | 2.052 | 1000 | 1000 |
| 10000 | 0 | 39.546 | **252.873** | 19.307 | 16670 | 20.121 | 10000 | 10000 |
| 10000 | 1 | 45.237 | **221.056** | 21.831 | 16670 | 23.275 | 10000 | 10000 |
| 10000 | 2 | 41.257 | **242.383** | 20.144 | 16670 | 20.992 | 10000 | 10000 |

## Reproduce

```bash
docker compose up postgres -d
bin/benchmark.py all --backend postgres --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/postgres-20261115T093830Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/postgres-20261115T093830Z.jsonl --output docs/benchmarks/postgres.md
```
