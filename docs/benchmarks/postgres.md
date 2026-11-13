# dj_queue PostgreSQL Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-11-13T14:18:29.772942+00:00

## Environment

- backend: `postgres`
- database: `postgresql` `dj_queue_benchmark`
- database version: `PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit`
- Python: `3.14.10`
- Django: `6.1.2`
- psycopg implementation: `binary`
- dj_queue: `0.15.0`
- platform: `macOS-27.2.1-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `32fdd401633f`
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
| 1000 | 0 | 2.446 | 408.812 | 4 | **3.760** | 1000 | 1000 | 2.445 | 2.055 | 3.960 |
| 1000 | 1 | 2.802 | 356.888 | 4 | **3.782** | 1000 | 1000 | 2.800 | 3.114 | 4.126 |
| 1000 | 2 | 2.678 | 373.391 | 4 | **3.754** | 1000 | 1000 | 2.676 | 2.745 | 4.088 |
| 10000 | 0 | 24.881 | 401.920 | 4 | **3.625** | 10000 | 10000 | 2.486 | 2.111 | 4.158 |
| 10000 | 1 | 24.032 | 416.109 | 4 | **3.697** | 10000 | 10000 | 2.402 | 1.977 | 3.983 |
| 10000 | 2 | 20.365 | 491.034 | 4 | **3.595** | 10000 | 10000 | 2.035 | 1.778 | 4.224 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.062 | **16045.902** | 4 | 1000 | 1000 |
| 1000 | 1 | 0.055 | **18026.084** | 4 | 1000 | 1000 |
| 1000 | 2 | 0.053 | **19039.036** | 4 | 1000 | 1000 |
| 10000 | 0 | 0.554 | **18040.192** | 4 | 10000 | 10000 |
| 10000 | 1 | 0.562 | **17803.254** | 4 | 10000 | 10000 |
| 10000 | 2 | 0.538 | **18595.663** | 4 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.039 | **25501.151** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.038 | **26303.157** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.047 | **21463.008** | 1000 | 1000 | 1000 |
| 10000 | 0 | 0.497 | **20108.917** | 10000 | 10000 | 10000 |
| 10000 | 1 | 0.503 | **19871.282** | 10000 | 10000 | 10000 |
| 10000 | 2 | 0.474 | **21107.064** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | query_count | fired_count | recurring_task_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | **0.001** | 1149480.790 | 1 | 0 | 1000 |
| 1000 | 1 | **0.001** | 1161271.600 | 1 | 0 | 1000 |
| 1000 | 2 | **0.001** | 1076860.952 | 1 | 0 | 1000 |
| 10000 | 0 | **0.001** | 6742711.008 | 1 | 0 | 10000 |
| 10000 | 1 | **0.002** | 6281734.552 | 1 | 0 | 10000 |
| 10000 | 2 | **0.001** | 6865382.857 | 1 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 0.310 | **3230.296** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 0.297 | **3369.102** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 0.338 | **2955.061** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 2.453 | **4075.945** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 2.442 | **4094.348** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 2.453 | **4076.570** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `held-xmin-worker-drain`: PostgreSQL worker drain under a held repeatable-read snapshot

- key metric: **`jobs_per_second`** - end-to-end worker-drain throughput while a second connection pins xmin; higher is better
- healthy local baseline: compare with `worker-drain` and watch dead tuples and relation bytes during the hold
- use case: PostgreSQL operations where long transactions, replication slots, or prepared transactions can delay vacuum cleanup of queue churn
- mechanics: opens a second PostgreSQL connection, begins a repeatable-read transaction to pin xmin, drains ready jobs through `worker-drain`, then samples queue-table dead tuples and relation bytes before, during, and after releasing the snapshot

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | dead_tuples_before | dead_tuples_during | dead_tuples_after_release | relation_bytes_before | relation_bytes_during | relation_bytes_after_release | claimed_count | completed_count | held_xmin | job_count | live_tuples_after_release | live_tuples_before | live_tuples_during | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 0.323 | **3100.287** | 1000 | 0 | 30357 | 24141 | 24141 | 442368 | 2064384 | 2064384 | 0 | 1000 | True | 1000 | 12000 | 10010 | 12000 | True | 4 |
| 1000 | 1 | 0.324 | **3084.412** | 1000 | 0 | 24141 | 26141 | 26141 | 442368 | 2080768 | 2080768 | 0 | 1000 | True | 1000 | 12000 | 12000 | 12000 | True | 4 |
| 1000 | 2 | 0.308 | **3244.587** | 1000 | 0 | 26141 | 26141 | 26141 | 442368 | 2113536 | 2113536 | 0 | 1000 | True | 1000 | 14005 | 12000 | 14005 | True | 4 |
| 10000 | 0 | 2.435 | **4105.960** | 10000 | 0 | 56486 | 76489 | 76489 | 442368 | 13533184 | 13533184 | 0 | 10000 | True | 10000 | 33657 | 23665 | 33657 | True | 4 |
| 10000 | 1 | 2.425 | **4123.410** | 10000 | 0 | 76489 | 96501 | 96501 | 442368 | 13500416 | 13500416 | 0 | 10000 | True | 10000 | 43645 | 33657 | 43645 | True | 4 |
| 10000 | 2 | 2.455 | **4073.801** | 10000 | 0 | 96501 | 116348 | 116348 | 442368 | 13524992 | 13524992 | 0 | 10000 | True | 10000 | 53798 | 43645 | 53798 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 309.800 | 3000 | **112.493** | 8001 | 5000 | 3001 | 1000 | 8.889 | 3.228 |
| 1000 | 1 | 314.253 | 3000 | **113.937** | 8001 | 5000 | 3001 | 1000 | 8.777 | 3.182 |
| 1000 | 2 | 332.332 | 3000 | **116.591** | 8001 | 5000 | 3001 | 1000 | 8.577 | 3.009 |
| 10000 | 0 | 296.598 | 30000 | **110.867** | 80001 | 50000 | 30001 | 10000 | 90.198 | 33.716 |
| 10000 | 1 | 290.625 | 30000 | **111.491** | 80001 | 50000 | 30001 | 10000 | 89.694 | 34.409 |
| 10000 | 2 | 287.501 | 30000 | **114.886** | 80001 | 50000 | 30001 | 10000 | 87.043 | 34.783 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 292.519 | **121.401** | 1000 | 0 | 0 | 0 | 1000 | 8.237 | 3.419 | 1000 | True | 4 |
| 1000 | 1 | 331.574 | **138.715** | 1000 | 0 | 0 | 0 | 1000 | 7.209 | 3.016 | 1000 | True | 4 |
| 1000 | 2 | 360.307 | **137.571** | 1000 | 0 | 0 | 0 | 1000 | 7.269 | 2.775 | 1000 | True | 4 |
| 10000 | 0 | 303.843 | **125.969** | 10000 | 0 | 0 | 0 | 10000 | 79.385 | 32.912 | 10000 | True | 4 |
| 10000 | 1 | 318.258 | **120.867** | 10000 | 0 | 0 | 0 | 10000 | 82.735 | 31.421 | 10000 | True | 4 |
| 10000 | 2 | 290.305 | **122.611** | 10000 | 0 | 0 | 0 | 10000 | 81.559 | 34.446 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 3.630 | **275.502** | 1.838 | 1670 | 1.778 | 1000 | 1000 |
| 1000 | 1 | 3.376 | **296.189** | 1.617 | 1670 | 1.749 | 1000 | 1000 |
| 1000 | 2 | 1.946 | **513.981** | 0.924 | 1670 | 1.015 | 1000 | 1000 |
| 10000 | 0 | 20.227 | **494.391** | 10.308 | 16670 | 9.846 | 10000 | 10000 |
| 10000 | 1 | 38.747 | **258.082** | 18.867 | 16670 | 19.752 | 10000 | 10000 |
| 10000 | 2 | 39.857 | **250.899** | 19.726 | 16670 | 20.001 | 10000 | 10000 |

## Reproduce

```bash
docker compose up postgres -d
bin/benchmark.py all --backend postgres --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/postgres-20261113T135628Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/postgres-20261113T135628Z.jsonl --output docs/benchmarks/postgres.md
```
