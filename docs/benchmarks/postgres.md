# dj_queue PostgreSQL Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-11-14T21:40:40.979184+00:00

## Environment

- backend: `postgres`
- database: `postgresql` `dj_queue_benchmark`
- database version: `PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit`
- Python: `3.14.10`
- Django: `6.1.3`
- psycopg implementation: `binary`
- dj_queue: `0.17.0`
- platform: `macOS-27.2.1-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `9602450ee9bb`
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
| 1000 | 0 | 2.632 | 379.883 | 4 | **3.788** | 1000 | 1000 | 2.630 | 2.618 | 4.393 |
| 1000 | 1 | 2.682 | 372.848 | 4 | **3.832** | 1000 | 1000 | 2.680 | 2.578 | 4.490 |
| 1000 | 2 | 2.806 | 356.436 | 4 | **4.326** | 1000 | 1000 | 2.803 | 2.675 | 5.702 |
| 10000 | 0 | 30.595 | 326.848 | 4 | **5.245** | 10000 | 10000 | 3.057 | 2.883 | 6.599 |
| 10000 | 1 | 30.856 | 324.085 | 4 | **5.286** | 10000 | 10000 | 3.083 | 2.931 | 6.168 |
| 10000 | 2 | 29.886 | 334.610 | 4 | **5.301** | 10000 | 10000 | 2.986 | 2.639 | 6.189 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.081 | **12296.877** | 4 | 1000 | 1000 |
| 1000 | 1 | 0.069 | **14496.518** | 4 | 1000 | 1000 |
| 1000 | 2 | 0.063 | **15806.341** | 4 | 1000 | 1000 |
| 10000 | 0 | 0.533 | **18755.452** | 4 | 10000 | 10000 |
| 10000 | 1 | 0.539 | **18546.636** | 4 | 10000 | 10000 |
| 10000 | 2 | 0.546 | **18328.436** | 4 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.045 | **22140.977** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.044 | **22939.463** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.049 | **20394.324** | 1000 | 1000 | 1000 |
| 10000 | 0 | 0.485 | **20638.009** | 10000 | 10000 | 10000 |
| 10000 | 1 | 0.484 | **20644.906** | 10000 | 10000 | 10000 |
| 10000 | 2 | 0.492 | **20322.825** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | query_count | fired_count | recurring_task_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | **0.001** | 1178318.472 | 1 | 0 | 1000 |
| 1000 | 1 | **0.001** | 1225239.424 | 1 | 0 | 1000 |
| 1000 | 2 | **0.001** | 1178435.103 | 1 | 0 | 1000 |
| 10000 | 0 | **0.001** | 6743279.296 | 1 | 0 | 10000 |
| 10000 | 1 | **0.002** | 5265696.465 | 1 | 0 | 10000 |
| 10000 | 2 | **0.002** | 5790387.972 | 1 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 0.331 | **3025.510** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 0.305 | **3274.338** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 0.308 | **3244.252** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 2.423 | **4127.352** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 2.454 | **4074.436** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 2.433 | **4110.608** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `held-xmin-worker-drain`: PostgreSQL worker drain under a held repeatable-read snapshot

- key metric: **`jobs_per_second`** - end-to-end worker-drain throughput while a second connection pins xmin; higher is better
- healthy local baseline: compare with `worker-drain` and watch dead tuples and relation bytes during the hold
- use case: PostgreSQL operations where long transactions, replication slots, or prepared transactions can delay vacuum cleanup of queue churn
- mechanics: opens a second PostgreSQL connection, begins a repeatable-read transaction to pin xmin, drains ready jobs through `worker-drain`, then samples queue-table dead tuples and relation bytes before, during, and after releasing the snapshot

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | dead_tuples_before | dead_tuples_during | dead_tuples_after_release | relation_bytes_before | relation_bytes_during | relation_bytes_after_release | claimed_count | completed_count | held_xmin | job_count | live_tuples_after_release | live_tuples_before | live_tuples_during | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 0.316 | **3161.717** | 1000 | 0 | 98291 | 89291 | 89291 | 442368 | 2097152 | 2097152 | 0 | 1000 | True | 1000 | 45709 | 43714 | 45709 | True | 4 |
| 1000 | 1 | 0.314 | **3183.443** | 1000 | 0 | 89291 | 91291 | 91291 | 442368 | 2113536 | 2113536 | 0 | 1000 | True | 1000 | 45709 | 45709 | 45709 | True | 4 |
| 1000 | 2 | 0.334 | **2997.526** | 1000 | 0 | 91291 | 96000 | 96000 | 442368 | 2080768 | 2080768 | 0 | 1000 | True | 1000 | 47714 | 45709 | 47714 | True | 4 |
| 10000 | 0 | 2.471 | **4046.545** | 10000 | 0 | 124163 | 144172 | 144172 | 442368 | 13557760 | 13557760 | 0 | 10000 | True | 10000 | 65833 | 54847 | 65833 | True | 4 |
| 10000 | 1 | 2.511 | **3982.636** | 10000 | 0 | 144172 | 164061 | 164061 | 442368 | 13434880 | 13434880 | 0 | 10000 | True | 10000 | 75944 | 65833 | 75944 | True | 4 |
| 10000 | 2 | 2.541 | **3935.660** | 10000 | 0 | 164061 | 184144 | 184144 | 442368 | 13524992 | 13524992 | 0 | 10000 | True | 10000 | 85861 | 75944 | 85861 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 275.917 | 3000 | **112.019** | 8001 | 5000 | 3001 | 1000 | 8.927 | 3.624 |
| 1000 | 1 | 273.349 | 3000 | **114.032** | 8001 | 5000 | 3001 | 1000 | 8.769 | 3.658 |
| 1000 | 2 | 250.487 | 3000 | **106.002** | 8001 | 5000 | 3001 | 1000 | 9.434 | 3.992 |
| 10000 | 0 | 273.068 | 30000 | **116.230** | 80001 | 50000 | 30001 | 10000 | 86.037 | 36.621 |
| 10000 | 1 | 447.204 | 30000 | **145.384** | 80001 | 50000 | 30001 | 10000 | 68.783 | 22.361 |
| 10000 | 2 | 264.419 | 30000 | **133.011** | 80001 | 50000 | 30001 | 10000 | 75.182 | 37.819 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 264.512 | **112.868** | 1000 | 0 | 0 | 0 | 1000 | 8.860 | 3.781 | 1000 | True | 4 |
| 1000 | 1 | 256.440 | **115.077** | 1000 | 0 | 0 | 0 | 1000 | 8.690 | 3.900 | 1000 | True | 4 |
| 1000 | 2 | 239.940 | **118.666** | 1000 | 0 | 0 | 0 | 1000 | 8.427 | 4.168 | 1000 | True | 4 |
| 10000 | 0 | 241.620 | **114.532** | 10000 | 0 | 0 | 0 | 10000 | 87.312 | 41.387 | 10000 | True | 4 |
| 10000 | 1 | 247.961 | **122.372** | 10000 | 0 | 0 | 0 | 10000 | 81.718 | 40.329 | 10000 | True | 4 |
| 10000 | 2 | 234.652 | **105.561** | 10000 | 0 | 0 | 0 | 10000 | 94.732 | 42.616 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 4.412 | **226.667** | 2.199 | 1670 | 2.197 | 1000 | 1000 |
| 1000 | 1 | 5.400 | **185.172** | 2.655 | 1670 | 2.726 | 1000 | 1000 |
| 1000 | 2 | 5.001 | **199.973** | 2.483 | 1670 | 2.499 | 1000 | 1000 |
| 10000 | 0 | 50.991 | **196.111** | 25.441 | 16670 | 25.371 | 10000 | 10000 |
| 10000 | 1 | 51.931 | **192.563** | 25.869 | 16670 | 25.880 | 10000 | 10000 |
| 10000 | 2 | 54.728 | **182.723** | 27.098 | 16670 | 27.432 | 10000 | 10000 |

## Reproduce

```bash
docker compose up postgres -d
bin/benchmark.py all --backend postgres --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/postgres-20261114T211608Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/postgres-20261114T211608Z.jsonl --output docs/benchmarks/postgres.md
```
