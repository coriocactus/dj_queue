# dj_queue PostgreSQL Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-06-09T13:43:41.488993+00:00

## Environment

- backend: `postgres`
- database: `postgresql` `dj_queue_benchmark`
- database version: `PostgreSQL 17.9 (Debian 17.9-1.pgdg13+1) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit`
- Python: `3.14.5`
- Django: `6.0.5`
- dj_queue: `0.12.0`
- platform: `macOS-26.5.1-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `cef37cdd23be`
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
| 1000 | 0 | 1.914 | 522.469 | 5 | **2.237** | 1000 | 1000 | 1.913 | 1.771 | 3.464 |
| 1000 | 1 | 1.916 | 521.912 | 5 | **2.372** | 1000 | 1000 | 1.915 | 1.757 | 3.124 |
| 1000 | 2 | 1.658 | 603.309 | 5 | **2.062** | 1000 | 1000 | 1.657 | 1.583 | 2.935 |
| 10000 | 0 | 45.731 | 218.669 | 5 | **7.971** | 10000 | 10000 | 4.571 | 4.280 | 10.960 |
| 10000 | 1 | 29.600 | 337.842 | 5 | **6.982** | 10000 | 10000 | 2.959 | 2.021 | 8.963 |
| 10000 | 2 | 22.716 | 440.210 | 5 | **3.500** | 10000 | 10000 | 2.271 | 2.058 | 4.593 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.075 | **13367.942** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.074 | **13458.588** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.073 | **13722.198** | 5 | 1000 | 1000 |
| 10000 | 0 | 0.732 | **13670.405** | 5 | 10000 | 10000 |
| 10000 | 1 | 0.728 | **13738.436** | 5 | 10000 | 10000 |
| 10000 | 2 | 0.762 | **13118.899** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.082 | **12226.602** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.083 | **11982.733** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.084 | **11865.663** | 1000 | 1000 | 1000 |
| 10000 | 0 | 0.889 | **11248.012** | 10000 | 10000 | 10000 |
| 10000 | 1 | 0.886 | **11291.586** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.001 | **9990.260** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | query_count | fired_count | recurring_task_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | **0.001** | 1712575.181 | 1 | 0 | 1000 |
| 1000 | 1 | **0.001** | 1518794.311 | 1 | 0 | 1000 |
| 1000 | 2 | **0.001** | 1477104.930 | 1 | 0 | 1000 |
| 10000 | 0 | **0.001** | 12459770.470 | 1 | 0 | 10000 |
| 10000 | 1 | **0.001** | 14296784.010 | 1 | 0 | 10000 |
| 10000 | 2 | **0.001** | 12585883.325 | 1 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.475 | **677.814** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.298 | **770.352** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.297 | **771.291** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 14.100 | **709.212** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 13.391 | **746.798** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 13.266 | **753.818** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `held-xmin-worker-drain`: PostgreSQL worker drain under a held repeatable-read snapshot

- key metric: **`jobs_per_second`** - end-to-end worker-drain throughput while a second connection pins xmin; higher is better
- healthy local baseline: compare with `worker-drain` and watch dead tuples and relation bytes during the hold
- use case: PostgreSQL operations where long transactions, replication slots, or prepared transactions can delay vacuum cleanup of queue churn
- mechanics: opens a second PostgreSQL connection, begins a repeatable-read transaction to pin xmin, drains ready jobs through `worker-drain`, then samples queue-table dead tuples and relation bytes before, during, and after releasing the snapshot

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | dead_tuples_before | dead_tuples_during | dead_tuples_after_release | relation_bytes_before | relation_bytes_during | relation_bytes_after_release | claimed_count | completed_count | held_xmin | job_count | live_tuples_after_release | live_tuples_before | live_tuples_during | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.330 | **751.894** | 1000 | 0 | 55381 | 57206 | 57206 | 442368 | 2121728 | 2121728 | 0 | 1000 | True | 1000 | 32399 | 31219 | 32399 | True | 4 |
| 1000 | 1 | 1.438 | **695.438** | 1000 | 0 | 57206 | 31015 | 31015 | 442368 | 2113536 | 2113536 | 0 | 1000 | True | 1000 | 33285 | 32399 | 33285 | True | 4 |
| 1000 | 2 | 1.321 | **756.924** | 1000 | 0 | 31015 | 32841 | 32841 | 442368 | 2113536 | 2113536 | 0 | 1000 | True | 1000 | 34464 | 33285 | 34464 | True | 4 |
| 10000 | 0 | 13.921 | **718.347** | 10000 | 0 | 62265 | 29584 | 29584 | 442368 | 13590528 | 13590528 | 0 | 10000 | True | 10000 | 20421 | 44422 | 20421 | True | 4 |
| 10000 | 1 | 12.690 | **788.053** | 10000 | 0 | 29584 | 40167 | 40167 | 442368 | 13631488 | 13631488 | 0 | 10000 | True | 10000 | 20201 | 20421 | 20201 | True | 4 |
| 10000 | 2 | 12.535 | **797.751** | 10000 | 0 | 40167 | 60243 | 60243 | 442368 | 13615104 | 13615104 | 0 | 10000 | True | 10000 | 30178 | 20201 | 30178 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 554.962 | 3001 | **123.640** | 7000 | 3000 | 4000 | 1000 | 8.088 | 1.802 |
| 1000 | 1 | 293.132 | 3001 | **100.880** | 7000 | 3000 | 4000 | 1000 | 9.913 | 3.411 |
| 1000 | 2 | 284.927 | 3001 | **71.149** | 7000 | 3000 | 4000 | 1000 | 14.055 | 3.510 |
| 10000 | 0 | 166.180 | 30001 | **80.458** | 70000 | 30000 | 40000 | 10000 | 124.288 | 60.176 |
| 10000 | 1 | 447.822 | 30001 | **104.538** | 70000 | 30000 | 40000 | 10000 | 95.659 | 22.330 |
| 10000 | 2 | 414.924 | 30001 | **99.667** | 70000 | 30000 | 40000 | 10000 | 100.334 | 24.101 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 488.532 | **167.717** | 1000 | 0 | 0 | 0 | 1000 | 5.962 | 2.047 | 1000 | True | 4 |
| 1000 | 1 | 549.730 | **157.914** | 1000 | 0 | 0 | 0 | 1000 | 6.333 | 1.819 | 1000 | True | 4 |
| 1000 | 2 | 453.314 | **156.927** | 1000 | 0 | 0 | 0 | 1000 | 6.372 | 2.206 | 1000 | True | 4 |
| 10000 | 0 | 482.430 | **156.096** | 10000 | 0 | 0 | 0 | 10000 | 64.063 | 20.728 | 10000 | True | 4 |
| 10000 | 1 | 483.500 | **162.535** | 10000 | 0 | 0 | 0 | 10000 | 61.525 | 20.683 | 10000 | True | 4 |
| 10000 | 2 | 322.774 | **160.238** | 10000 | 0 | 0 | 0 | 10000 | 62.407 | 30.981 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 4.965 | **201.421** | 2.130 | 1002 | 2.821 | 1000 | 1000 |
| 1000 | 1 | 5.111 | **195.637** | 2.184 | 1002 | 2.914 | 1000 | 1000 |
| 1000 | 2 | 5.450 | **183.502** | 2.330 | 1002 | 3.105 | 1000 | 1000 |
| 10000 | 0 | 52.763 | **189.526** | 25.590 | 10002 | 27.037 | 10000 | 10000 |
| 10000 | 1 | 50.761 | **197.002** | 24.421 | 10002 | 26.213 | 10000 | 10000 |
| 10000 | 2 | 53.797 | **185.884** | 24.835 | 10002 | 28.810 | 10000 | 10000 |

## Reproduce

```bash
docker compose up postgres -d
bin/benchmark.py all --backend postgres --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/postgres-20260609T131721Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/postgres-20260609T131721Z.jsonl --output docs/benchmarks/postgres.md
```
