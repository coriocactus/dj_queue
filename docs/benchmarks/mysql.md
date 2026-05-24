# dj_queue mysql Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-24T22:20:12.412864+00:00

## Environment

- backend: `mysql`
- database: `mysql` `dj_queue_benchmark`
- database version: `8.4.8`
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
| 1000 | 0 | 4.542 | 220.152 | **6.387** | 1000 | 1000 | 4.541 | 4.201 | 10.251 |
| 1000 | 1 | 4.522 | 221.125 | **5.813** | 1000 | 1000 | 4.521 | 4.353 | 8.789 |
| 1000 | 2 | 4.594 | 217.655 | **6.300** | 1000 | 1000 | 4.593 | 4.365 | 8.338 |
| 10000 | 0 | 51.543 | 194.012 | **9.706** | 10000 | 10000 | 5.153 | 4.417 | 16.165 |
| 10000 | 1 | 47.313 | 211.358 | **6.808** | 10000 | 10000 | 4.730 | 4.350 | 11.458 |
| 10000 | 2 | 55.014 | 181.772 | **10.766** | 10000 | 10000 | 5.500 | 4.427 | 17.473 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.258 | **3871.252** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.225 | **4444.448** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.295 | **3384.535** | 5 | 1000 | 1000 |
| 10000 | 0 | 1.616 | **6189.808** | 5 | 10000 | 10000 |
| 10000 | 1 | 1.726 | **5795.380** | 5 | 10000 | 10000 |
| 10000 | 2 | 1.582 | **6322.625** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.226 | **4429.965** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.197 | **5087.355** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.163 | **6145.869** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.070 | **9342.567** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.119 | **8940.227** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.135 | **8807.027** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.006** | 171935.782 | 0 | 1000 |
| 1000 | 1 | **0.009** | 113593.869 | 0 | 1000 |
| 1000 | 2 | **0.006** | 176194.626 | 0 | 1000 |
| 10000 | 0 | **0.020** | 511808.942 | 0 | 10000 |
| 10000 | 1 | **0.021** | 467645.721 | 0 | 10000 |
| 10000 | 2 | **0.019** | 522748.280 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.645 | **607.869** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.472 | **679.421** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.437 | **695.797** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 13.880 | **720.448** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 14.884 | **671.867** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 13.817 | **723.741** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 225.227 | **75.380** | 11998 | 5000 | 6998 | 1000 | 13.266 | 4.440 |
| 1000 | 1 | 116.895 | **74.344** | 11998 | 5000 | 6998 | 1000 | 13.451 | 8.555 |
| 1000 | 2 | 213.088 | **66.585** | 11998 | 5000 | 6998 | 1000 | 15.018 | 4.693 |
| 10000 | 0 | 199.134 | **78.442** | 119998 | 50000 | 69998 | 10000 | 127.483 | 50.218 |
| 10000 | 1 | 185.973 | **76.505** | 119998 | 50000 | 69998 | 10000 | 130.711 | 53.771 |
| 10000 | 2 | 208.090 | **76.743** | 119998 | 50000 | 69998 | 10000 | 130.306 | 48.056 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_query_count | finished_count |
|---|---|---|---|---|---|
| 1000 | 0 | 6.350 | **157.484** | 2005 | 1000 |
| 1000 | 1 | 7.851 | **127.365** | 2005 | 1000 |
| 1000 | 2 | 6.479 | **154.355** | 2005 | 1000 |
| 10000 | 0 | 91.052 | **109.827** | 20005 | 10000 |
| 10000 | 1 | 76.676 | **130.419** | 20005 | 10000 |
| 10000 | 2 | 76.372 | **130.939** | 20005 | 10000 |

## Reproduce

```bash
docker compose up mysql -d
bin/benchmark.py all --backend mysql --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mysql-20260524T215512Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mysql-20260524T215512Z.jsonl --output docs/benchmarks/mysql.md
```
