# dj_queue PostgreSQL Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-06-03T14:30:01.009190+00:00

## Environment

- backend: `postgres`
- database: `postgresql` `dj_queue_benchmark`
- database version: `PostgreSQL 17.9 (Debian 17.9-1.pgdg13+1) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit`
- Python: `3.14.5`
- Django: `6.0.5`
- dj_queue: `0.12.0`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `411646c33337`
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
| 1000 | 0 | 3.380 | 295.896 | 5 | **6.923** | 1000 | 1000 | 3.378 | 2.939 | 8.899 |
| 1000 | 1 | 5.106 | 195.848 | 5 | **8.000** | 1000 | 1000 | 5.103 | 4.413 | 10.789 |
| 1000 | 2 | 6.588 | 151.793 | 5 | **10.059** | 1000 | 1000 | 6.584 | 6.590 | 12.405 |
| 10000 | 0 | 53.780 | 185.943 | 5 | **8.545** | 10000 | 10000 | 5.375 | 4.900 | 11.929 |
| 10000 | 1 | 53.735 | 186.097 | 5 | **8.515** | 10000 | 10000 | 5.370 | 4.891 | 11.403 |
| 10000 | 2 | 50.802 | 196.843 | 5 | **7.932** | 10000 | 10000 | 5.077 | 4.581 | 10.895 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.088 | **11359.312** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.081 | **12301.214** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.078 | **12797.611** | 5 | 1000 | 1000 |
| 10000 | 0 | 0.728 | **13734.120** | 5 | 10000 | 10000 |
| 10000 | 1 | 0.767 | **13037.961** | 5 | 10000 | 10000 |
| 10000 | 2 | 0.701 | **14268.014** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.084 | **11944.220** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.097 | **10355.353** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.115 | **8712.544** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.078 | **9274.718** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.059 | **9442.012** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.061 | **9423.885** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.006** | 178576.753 | 0 | 1000 |
| 1000 | 1 | **0.005** | 203591.683 | 0 | 1000 |
| 1000 | 2 | **0.005** | 198130.951 | 0 | 1000 |
| 10000 | 0 | **0.015** | 653733.636 | 0 | 10000 |
| 10000 | 1 | **0.006** | 1681107.669 | 0 | 10000 |
| 10000 | 2 | **0.007** | 1498267.633 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.353 | **739.073** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.309 | **763.917** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.294 | **772.712** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 12.693 | **787.860** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 12.798 | **781.395** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 12.594 | **794.034** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 201.458 | 3001 | **70.858** | 7000 | 3000 | 4000 | 1000 | 14.113 | 4.964 |
| 1000 | 1 | 186.278 | 3001 | **70.959** | 7000 | 3000 | 4000 | 1000 | 14.093 | 5.368 |
| 1000 | 2 | 189.955 | 3001 | **70.242** | 7000 | 3000 | 4000 | 1000 | 14.237 | 5.264 |
| 10000 | 0 | 200.645 | 30001 | **64.228** | 70000 | 30000 | 40000 | 10000 | 155.696 | 49.839 |
| 10000 | 1 | 156.870 | 30001 | **60.138** | 70000 | 30000 | 40000 | 10000 | 166.284 | 63.747 |
| 10000 | 2 | 189.236 | 30001 | **60.813** | 70000 | 30000 | 40000 | 10000 | 164.439 | 52.844 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 163.678 | **142.533** | 1000 | 0 | 0 | 0 | 1000 | 7.016 | 6.110 | 1000 | True | 4 |
| 1000 | 1 | 193.600 | **138.059** | 1000 | 0 | 0 | 0 | 1000 | 7.243 | 5.165 | 1000 | True | 4 |
| 1000 | 2 | 162.532 | **144.991** | 1000 | 0 | 0 | 0 | 1000 | 6.897 | 6.153 | 1000 | True | 4 |
| 10000 | 0 | 179.006 | **152.026** | 10000 | 0 | 0 | 0 | 10000 | 65.778 | 55.864 | 10000 | True | 4 |
| 10000 | 1 | 181.005 | **148.389** | 10000 | 0 | 0 | 0 | 10000 | 67.390 | 55.247 | 10000 | True | 4 |
| 10000 | 2 | 163.310 | **143.020** | 10000 | 0 | 0 | 0 | 10000 | 69.920 | 61.233 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 6.861 | **145.747** | 3.498 | 1337 | 3.345 | 1000 | 1000 |
| 1000 | 1 | 7.265 | **137.647** | 3.652 | 1337 | 3.594 | 1000 | 1000 |
| 1000 | 2 | 6.946 | **143.962** | 3.334 | 1337 | 3.593 | 1000 | 1000 |
| 10000 | 0 | 64.061 | **156.102** | 30.552 | 13337 | 33.320 | 10000 | 10000 |
| 10000 | 1 | 60.956 | **164.053** | 29.151 | 13337 | 31.621 | 10000 | 10000 |
| 10000 | 2 | 62.081 | **161.080** | 29.619 | 13337 | 32.273 | 10000 | 10000 |

## Reproduce

```bash
docker compose up postgres -d
bin/benchmark.py all --backend postgres --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/postgres-20260603T135500Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/postgres-20260603T135500Z.jsonl --output docs/benchmarks/postgres.md
```
