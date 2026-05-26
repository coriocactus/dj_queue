# dj_queue PostgreSQL Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-26T18:56:29.673626+00:00

## Environment

- backend: `postgres`
- database: `postgresql` `dj_queue_benchmark`
- database version: `PostgreSQL 17.9 (Debian 17.9-1.pgdg13+1) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit`
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
| 1000 | 0 | 8.426 | 118.676 | 5 | **14.077** | 1000 | 1000 | 8.423 | 8.160 | 24.689 |
| 1000 | 1 | 7.294 | 137.098 | 5 | **13.757** | 1000 | 1000 | 7.291 | 6.790 | 18.987 |
| 1000 | 2 | 7.790 | 128.375 | 5 | **13.522** | 1000 | 1000 | 7.786 | 6.924 | 16.305 |
| 10000 | 0 | 83.617 | 119.593 | 5 | **11.963** | 10000 | 10000 | 8.358 | 8.005 | 13.782 |
| 10000 | 1 | 78.720 | 127.033 | 5 | **11.727** | 10000 | 10000 | 7.868 | 7.459 | 13.784 |
| 10000 | 2 | 85.674 | 116.721 | 5 | **13.177** | 10000 | 10000 | 8.563 | 8.122 | 15.588 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.104 | **9605.056** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.074 | **13572.189** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.072 | **13916.755** | 5 | 1000 | 1000 |
| 10000 | 0 | 0.769 | **13001.545** | 5 | 10000 | 10000 |
| 10000 | 1 | 0.795 | **12573.838** | 5 | 10000 | 10000 |
| 10000 | 2 | 0.774 | **12912.228** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.089 | **11243.293** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.111 | **8974.143** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.113 | **8822.790** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.041 | **9609.506** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.130 | **8848.512** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.099 | **9101.644** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.005** | 218928.132 | 0 | 1000 |
| 1000 | 1 | **0.004** | 259695.336 | 0 | 1000 |
| 1000 | 2 | **0.004** | 246320.586 | 0 | 1000 |
| 10000 | 0 | **0.008** | 1230018.654 | 0 | 10000 |
| 10000 | 1 | **0.015** | 677325.100 | 0 | 10000 |
| 10000 | 2 | **0.011** | 891576.854 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.806 | **553.627** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.804 | **554.457** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.737 | **575.832** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 16.339 | **612.016** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 16.614 | **601.910** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 16.719 | **598.117** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 79.255 | 4999 | **38.716** | 8999 | 4000 | 4999 | 1000 | 25.829 | 12.618 |
| 1000 | 1 | 77.432 | 4999 | **38.474** | 8999 | 4000 | 4999 | 1000 | 25.992 | 12.915 |
| 1000 | 2 | 81.086 | 4999 | **38.768** | 8999 | 4000 | 4999 | 1000 | 25.794 | 12.333 |
| 10000 | 0 | 102.227 | 49999 | **48.162** | 89999 | 40000 | 49999 | 10000 | 207.634 | 97.821 |
| 10000 | 1 | 100.090 | 49999 | **48.763** | 89999 | 40000 | 49999 | 10000 | 205.072 | 99.910 |
| 10000 | 2 | 105.216 | 49999 | **45.793** | 89999 | 40000 | 49999 | 10000 | 218.372 | 95.043 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 108.757 | **74.821** | 1000 | 0 | 0 | 0 | 1000 | 13.365 | 9.195 | 1000 | True | 4 |
| 1000 | 1 | 101.029 | **84.442** | 1000 | 0 | 0 | 0 | 1000 | 11.842 | 9.898 | 1000 | True | 4 |
| 1000 | 2 | 120.333 | **87.013** | 1000 | 0 | 0 | 0 | 1000 | 11.493 | 8.310 | 1000 | True | 4 |
| 10000 | 0 | 88.611 | **86.460** | 10000 | 0 | 0 | 0 | 10000 | 115.661 | 112.852 | 10000 | True | 4 |
| 10000 | 1 | 92.751 | **92.571** | 10000 | 0 | 0 | 0 | 10000 | 108.025 | 107.816 | 10000 | True | 4 |
| 10000 | 2 | 85.633 | **80.582** | 10000 | 0 | 0 | 0 | 10000 | 124.098 | 116.778 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 11.379 | **87.884** | 5.752 | 1671 | 5.601 | 1000 | 1000 |
| 1000 | 1 | 10.651 | **93.889** | 5.390 | 1671 | 5.237 | 1000 | 1000 |
| 1000 | 2 | 11.412 | **87.629** | 5.717 | 1671 | 5.668 | 1000 | 1000 |
| 10000 | 0 | 109.984 | **90.922** | 54.284 | 16671 | 55.433 | 10000 | 10000 |
| 10000 | 1 | 111.201 | **89.927** | 55.449 | 16671 | 55.484 | 10000 | 10000 |
| 10000 | 2 | 116.655 | **85.723** | 58.194 | 16671 | 58.192 | 10000 | 10000 |

## Reproduce

```bash
docker compose up postgres -d
bin/benchmark.py all --backend postgres --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/postgres-20260526T175954Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/postgres-20260526T175954Z.jsonl --output docs/benchmarks/postgres.md
```
