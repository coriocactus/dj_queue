# dj_queue sqlite Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-09-02T22:25:20.324907+00:00

## Environment

- backend: `sqlite`
- database: `sqlite` `benchmark-results/dj_queue_benchmark.sqlite3`
- database version: `3.50.4`
- Python: `3.14.5`
- Django: `6.1`
- dj_queue: `0.14.0`
- platform: `macOS-26.6.2-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `2ae301b9176e`
- benchmark worker count: `1`
- benchmark worker threads: `1`
- preserve finished jobs: `True`
- database CONN_MAX_AGE: `0`

## Results

### `single-enqueue`: one-by-one immediate enqueue latency and throughput

- key metric: **`latency_p95_ms`** - enqueue tail latency for individual task submissions; lower is better
- healthy local baseline: `<= 15 ms` p95 for request-path enqueue on the 10k local benchmark
- use case: web requests, admin actions, and small fan-out paths that submit tasks one at a time
- mechanics: calls the public `Task.enqueue()` path once per job, including validation, job insert, ready-row insert, result mapping, and ready wakeup registration

| size | run | duration_seconds | jobs_per_second | query_count_sample | **latency_p95_ms** | ready_count | job_count | latency_mean_ms | latency_p50_ms | latency_p99_ms |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 0.991 | 1009.104 | 5 | **1.243** | 1000 | 1000 | 0.990 | 0.946 | 1.924 |
| 1000 | 1 | 0.975 | 1025.309 | 5 | **1.198** | 1000 | 1000 | 0.975 | 0.950 | 1.349 |
| 1000 | 2 | 1.007 | 993.131 | 5 | **1.244** | 1000 | 1000 | 1.006 | 0.979 | 1.563 |
| 10000 | 0 | 10.445 | 957.394 | 5 | **1.274** | 10000 | 10000 | 1.044 | 1.005 | 1.493 |
| 10000 | 1 | 10.447 | 957.207 | 5 | **1.282** | 10000 | 10000 | 1.044 | 1.011 | 1.497 |
| 10000 | 2 | 10.503 | 952.086 | 5 | **1.296** | 10000 | 10000 | 1.050 | 1.017 | 1.527 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.083 | **11976.640** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.072 | **13968.057** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.071 | **14182.662** | 5 | 1000 | 1000 |
| 10000 | 0 | 0.767 | **13032.834** | 11 | 10000 | 10000 |
| 10000 | 1 | 0.792 | **12622.144** | 11 | 10000 | 10000 |
| 10000 | 2 | 0.765 | **13078.681** | 11 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.089 | **11204.560** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.093 | **10723.396** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.094 | **10655.613** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.037 | **9644.909** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.034 | **9674.361** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.035 | **9663.570** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | query_count | fired_count | recurring_task_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | **0.001** | 798349.652 | 1 | 0 | 1000 |
| 1000 | 1 | **0.001** | 837813.543 | 1 | 0 | 1000 |
| 1000 | 2 | **0.001** | 845785.400 | 1 | 0 | 1000 |
| 10000 | 0 | **0.002** | 4035512.495 | 1 | 0 | 10000 |
| 10000 | 1 | **0.003** | 3947756.964 | 1 | 0 | 10000 |
| 10000 | 2 | **0.003** | 3974167.916 | 1 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 2.265 | **441.422** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 1 | 2.245 | **445.385** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 2 | 2.205 | **453.423** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 10000 | 0 | 22.881 | **437.047** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 1 | 22.659 | **441.316** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 2 | 22.955 | **435.632** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |

## Reproduce

```bash
bin/benchmark.py all --backend sqlite --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/sqlite-20260902T222231Z.jsonl
bin/benchmark.py report benchmark-results/sqlite-20260902T222231Z.jsonl --output docs/benchmarks/sqlite.md
```
