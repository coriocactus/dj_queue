# dj_queue sqlite Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-06-09T14:57:23.443997+00:00

## Environment

- backend: `sqlite`
- database: `sqlite` `benchmark-results/dj_queue_benchmark.sqlite3`
- database version: `3.50.4`
- Python: `3.14.5`
- Django: `6.0.5`
- dj_queue: `0.12.0`
- platform: `macOS-26.5.1-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `cef37cdd23be`
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
| 1000 | 0 | 1.003 | 996.817 | 5 | **1.670** | 1000 | 1000 | 1.003 | 0.894 | 1.980 |
| 1000 | 1 | 0.951 | 1051.385 | 5 | **1.590** | 1000 | 1000 | 0.951 | 0.872 | 1.781 |
| 1000 | 2 | 0.985 | 1015.729 | 5 | **1.604** | 1000 | 1000 | 0.984 | 0.898 | 1.855 |
| 10000 | 0 | 9.939 | 1006.117 | 5 | **1.238** | 10000 | 10000 | 0.993 | 0.940 | 1.667 |
| 10000 | 1 | 10.108 | 989.329 | 5 | **1.234** | 10000 | 10000 | 1.010 | 0.959 | 1.448 |
| 10000 | 2 | 9.942 | 1005.795 | 5 | **1.226** | 10000 | 10000 | 0.994 | 0.948 | 1.508 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.065 | **15444.274** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.075 | **13312.566** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.064 | **15715.796** | 5 | 1000 | 1000 |
| 10000 | 0 | 0.692 | **14453.777** | 10 | 10000 | 10000 |
| 10000 | 1 | 0.689 | **14513.489** | 10 | 10000 | 10000 |
| 10000 | 2 | 0.687 | **14560.155** | 10 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.088 | **11319.137** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.088 | **11335.068** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.096 | **10391.356** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.008 | **9922.463** | 10000 | 10000 | 10000 |
| 10000 | 1 | 0.995 | **10049.965** | 10000 | 10000 | 10000 |
| 10000 | 2 | 0.975 | **10256.210** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | query_count | fired_count | recurring_task_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | **0.001** | 1581027.706 | 1 | 0 | 1000 |
| 1000 | 1 | **0.001** | 1511430.184 | 1 | 0 | 1000 |
| 1000 | 2 | **0.001** | 1577184.990 | 1 | 0 | 1000 |
| 10000 | 0 | **0.001** | 7190793.799 | 1 | 0 | 10000 |
| 10000 | 1 | **0.001** | 7352265.283 | 1 | 0 | 10000 |
| 10000 | 2 | **0.001** | 7026992.070 | 1 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 2.191 | **456.333** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 1 | 2.105 | **475.112** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 2 | 2.112 | **473.533** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 10000 | 0 | 25.594 | **390.713** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 1 | 24.806 | **403.126** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 2 | 24.761 | **403.855** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |

## Reproduce

```bash
bin/benchmark.py all --backend sqlite --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/sqlite-20260609T145432Z.jsonl
bin/benchmark.py report benchmark-results/sqlite-20260609T145432Z.jsonl --output docs/benchmarks/sqlite.md
```
