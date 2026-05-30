# dj_queue sqlite Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-30T10:00:59.778205+00:00

## Environment

- backend: `sqlite`
- database: `sqlite` `benchmark-results/dj_queue_benchmark.sqlite3`
- database version: `3.50.4`
- Python: `3.14.5`
- Django: `6.0.5`
- dj_queue: `0.10.6`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `75282c93bac5`
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
| 1000 | 0 | 0.976 | 1024.108 | 5 | **1.590** | 1000 | 1000 | 0.976 | 0.881 | 1.718 |
| 1000 | 1 | 0.992 | 1007.923 | 5 | **1.618** | 1000 | 1000 | 0.992 | 0.887 | 1.874 |
| 1000 | 2 | 1.031 | 970.278 | 5 | **1.760** | 1000 | 1000 | 1.030 | 0.902 | 2.022 |
| 10000 | 0 | 10.048 | 995.212 | 5 | **1.582** | 10000 | 10000 | 1.004 | 0.912 | 1.788 |
| 10000 | 1 | 10.102 | 989.945 | 5 | **1.553** | 10000 | 10000 | 1.010 | 0.913 | 1.772 |
| 10000 | 2 | 10.834 | 923.037 | 5 | **1.264** | 10000 | 10000 | 1.083 | 0.947 | 1.663 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.065 | **15384.477** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.082 | **12261.739** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.066 | **15175.486** | 5 | 1000 | 1000 |
| 10000 | 0 | 0.712 | **14041.372** | 10 | 10000 | 10000 |
| 10000 | 1 | 0.705 | **14194.356** | 10 | 10000 | 10000 |
| 10000 | 2 | 0.688 | **14535.375** | 10 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.110 | **9072.733** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.094 | **10613.678** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.097 | **10330.930** | 1000 | 1000 | 1000 |
| 10000 | 0 | 0.998 | **10023.702** | 10000 | 10000 | 10000 |
| 10000 | 1 | 0.994 | **10057.498** | 10000 | 10000 | 10000 |
| 10000 | 2 | 0.993 | **10071.268** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.003** | 345358.602 | 0 | 1000 |
| 1000 | 1 | **0.003** | 292918.749 | 0 | 1000 |
| 1000 | 2 | **0.003** | 353794.440 | 0 | 1000 |
| 10000 | 0 | **0.006** | 1692584.375 | 0 | 10000 |
| 10000 | 1 | **0.006** | 1666805.570 | 0 | 10000 |
| 10000 | 2 | **0.006** | 1605684.119 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 3.686 | **271.288** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 1 | 3.477 | **287.590** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 2 | 3.459 | **289.117** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 10000 | 0 | 35.970 | **278.008** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 1 | 35.853 | **278.917** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 2 | 35.463 | **281.985** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |

## Reproduce

```bash
bin/benchmark.py all --backend sqlite --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/sqlite-20260530T095716Z.jsonl
bin/benchmark.py report benchmark-results/sqlite-20260530T095716Z.jsonl --output docs/benchmarks/sqlite.md
```
