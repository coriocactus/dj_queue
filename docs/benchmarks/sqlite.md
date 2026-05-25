# dj_queue sqlite Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-25T15:51:09.982120+00:00

## Environment

- backend: `sqlite`
- database: `sqlite` `benchmark-results/dj_queue_benchmark.sqlite3`
- database version: `3.50.4`
- Python: `3.14.4`
- Django: `6.0.5`
- dj_queue: `0.10.3`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `1a65bc8ef066`
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
| 1000 | 0 | 0.957 | 1044.908 | 5 | **1.150** | 1000 | 1000 | 0.956 | 0.924 | 1.855 |
| 1000 | 1 | 0.997 | 1002.776 | 5 | **1.335** | 1000 | 1000 | 0.997 | 0.945 | 1.810 |
| 1000 | 2 | 0.964 | 1037.841 | 5 | **1.369** | 1000 | 1000 | 0.963 | 0.892 | 1.884 |
| 10000 | 0 | 9.955 | 1004.529 | 5 | **1.192** | 10000 | 10000 | 0.995 | 0.954 | 1.569 |
| 10000 | 1 | 9.976 | 1002.376 | 5 | **1.205** | 10000 | 10000 | 0.997 | 0.956 | 1.449 |
| 10000 | 2 | 9.968 | 1003.191 | 5 | **1.194** | 10000 | 10000 | 0.996 | 0.956 | 1.501 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.079 | **12597.162** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.073 | **13772.160** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.076 | **13108.250** | 5 | 1000 | 1000 |
| 10000 | 0 | 0.750 | **13333.124** | 10 | 10000 | 10000 |
| 10000 | 1 | 0.754 | **13259.929** | 10 | 10000 | 10000 |
| 10000 | 2 | 0.755 | **13245.126** | 10 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.098 | **10217.314** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.101 | **9889.730** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.102 | **9824.811** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.069 | **9355.746** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.033 | **9678.140** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.071 | **9332.851** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.002** | 463723.169 | 0 | 1000 |
| 1000 | 1 | **0.002** | 469685.775 | 0 | 1000 |
| 1000 | 2 | **0.002** | 475031.175 | 0 | 1000 |
| 10000 | 0 | **0.006** | 1814538.886 | 0 | 10000 |
| 10000 | 1 | **0.005** | 1820112.464 | 0 | 10000 |
| 10000 | 2 | **0.005** | 1899680.890 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 3.006 | **332.688** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 1 | 2.982 | **335.301** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 2 | 2.959 | **337.898** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 10000 | 0 | 30.788 | **324.798** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 1 | 30.479 | **328.095** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 2 | 30.681 | **325.937** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |

## Reproduce

```bash
bin/benchmark.py all --backend sqlite --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/sqlite-20260525T154750Z.jsonl
bin/benchmark.py report benchmark-results/sqlite-20260525T154750Z.jsonl --output docs/benchmarks/sqlite.md
```
