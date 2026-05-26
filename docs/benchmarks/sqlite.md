# dj_queue sqlite Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-26T20:43:57.588300+00:00

## Environment

- backend: `sqlite`
- database: `sqlite` `benchmark-results/dj_queue_benchmark.sqlite3`
- database version: `3.50.4`
- Python: `3.14.5`
- Django: `6.0.5`
- dj_queue: `0.10.5`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `e3d51861cac1`
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
| 1000 | 0 | 0.936 | 1068.527 | 5 | **1.098** | 1000 | 1000 | 0.935 | 0.916 | 1.233 |
| 1000 | 1 | 0.972 | 1029.197 | 5 | **1.192** | 1000 | 1000 | 0.971 | 0.940 | 1.408 |
| 1000 | 2 | 0.999 | 1000.872 | 5 | **1.268** | 1000 | 1000 | 0.999 | 0.950 | 1.460 |
| 10000 | 0 | 9.879 | 1012.198 | 5 | **1.167** | 10000 | 10000 | 0.987 | 0.933 | 1.591 |
| 10000 | 1 | 9.926 | 1007.416 | 5 | **1.220** | 10000 | 10000 | 0.992 | 0.941 | 1.471 |
| 10000 | 2 | 10.012 | 998.841 | 5 | **1.243** | 10000 | 10000 | 1.001 | 0.957 | 1.731 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.066 | **15242.808** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.071 | **14016.579** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.063 | **15890.188** | 5 | 1000 | 1000 |
| 10000 | 0 | 0.670 | **14933.634** | 10 | 10000 | 10000 |
| 10000 | 1 | 0.666 | **15013.087** | 10 | 10000 | 10000 |
| 10000 | 2 | 0.666 | **15022.189** | 10 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.103 | **9753.417** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.088 | **11428.512** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.088 | **11336.481** | 1000 | 1000 | 1000 |
| 10000 | 0 | 0.980 | **10199.206** | 10000 | 10000 | 10000 |
| 10000 | 1 | 0.974 | **10272.011** | 10000 | 10000 | 10000 |
| 10000 | 2 | 0.967 | **10344.835** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.002** | 485162.046 | 0 | 1000 |
| 1000 | 1 | **0.002** | 486440.472 | 0 | 1000 |
| 1000 | 2 | **0.002** | 420831.141 | 0 | 1000 |
| 10000 | 0 | **0.005** | 1826484.018 | 0 | 10000 |
| 10000 | 1 | **0.005** | 1834792.023 | 0 | 10000 |
| 10000 | 2 | **0.005** | 1847660.400 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 3.333 | **300.059** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 1 | 3.321 | **301.101** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 2 | 3.393 | **294.736** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 10000 | 0 | 33.972 | **294.358** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 1 | 33.819 | **295.688** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 2 | 33.740 | **296.384** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |

## Reproduce

```bash
bin/benchmark.py all --backend sqlite --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/sqlite-20260526T204024Z.jsonl
bin/benchmark.py report benchmark-results/sqlite-20260526T204024Z.jsonl --output docs/benchmarks/sqlite.md
```
