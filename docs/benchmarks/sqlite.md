# dj_queue sqlite Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-06-03T15:37:48.885404+00:00

## Environment

- backend: `sqlite`
- database: `sqlite` `benchmark-results/dj_queue_benchmark.sqlite3`
- database version: `3.50.4`
- Python: `3.14.5`
- Django: `6.0.5`
- dj_queue: `0.12.0`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `411646c33337`
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
| 1000 | 0 | 1.065 | 939.046 | 5 | **1.369** | 1000 | 1000 | 1.064 | 1.007 | 1.848 |
| 1000 | 1 | 0.967 | 1034.438 | 5 | **1.205** | 1000 | 1000 | 0.966 | 0.932 | 1.503 |
| 1000 | 2 | 0.982 | 1018.489 | 5 | **1.219** | 1000 | 1000 | 0.981 | 0.947 | 1.396 |
| 10000 | 0 | 10.294 | 971.486 | 5 | **1.273** | 10000 | 10000 | 1.029 | 0.996 | 1.502 |
| 10000 | 1 | 10.150 | 985.261 | 5 | **1.262** | 10000 | 10000 | 1.014 | 0.978 | 1.479 |
| 10000 | 2 | 10.186 | 981.711 | 5 | **1.280** | 10000 | 10000 | 1.018 | 0.982 | 1.527 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.064 | **15616.887** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.079 | **12617.713** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.064 | **15610.122** | 5 | 1000 | 1000 |
| 10000 | 0 | 0.706 | **14155.821** | 10 | 10000 | 10000 |
| 10000 | 1 | 0.708 | **14118.724** | 10 | 10000 | 10000 |
| 10000 | 2 | 0.755 | **13253.264** | 10 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.112 | **8916.033** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.091 | **11046.637** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.093 | **10728.328** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.014 | **9857.526** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.016 | **9844.217** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.044 | **9578.694** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.003** | 384516.886 | 0 | 1000 |
| 1000 | 1 | **0.003** | 346966.217 | 0 | 1000 |
| 1000 | 2 | **0.003** | 323537.374 | 0 | 1000 |
| 10000 | 0 | **0.007** | 1524157.899 | 0 | 10000 |
| 10000 | 1 | **0.007** | 1424966.675 | 0 | 10000 |
| 10000 | 2 | **0.006** | 1555270.421 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 2.156 | **463.812** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 1 | 2.180 | **458.636** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 2 | 2.412 | **414.646** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 10000 | 0 | 22.753 | **439.496** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 1 | 22.784 | **438.911** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 2 | 22.702 | **440.494** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |

## Reproduce

```bash
bin/benchmark.py all --backend sqlite --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/sqlite-20260603T153501Z.jsonl
bin/benchmark.py report benchmark-results/sqlite-20260603T153501Z.jsonl --output docs/benchmarks/sqlite.md
```
