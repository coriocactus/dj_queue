# dj_queue sqlite Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-11-13T15:31:33.068613+00:00

## Environment

- backend: `sqlite`
- database: `sqlite` `benchmark-results/dj_queue_benchmark.sqlite3`
- database version: `3.54.1`
- Python: `3.14.10`
- Django: `6.1.2`
- psycopg implementation: `None`
- dj_queue: `0.15.0`
- platform: `macOS-27.2.1-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `876465e887a1`
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
| 1000 | 0 | 0.586 | 1705.697 | 4 | **0.727** | 1000 | 1000 | 0.586 | 0.575 | 0.794 |
| 1000 | 1 | 0.594 | 1683.508 | 4 | **0.725** | 1000 | 1000 | 0.594 | 0.582 | 0.805 |
| 1000 | 2 | 0.590 | 1696.268 | 4 | **0.717** | 1000 | 1000 | 0.589 | 0.581 | 0.804 |
| 10000 | 0 | 6.425 | 1556.535 | 4 | **0.763** | 10000 | 10000 | 0.642 | 0.618 | 0.843 |
| 10000 | 1 | 6.421 | 1557.481 | 4 | **0.771** | 10000 | 10000 | 0.642 | 0.628 | 0.851 |
| 10000 | 2 | 6.426 | 1556.081 | 4 | **0.771** | 10000 | 10000 | 0.642 | 0.626 | 0.868 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.057 | **17627.668** | 4 | 1000 | 1000 |
| 1000 | 1 | 0.049 | **20234.603** | 4 | 1000 | 1000 |
| 1000 | 2 | 0.050 | **20152.217** | 4 | 1000 | 1000 |
| 10000 | 0 | 0.544 | **18381.469** | 4 | 10000 | 10000 |
| 10000 | 1 | 0.557 | **17966.466** | 4 | 10000 | 10000 |
| 10000 | 2 | 0.540 | **18521.520** | 4 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.064 | **15721.756** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.074 | **13475.273** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.065 | **15438.075** | 1000 | 1000 | 1000 |
| 10000 | 0 | 0.724 | **13818.443** | 10000 | 10000 | 10000 |
| 10000 | 1 | 0.738 | **13542.571** | 10000 | 10000 | 10000 |
| 10000 | 2 | 0.745 | **13420.685** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | query_count | fired_count | recurring_task_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | **0.001** | 1211875.909 | 1 | 0 | 1000 |
| 1000 | 1 | **0.001** | 1323407.765 | 1 | 0 | 1000 |
| 1000 | 2 | **0.001** | 1315789.482 | 1 | 0 | 1000 |
| 10000 | 0 | **0.002** | 6062288.810 | 1 | 0 | 10000 |
| 10000 | 1 | **0.002** | 6206843.042 | 1 | 0 | 10000 |
| 10000 | 2 | **0.002** | 6112159.382 | 1 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.551 | **644.738** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 1 | 1.477 | **677.000** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 2 | 1.475 | **678.026** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 10000 | 0 | 14.536 | **687.970** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 1 | 15.004 | **666.479** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 2 | 16.734 | **597.581** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |

## Reproduce

```bash
bin/benchmark.py all --backend sqlite --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/sqlite-20261113T152935Z.jsonl
bin/benchmark.py report benchmark-results/sqlite-20261113T152935Z.jsonl --output docs/benchmarks/sqlite.md
```
