# dj_queue sqlite Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-11-15T15:24:50.515864+00:00

## Environment

- backend: `sqlite`
- database: `sqlite` `benchmark-results/dj_queue_benchmark.sqlite3`
- database version: `3.54.1`
- Python: `3.14.10`
- Django: `6.1.3`
- psycopg implementation: `None`
- dj_queue: `0.19.0`
- platform: `macOS-27.2.1-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `45d4035b87ee`
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
| 1000 | 0 | 0.613 | 1631.427 | 4 | **0.771** | 1000 | 1000 | 0.613 | 0.601 | 0.863 |
| 1000 | 1 | 0.614 | 1628.957 | 4 | **0.757** | 1000 | 1000 | 0.613 | 0.602 | 0.898 |
| 1000 | 2 | 0.611 | 1635.689 | 4 | **0.752** | 1000 | 1000 | 0.611 | 0.603 | 0.864 |
| 10000 | 0 | 6.244 | 1601.553 | 4 | **0.754** | 10000 | 10000 | 0.624 | 0.610 | 0.840 |
| 10000 | 1 | 6.273 | 1594.029 | 4 | **0.756** | 10000 | 10000 | 0.627 | 0.612 | 0.850 |
| 10000 | 2 | 6.423 | 1556.804 | 4 | **0.777** | 10000 | 10000 | 0.642 | 0.624 | 0.880 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.056 | **17920.638** | 4 | 1000 | 1000 |
| 1000 | 1 | 0.049 | **20551.517** | 4 | 1000 | 1000 |
| 1000 | 2 | 0.049 | **20495.548** | 4 | 1000 | 1000 |
| 10000 | 0 | 0.535 | **18676.168** | 4 | 10000 | 10000 |
| 10000 | 1 | 0.549 | **18221.677** | 4 | 10000 | 10000 |
| 10000 | 2 | 0.532 | **18786.543** | 4 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.061 | **16344.888** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.071 | **14008.832** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.061 | **16262.697** | 1000 | 1000 | 1000 |
| 10000 | 0 | 0.691 | **14475.587** | 10000 | 10000 | 10000 |
| 10000 | 1 | 0.707 | **14136.061** | 10000 | 10000 | 10000 |
| 10000 | 2 | 0.725 | **13795.770** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | query_count | fired_count | recurring_task_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | **0.001** | 1177394.483 | 1 | 0 | 1000 |
| 1000 | 1 | **0.001** | 1255953.152 | 1 | 0 | 1000 |
| 1000 | 2 | **0.001** | 1340633.561 | 1 | 0 | 1000 |
| 10000 | 0 | **0.002** | 5260043.364 | 1 | 0 | 10000 |
| 10000 | 1 | **0.002** | 5740389.664 | 1 | 0 | 10000 |
| 10000 | 2 | **0.003** | 3633446.341 | 1 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.532 | **652.919** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 1 | 1.502 | **665.809** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 2 | 1.465 | **682.800** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 10000 | 0 | 14.840 | **673.851** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 1 | 15.892 | **629.256** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 2 | 15.427 | **648.214** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |

## Reproduce

```bash
bin/benchmark.py all --backend sqlite --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/sqlite-20261115T152243Z.jsonl
bin/benchmark.py report benchmark-results/sqlite-20261115T152243Z.jsonl --output docs/benchmarks/sqlite.md
```
