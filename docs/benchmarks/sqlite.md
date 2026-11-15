# dj_queue sqlite Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-11-15T11:12:37.976195+00:00

## Environment

- backend: `sqlite`
- database: `sqlite` `benchmark-results/dj_queue_benchmark.sqlite3`
- database version: `3.54.1`
- Python: `3.14.10`
- Django: `6.1.3`
- psycopg implementation: `None`
- dj_queue: `0.18.0`
- platform: `macOS-27.2.1-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `a56d9403ea6d`
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
| 1000 | 0 | 0.584 | 1712.560 | 4 | **0.706** | 1000 | 1000 | 0.584 | 0.564 | 0.863 |
| 1000 | 1 | 0.578 | 1730.630 | 4 | **0.698** | 1000 | 1000 | 0.577 | 0.566 | 0.795 |
| 1000 | 2 | 0.598 | 1672.173 | 4 | **0.750** | 1000 | 1000 | 0.598 | 0.579 | 0.907 |
| 10000 | 0 | 6.242 | 1602.102 | 4 | **0.748** | 10000 | 10000 | 0.624 | 0.612 | 0.835 |
| 10000 | 1 | 6.232 | 1604.715 | 4 | **0.752** | 10000 | 10000 | 0.623 | 0.608 | 0.846 |
| 10000 | 2 | 6.068 | 1647.856 | 4 | **0.735** | 10000 | 10000 | 0.606 | 0.591 | 0.822 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.056 | **17880.504** | 4 | 1000 | 1000 |
| 1000 | 1 | 0.048 | **20674.310** | 4 | 1000 | 1000 |
| 1000 | 2 | 0.049 | **20343.534** | 4 | 1000 | 1000 |
| 10000 | 0 | 0.537 | **18632.717** | 4 | 10000 | 10000 |
| 10000 | 1 | 0.549 | **18201.310** | 4 | 10000 | 10000 |
| 10000 | 2 | 0.538 | **18584.742** | 4 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.061 | **16289.176** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.071 | **14015.761** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.061 | **16283.175** | 1000 | 1000 | 1000 |
| 10000 | 0 | 0.690 | **14489.263** | 10000 | 10000 | 10000 |
| 10000 | 1 | 0.701 | **14274.972** | 10000 | 10000 | 10000 |
| 10000 | 2 | 0.707 | **14140.862** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | query_count | fired_count | recurring_task_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | **0.001** | 1264822.141 | 1 | 0 | 1000 |
| 1000 | 1 | **0.001** | 1301588.464 | 1 | 0 | 1000 |
| 1000 | 2 | **0.001** | 1363326.522 | 1 | 0 | 1000 |
| 10000 | 0 | **0.002** | 5995354.850 | 1 | 0 | 10000 |
| 10000 | 1 | **0.002** | 6163009.095 | 1 | 0 | 10000 |
| 10000 | 2 | **0.002** | 6095238.069 | 1 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.412 | **708.199** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 1 | 1.405 | **711.698** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 2 | 1.464 | **683.228** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 10000 | 0 | 14.501 | **689.586** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 1 | 14.184 | **705.022** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 2 | 14.314 | **698.604** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |

## Reproduce

```bash
bin/benchmark.py all --backend sqlite --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/sqlite-20261115T111051Z.jsonl
bin/benchmark.py report benchmark-results/sqlite-20261115T111051Z.jsonl --output docs/benchmarks/sqlite.md
```
