# dj_queue sqlite Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-11-14T22:58:12.389632+00:00

## Environment

- backend: `sqlite`
- database: `sqlite` `benchmark-results/dj_queue_benchmark.sqlite3`
- database version: `3.54.1`
- Python: `3.14.10`
- Django: `6.1.3`
- psycopg implementation: `None`
- dj_queue: `0.17.0`
- platform: `macOS-27.2.1-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `4d0c3b99d972`
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
| 1000 | 0 | 0.763 | 1310.135 | 4 | **0.869** | 1000 | 1000 | 0.763 | 0.595 | 2.804 |
| 1000 | 1 | 0.596 | 1676.685 | 4 | **0.721** | 1000 | 1000 | 0.596 | 0.593 | 0.796 |
| 1000 | 2 | 0.745 | 1341.414 | 4 | **0.861** | 1000 | 1000 | 0.745 | 0.609 | 2.660 |
| 10000 | 0 | 12.028 | 831.412 | 4 | **1.944** | 10000 | 10000 | 1.202 | 0.637 | 8.414 |
| 10000 | 1 | 7.389 | 1353.281 | 4 | **0.839** | 10000 | 10000 | 0.739 | 0.641 | 1.080 |
| 10000 | 2 | 16.023 | 624.112 | 4 | **6.937** | 10000 | 10000 | 1.602 | 0.658 | 32.061 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.055 | **18119.690** | 4 | 1000 | 1000 |
| 1000 | 1 | 0.047 | **21063.385** | 4 | 1000 | 1000 |
| 1000 | 2 | 0.048 | **20853.698** | 4 | 1000 | 1000 |
| 10000 | 0 | 0.523 | **19123.946** | 4 | 10000 | 10000 |
| 10000 | 1 | 0.539 | **18551.185** | 4 | 10000 | 10000 |
| 10000 | 2 | 0.526 | **19000.701** | 4 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.062 | **16162.345** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.071 | **14132.162** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.062 | **16253.401** | 1000 | 1000 | 1000 |
| 10000 | 0 | 0.686 | **14573.313** | 10000 | 10000 | 10000 |
| 10000 | 1 | 0.710 | **14091.122** | 10000 | 10000 | 10000 |
| 10000 | 2 | 0.710 | **14093.043** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | query_count | fired_count | recurring_task_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | **0.001** | 1263955.656 | 1 | 0 | 1000 |
| 1000 | 1 | **0.001** | 1385521.312 | 1 | 0 | 1000 |
| 1000 | 2 | **0.001** | 1386080.123 | 1 | 0 | 1000 |
| 10000 | 0 | **0.002** | 6205240.837 | 1 | 0 | 10000 |
| 10000 | 1 | **0.002** | 6160954.985 | 1 | 0 | 10000 |
| 10000 | 2 | **0.002** | 6208607.955 | 1 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.471 | **679.767** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 1 | 1.586 | **630.605** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 2 | 1.435 | **697.036** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 10000 | 0 | 13.927 | **718.055** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 1 | 14.070 | **710.752** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 2 | 14.308 | **698.929** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |

## Reproduce

```bash
bin/benchmark.py all --backend sqlite --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/sqlite-20261114T225605Z.jsonl
bin/benchmark.py report benchmark-results/sqlite-20261114T225605Z.jsonl --output docs/benchmarks/sqlite.md
```
