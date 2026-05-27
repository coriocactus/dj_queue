# dj_queue sqlite Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-27T06:10:06.887444+00:00

## Environment

- backend: `sqlite`
- database: `sqlite` `benchmark-results/dj_queue_benchmark.sqlite3`
- database version: `3.50.4`
- Python: `3.14.5`
- Django: `6.0.5`
- dj_queue: `0.10.6`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `8a524c8f1d1f`
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
| 1000 | 0 | 0.955 | 1047.173 | 5 | **1.336** | 1000 | 1000 | 0.954 | 0.897 | 1.567 |
| 1000 | 1 | 0.982 | 1018.030 | 5 | **1.332** | 1000 | 1000 | 0.982 | 0.959 | 1.572 |
| 1000 | 2 | 0.982 | 1018.414 | 5 | **1.286** | 1000 | 1000 | 0.981 | 0.967 | 1.479 |
| 10000 | 0 | 9.990 | 1001.042 | 5 | **1.293** | 10000 | 10000 | 0.998 | 0.967 | 1.595 |
| 10000 | 1 | 9.994 | 1000.616 | 5 | **1.305** | 10000 | 10000 | 0.999 | 0.975 | 1.576 |
| 10000 | 2 | 9.974 | 1002.583 | 5 | **1.285** | 10000 | 10000 | 0.997 | 0.969 | 1.549 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.064 | **15694.470** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.072 | **13805.719** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.064 | **15675.988** | 5 | 1000 | 1000 |
| 10000 | 0 | 0.671 | **14896.231** | 10 | 10000 | 10000 |
| 10000 | 1 | 0.669 | **14937.038** | 10 | 10000 | 10000 |
| 10000 | 2 | 0.668 | **14975.444** | 10 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.101 | **9862.634** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.087 | **11508.374** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.088 | **11393.609** | 1000 | 1000 | 1000 |
| 10000 | 0 | 0.964 | **10374.040** | 10000 | 10000 | 10000 |
| 10000 | 1 | 0.959 | **10431.558** | 10000 | 10000 | 10000 |
| 10000 | 2 | 0.954 | **10481.275** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.003** | 360609.473 | 0 | 1000 |
| 1000 | 1 | **0.002** | 423968.284 | 0 | 1000 |
| 1000 | 2 | **0.002** | 412817.999 | 0 | 1000 |
| 10000 | 0 | **0.006** | 1801369.148 | 0 | 10000 |
| 10000 | 1 | **0.006** | 1776961.480 | 0 | 10000 |
| 10000 | 2 | **0.006** | 1777238.129 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 3.347 | **298.769** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 1 | 3.325 | **300.768** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 2 | 3.274 | **305.462** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 10000 | 0 | 33.456 | **298.901** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 1 | 33.256 | **300.701** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 2 | 33.098 | **302.135** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |

## Reproduce

```bash
bin/benchmark.py all --backend sqlite --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/sqlite-20260527T060635Z.jsonl
bin/benchmark.py report benchmark-results/sqlite-20260527T060635Z.jsonl --output docs/benchmarks/sqlite.md
```
