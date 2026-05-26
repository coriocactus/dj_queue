# dj_queue sqlite Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-26T08:09:54.735939+00:00

## Environment

- backend: `sqlite`
- database: `sqlite` `benchmark-results/dj_queue_benchmark.sqlite3`
- database version: `3.50.4`
- Python: `3.14.5`
- Django: `6.0.5`
- dj_queue: `0.10.4`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `bccceb8adc16`
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
| 1000 | 0 | 1.025 | 975.531 | 5 | **1.370** | 1000 | 1000 | 1.024 | 0.953 | 2.184 |
| 1000 | 1 | 0.962 | 1039.295 | 5 | **1.186** | 1000 | 1000 | 0.962 | 0.928 | 1.343 |
| 1000 | 2 | 0.947 | 1055.873 | 5 | **1.139** | 1000 | 1000 | 0.947 | 0.928 | 1.334 |
| 10000 | 0 | 10.060 | 994.050 | 5 | **1.204** | 10000 | 10000 | 1.005 | 0.972 | 1.399 |
| 10000 | 1 | 9.897 | 1010.375 | 5 | **1.183** | 10000 | 10000 | 0.989 | 0.959 | 1.370 |
| 10000 | 2 | 10.029 | 997.100 | 5 | **1.178** | 10000 | 10000 | 1.002 | 0.958 | 1.390 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.075 | **13292.518** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.074 | **13424.884** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.075 | **13365.366** | 5 | 1000 | 1000 |
| 10000 | 0 | 0.815 | **12266.245** | 10 | 10000 | 10000 |
| 10000 | 1 | 0.829 | **12064.812** | 10 | 10000 | 10000 |
| 10000 | 2 | 0.819 | **12216.720** | 10 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.100 | **10026.164** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.105 | **9545.560** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.101 | **9910.353** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.138 | **8787.006** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.151 | **8685.327** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.135 | **8811.476** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.002** | 479424.768 | 0 | 1000 |
| 1000 | 1 | **0.002** | 404906.166 | 0 | 1000 |
| 1000 | 2 | **0.002** | 475595.980 | 0 | 1000 |
| 10000 | 0 | **0.006** | 1782968.193 | 0 | 10000 |
| 10000 | 1 | **0.006** | 1762813.449 | 0 | 10000 |
| 10000 | 2 | **0.006** | 1724732.658 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 3.078 | **324.883** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 1 | 3.029 | **330.145** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 2 | 3.045 | **328.393** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 10000 | 0 | 30.590 | **326.905** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 1 | 30.275 | **330.302** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 2 | 29.484 | **339.163** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |

## Reproduce

```bash
bin/benchmark.py all --backend sqlite --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/sqlite-20260526T080633Z.jsonl
bin/benchmark.py report benchmark-results/sqlite-20260526T080633Z.jsonl --output docs/benchmarks/sqlite.md
```
