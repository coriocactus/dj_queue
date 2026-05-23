# dj_queue sqlite Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-23T19:21:54.668762+00:00

## Environment

- backend: `sqlite`
- database: `sqlite` `benchmark-results/dj_queue_benchmark.sqlite3`
- database version: `3.50.4`
- Python: `3.14.4`
- Django: `6.0.5`
- dj_queue: `0.10.2`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `c939c210c215`
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

| size | run | duration_seconds | jobs_per_second | **latency_p95_ms** | ready_count | job_count | latency_mean_ms | latency_p50_ms | latency_p99_ms |
|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.026 | 974.434 | **1.161** | 1000 | 1000 | 1.026 | 0.940 | 1.974 |
| 1000 | 1 | 1.194 | 837.657 | **1.248** | 1000 | 1000 | 1.193 | 0.920 | 1.931 |
| 1000 | 2 | 0.981 | 1019.282 | **1.180** | 1000 | 1000 | 0.981 | 0.941 | 1.412 |
| 10000 | 0 | 9.967 | 1003.337 | **1.185** | 10000 | 10000 | 0.996 | 0.951 | 1.527 |
| 10000 | 1 | 10.380 | 963.369 | **1.258** | 10000 | 10000 | 1.038 | 0.973 | 1.586 |
| 10000 | 2 | 10.057 | 994.337 | **1.210** | 10000 | 10000 | 1.005 | 0.959 | 1.466 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.083 | **12088.768** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.076 | **13199.411** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.081 | **12319.968** | 5 | 1000 | 1000 |
| 10000 | 0 | 0.773 | **12941.519** | 10 | 10000 | 10000 |
| 10000 | 1 | 0.774 | **12913.411** | 10 | 10000 | 10000 |
| 10000 | 2 | 0.790 | **12651.387** | 10 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.102 | **9756.756** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.101 | **9915.200** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.100 | **10001.592** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.096 | **9124.385** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.055 | **9475.103** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.071 | **9335.298** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.003** | 387009.335 | 0 | 1000 |
| 1000 | 1 | **0.002** | 408607.897 | 0 | 1000 |
| 1000 | 2 | **0.002** | 487983.411 | 0 | 1000 |
| 10000 | 0 | **0.006** | 1790430.148 | 0 | 10000 |
| 10000 | 1 | **0.006** | 1678474.455 | 0 | 10000 |
| 10000 | 2 | **0.006** | 1738991.919 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 3.145 | **317.945** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 1 | 2.983 | **335.239** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 2 | 2.938 | **340.352** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 10000 | 0 | 30.778 | **324.906** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 1 | 30.095 | **332.277** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 2 | 29.965 | **333.727** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |

## Reproduce

```bash
bin/benchmark.py all --backend sqlite --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/sqlite-20260523T191832Z.jsonl
bin/benchmark.py report benchmark-results/sqlite-20260523T191832Z.jsonl --output docs/benchmarks/sqlite.md
```
