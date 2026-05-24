# dj_queue sqlite Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-24T22:23:34.880047+00:00

## Environment

- backend: `sqlite`
- database: `sqlite` `benchmark-results/dj_queue_benchmark.sqlite3`
- database version: `3.50.4`
- Python: `3.14.4`
- Django: `6.0.5`
- dj_queue: `0.10.3`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `5dbf01bdd08f`
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
| 1000 | 0 | 1.195 | 836.519 | **1.405** | 1000 | 1000 | 1.195 | 1.052 | 2.305 |
| 1000 | 1 | 0.965 | 1036.200 | **1.193** | 1000 | 1000 | 0.965 | 0.923 | 1.350 |
| 1000 | 2 | 0.999 | 1001.274 | **1.231** | 1000 | 1000 | 0.998 | 0.968 | 1.464 |
| 10000 | 0 | 10.142 | 985.969 | **1.186** | 10000 | 10000 | 1.014 | 0.963 | 1.423 |
| 10000 | 1 | 9.969 | 1003.085 | **1.175** | 10000 | 10000 | 0.996 | 0.940 | 1.613 |
| 10000 | 2 | 10.131 | 987.068 | **1.165** | 10000 | 10000 | 1.013 | 0.951 | 1.437 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.079 | **12649.375** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.075 | **13249.859** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.080 | **12534.764** | 5 | 1000 | 1000 |
| 10000 | 0 | 0.813 | **12302.445** | 10 | 10000 | 10000 |
| 10000 | 1 | 0.776 | **12894.658** | 10 | 10000 | 10000 |
| 10000 | 2 | 0.775 | **12901.659** | 10 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.099 | **10061.611** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.099 | **10088.128** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.100 | **9981.538** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.046 | **9558.180** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.050 | **9522.722** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.075 | **9305.140** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.002** | 475190.988 | 0 | 1000 |
| 1000 | 1 | **0.002** | 476256.702 | 0 | 1000 |
| 1000 | 2 | **0.002** | 472441.094 | 0 | 1000 |
| 10000 | 0 | **0.006** | 1669530.466 | 0 | 10000 |
| 10000 | 1 | **0.006** | 1625333.614 | 0 | 10000 |
| 10000 | 2 | **0.006** | 1757199.026 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 3.156 | **316.818** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 1 | 3.002 | **333.111** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 2 | 3.089 | **323.740** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 10000 | 0 | 31.327 | **319.216** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 1 | 31.186 | **320.660** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 2 | 30.871 | **323.927** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |

## Reproduce

```bash
bin/benchmark.py all --backend sqlite --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/sqlite-20260524T222012Z.jsonl
bin/benchmark.py report benchmark-results/sqlite-20260524T222012Z.jsonl --output docs/benchmarks/sqlite.md
```
