# dj_queue sqlite Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-06-03T07:37:43.902639+00:00

## Environment

- backend: `sqlite`
- database: `sqlite` `benchmark-results/dj_queue_benchmark.sqlite3`
- database version: `3.50.4`
- Python: `3.14.5`
- Django: `6.0.5`
- dj_queue: `0.11.0`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `cb4d0997597c`
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
| 1000 | 0 | 0.965 | 1036.592 | 5 | **1.160** | 1000 | 1000 | 0.964 | 0.928 | 1.742 |
| 1000 | 1 | 1.032 | 968.636 | 5 | **1.102** | 1000 | 1000 | 1.032 | 0.907 | 1.414 |
| 1000 | 2 | 0.981 | 1019.562 | 5 | **1.172** | 1000 | 1000 | 0.980 | 0.919 | 1.935 |
| 10000 | 0 | 9.846 | 1015.620 | 5 | **1.164** | 10000 | 10000 | 0.984 | 0.943 | 1.452 |
| 10000 | 1 | 9.841 | 1016.114 | 5 | **1.180** | 10000 | 10000 | 0.984 | 0.942 | 1.412 |
| 10000 | 2 | 9.949 | 1005.112 | 5 | **1.189** | 10000 | 10000 | 0.994 | 0.953 | 1.428 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.065 | **15409.933** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.078 | **12895.925** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.066 | **15062.453** | 5 | 1000 | 1000 |
| 10000 | 0 | 0.689 | **14505.867** | 10 | 10000 | 10000 |
| 10000 | 1 | 0.691 | **14478.902** | 10 | 10000 | 10000 |
| 10000 | 2 | 0.683 | **14642.625** | 10 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.103 | **9688.733** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.086 | **11579.558** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.088 | **11402.937** | 1000 | 1000 | 1000 |
| 10000 | 0 | 0.974 | **10263.040** | 10000 | 10000 | 10000 |
| 10000 | 1 | 0.974 | **10270.432** | 10000 | 10000 | 10000 |
| 10000 | 2 | 0.964 | **10373.482** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.002** | 420123.933 | 0 | 1000 |
| 1000 | 1 | **0.002** | 448816.308 | 0 | 1000 |
| 1000 | 2 | **0.003** | 322645.695 | 0 | 1000 |
| 10000 | 0 | **0.006** | 1638068.723 | 0 | 10000 |
| 10000 | 1 | **0.006** | 1776133.001 | 0 | 10000 |
| 10000 | 2 | **0.006** | 1728197.698 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 2.965 | **337.223** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 1 | 2.904 | **344.404** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 2 | 2.864 | **349.109** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 10000 | 0 | 29.936 | **334.046** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 1 | 29.799 | **335.581** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 2 | 29.969 | **333.676** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |

## Reproduce

```bash
bin/benchmark.py all --backend sqlite --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/sqlite-20260603T073427Z.jsonl
bin/benchmark.py report benchmark-results/sqlite-20260603T073427Z.jsonl --output docs/benchmarks/sqlite.md
```
