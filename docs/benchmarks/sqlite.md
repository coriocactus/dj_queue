# dj_queue sqlite Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-23T11:43:11.688596+00:00

## Environment

- backend: `sqlite`
- database: `sqlite` `benchmark-results/dj_queue_benchmark.sqlite3`
- database version: `3.50.4`
- Python: `3.14.4`
- Django: `6.0.5`
- dj_queue: `0.9.2`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `b0af38279ead`
- benchmark worker count: `1`
- benchmark worker threads: `1`
- preserve finished jobs: `True`
- database CONN_MAX_AGE: `0`

## Results

### `single-enqueue`: one-by-one immediate enqueue latency and throughput

- key metric: **`latency_p95_ms`** - enqueue tail latency for individual task submissions; lower is better
- good number: `<= 20 ms` for request-path enqueue on the 10k local benchmark
- use case: web requests, admin actions, and small fan-out paths that submit tasks one at a time
- mechanics: calls the public `Task.enqueue()` path once per job, including validation, job insert, ready-row insert, result mapping, and ready wakeup registration

| size | run | duration_seconds | jobs_per_second | **latency_p95_ms** | ready_count | job_count | latency_mean_ms | latency_p50_ms | latency_p99_ms |
|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.230 | 812.874 | **1.393** | 1000 | 1000 | 1.230 | 1.186 | 2.149 |
| 1000 | 1 | 1.338 | 747.395 | **1.676** | 1000 | 1000 | 1.337 | 1.268 | 2.711 |
| 1000 | 2 | 1.249 | 800.910 | **1.397** | 1000 | 1000 | 1.248 | 1.212 | 1.597 |
| 10000 | 0 | 12.904 | 774.963 | **1.451** | 10000 | 10000 | 1.290 | 1.239 | 1.696 |
| 10000 | 1 | 12.809 | 780.721 | **1.451** | 10000 | 10000 | 1.280 | 1.240 | 1.652 |
| 10000 | 2 | 12.700 | 787.395 | **1.453** | 10000 | 10000 | 1.269 | 1.236 | 1.720 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- good number: `>= 5,000 jobs/sec` for 10k independent immediate jobs
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.101 | **9925.017** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.103 | **9745.999** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.105 | **9535.623** | 5 | 1000 | 1000 |
| 10000 | 0 | 1.003 | **9968.899** | 10 | 10000 | 10000 |
| 10000 | 1 | 1.000 | **10001.937** | 10 | 10000 | 10000 |
| 10000 | 2 | 1.038 | **9633.148** | 10 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- good number: `>= 5,000 rows/sec` for a 10k due-row promotion burst
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.108 | **9288.350** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.103 | **9678.660** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.107 | **9372.163** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.067 | **9375.350** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.051 | **9514.278** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.049 | **9533.425** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- good number: `<= 0.050 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.002** | 458137.670 | 0 | 1000 |
| 1000 | 1 | **0.003** | 343829.768 | 0 | 1000 |
| 1000 | 2 | **0.002** | 418082.048 | 0 | 1000 |
| 10000 | 0 | **0.006** | 1806630.224 | 0 | 10000 |
| 10000 | 1 | **0.006** | 1812100.080 | 0 | 10000 |
| 10000 | 2 | **0.006** | 1810801.322 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- good number: `>= 250 jobs/sec` for draining 10k no-op ready jobs
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 3.320 | **301.179** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 1 | 3.348 | **298.728** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 2 | 3.349 | **298.628** | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 10000 | 0 | 33.465 | **298.822** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 1 | 33.656 | **297.120** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 2 | 33.473 | **298.749** | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |

## Reproduce

```bash
bin/benchmark.py all --backend sqlite --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/sqlite-20260523T091754Z.jsonl
bin/benchmark.py report benchmark-results/sqlite-20260523T091754Z.jsonl --output docs/benchmarks/sqlite.md
```
