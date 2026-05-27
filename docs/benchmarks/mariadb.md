# dj_queue mariadb Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-27T05:24:23.142973+00:00

## Environment

- backend: `mariadb`
- database: `mysql` `dj_queue_benchmark`
- database version: `10.6.25-MariaDB-ubu2204`
- Python: `3.14.5`
- Django: `6.0.5`
- dj_queue: `0.10.6`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `8a524c8f1d1f`
- benchmark worker count: `4`
- benchmark worker threads: `8`
- preserve finished jobs: `True`
- database CONN_MAX_AGE: `60`

## Results

### `single-enqueue`: one-by-one immediate enqueue latency and throughput

- key metric: **`latency_p95_ms`** - enqueue tail latency for individual task submissions; lower is better
- healthy local baseline: `<= 15 ms` p95 for request-path enqueue on the 10k local benchmark
- use case: web requests, admin actions, and small fan-out paths that submit tasks one at a time
- mechanics: calls the public `Task.enqueue()` path once per job, including validation, job insert, ready-row insert, result mapping, and ready wakeup registration

| size | run | duration_seconds | jobs_per_second | query_count_sample | **latency_p95_ms** | ready_count | job_count | latency_mean_ms | latency_p50_ms | latency_p99_ms |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 8.322 | 120.157 | 5 | **11.534** | 1000 | 1000 | 8.318 | 7.955 | 15.364 |
| 1000 | 1 | 7.494 | 133.432 | 5 | **11.682** | 1000 | 1000 | 7.490 | 7.055 | 14.648 |
| 1000 | 2 | 5.814 | 172.001 | 5 | **10.442** | 1000 | 1000 | 5.811 | 4.957 | 11.716 |
| 10000 | 0 | 26.374 | 379.164 | 5 | **4.950** | 10000 | 10000 | 2.636 | 2.424 | 10.137 |
| 10000 | 1 | 37.142 | 269.239 | 5 | **10.478** | 10000 | 10000 | 3.713 | 2.769 | 11.794 |
| 10000 | 2 | 33.818 | 295.698 | 5 | **9.968** | 10000 | 10000 | 3.380 | 2.493 | 11.606 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.206 | **4851.027** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.226 | **4428.160** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.209 | **4794.666** | 5 | 1000 | 1000 |
| 10000 | 0 | 1.227 | **8147.801** | 5 | 10000 | 10000 |
| 10000 | 1 | 1.247 | **8018.735** | 5 | 10000 | 10000 |
| 10000 | 2 | 1.200 | **8330.002** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.126 | **7933.593** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.130 | **7682.158** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.117 | **8582.724** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.031 | **9698.549** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.017 | **9836.661** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.054 | **9489.820** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.006** | 171146.161 | 0 | 1000 |
| 1000 | 1 | **0.008** | 132529.322 | 0 | 1000 |
| 1000 | 2 | **0.008** | 128636.671 | 0 | 1000 |
| 10000 | 0 | **0.030** | 330952.308 | 0 | 10000 |
| 10000 | 1 | **0.029** | 342391.494 | 0 | 10000 |
| 10000 | 2 | **0.022** | 455719.284 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.419 | **704.609** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.413 | **707.798** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.978 | **505.480** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 12.901 | **775.122** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 12.987 | **770.009** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 12.808 | **780.754** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 208.387 | 3001 | **37.770** | 11998 | 5000 | 6998 | 1000 | 26.476 | 4.799 |
| 1000 | 1 | 165.310 | 3001 | **37.377** | 11998 | 5000 | 6998 | 1000 | 26.754 | 6.049 |
| 1000 | 2 | 143.674 | 3001 | **36.427** | 11998 | 5000 | 6998 | 1000 | 27.452 | 6.960 |
| 10000 | 0 | 135.645 | 30001 | **34.929** | 119998 | 50000 | 69998 | 10000 | 286.293 | 73.722 |
| 10000 | 1 | 132.137 | 30001 | **34.202** | 119998 | 50000 | 69998 | 10000 | 292.385 | 75.679 |
| 10000 | 2 | 131.734 | 30001 | **35.501** | 119998 | 50000 | 69998 | 10000 | 281.681 | 75.910 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 175.537 | **98.816** | 1000 | 0 | 0 | 0 | 1000 | 10.120 | 5.697 | 1000 | True | 4 |
| 1000 | 1 | 173.243 | **100.446** | 1000 | 0 | 0 | 0 | 1000 | 9.956 | 5.772 | 1000 | True | 4 |
| 1000 | 2 | 128.679 | **97.207** | 1000 | 0 | 0 | 0 | 1000 | 10.287 | 7.771 | 1000 | True | 4 |
| 10000 | 0 | 153.595 | **71.534** | 10000 | 0 | 0 | 0 | 10000 | 139.794 | 65.106 | 10000 | True | 4 |
| 10000 | 1 | 167.266 | **69.666** | 10000 | 0 | 0 | 0 | 10000 | 143.543 | 59.785 | 10000 | True | 4 |
| 10000 | 2 | 180.598 | **69.017** | 10000 | 0 | 0 | 0 | 10000 | 144.892 | 55.372 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 12.463 | **80.239** | 6.161 | 2005 | 6.274 | 2000 | 1000 |
| 1000 | 1 | 12.774 | **78.281** | 6.319 | 2005 | 6.426 | 2000 | 1000 |
| 1000 | 2 | 14.178 | **70.532** | 6.866 | 2005 | 7.281 | 2000 | 1000 |
| 10000 | 0 | 106.407 | **93.979** | 57.538 | 20005 | 48.622 | 20000 | 10000 |
| 10000 | 1 | 110.599 | **90.417** | 59.247 | 20005 | 51.105 | 20000 | 10000 |
| 10000 | 2 | 96.565 | **103.557** | 50.141 | 20005 | 46.213 | 20000 | 10000 |

## Reproduce

```bash
docker compose up mariadb -d
bin/benchmark.py all --backend mariadb --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mariadb-20260527T043144Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mariadb-20260527T043144Z.jsonl --output docs/benchmarks/mariadb.md
```
