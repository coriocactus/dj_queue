# dj_queue PostgreSQL Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-25T14:19:48.772192+00:00

## Environment

- backend: `postgres`
- database: `postgresql` `dj_queue_benchmark`
- database version: `PostgreSQL 17.9 (Debian 17.9-1.pgdg13+1) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit`
- Python: `3.14.4`
- Django: `6.0.5`
- dj_queue: `0.10.3`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `1a65bc8ef066`
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
| 1000 | 0 | 7.378 | 135.538 | 5 | **11.927** | 1000 | 1000 | 7.374 | 6.816 | 13.396 |
| 1000 | 1 | 7.457 | 134.108 | 5 | **11.913** | 1000 | 1000 | 7.453 | 6.829 | 13.751 |
| 1000 | 2 | 7.058 | 141.688 | 5 | **12.047** | 1000 | 1000 | 7.054 | 6.553 | 13.189 |
| 10000 | 0 | 72.140 | 138.620 | 5 | **12.188** | 10000 | 10000 | 7.210 | 6.792 | 14.432 |
| 10000 | 1 | 76.054 | 131.486 | 5 | **12.304** | 10000 | 10000 | 7.602 | 7.066 | 14.051 |
| 10000 | 2 | 83.545 | 119.697 | 5 | **14.778** | 10000 | 10000 | 8.349 | 7.963 | 18.877 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.140 | **7118.970** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.121 | **8238.929** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.121 | **8291.691** | 5 | 1000 | 1000 |
| 10000 | 0 | 0.897 | **11142.892** | 5 | 10000 | 10000 |
| 10000 | 1 | 0.905 | **11054.226** | 5 | 10000 | 10000 |
| 10000 | 2 | 0.918 | **10887.401** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.138 | **7267.143** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.108 | **9266.012** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.136 | **7346.670** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.312 | **7622.324** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.298 | **7704.451** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.311 | **7628.475** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.006** | 181020.048 | 0 | 1000 |
| 1000 | 1 | **0.005** | 188133.481 | 0 | 1000 |
| 1000 | 2 | **0.006** | 179753.726 | 0 | 1000 |
| 10000 | 0 | **0.013** | 755745.994 | 0 | 10000 |
| 10000 | 1 | **0.009** | 1061655.651 | 0 | 10000 |
| 10000 | 2 | **0.014** | 708317.682 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.633 | **612.544** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.650 | **606.053** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.672 | **597.991** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 15.899 | **628.959** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 15.743 | **635.205** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 15.596 | **641.177** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 125.340 | 4999 | **44.177** | 8999 | 4000 | 4999 | 1000 | 22.636 | 7.978 |
| 1000 | 1 | 117.685 | 4999 | **44.141** | 8999 | 4000 | 4999 | 1000 | 22.655 | 8.497 |
| 1000 | 2 | 85.284 | 4999 | **42.764** | 8999 | 4000 | 4999 | 1000 | 23.384 | 11.726 |
| 10000 | 0 | 91.960 | 49999 | **47.707** | 89999 | 40000 | 49999 | 10000 | 209.612 | 108.743 |
| 10000 | 1 | 104.466 | 49999 | **49.978** | 89999 | 40000 | 49999 | 10000 | 200.087 | 95.725 |
| 10000 | 2 | 116.224 | 49999 | **50.460** | 89999 | 40000 | 49999 | 10000 | 198.176 | 86.041 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 170.068 | **82.457** | 1000 | 0 | 0 | 0 | 1000 | 12.127 | 5.880 | 1000 | True | 4 |
| 1000 | 1 | 112.521 | **79.320** | 1000 | 0 | 0 | 0 | 1000 | 12.607 | 8.887 | 1000 | True | 4 |
| 1000 | 2 | 107.236 | **81.091** | 1000 | 0 | 0 | 0 | 1000 | 12.332 | 9.325 | 1000 | True | 4 |
| 10000 | 0 | 104.000 | **78.566** | 10000 | 0 | 0 | 0 | 10000 | 127.282 | 96.154 | 10000 | True | 4 |
| 10000 | 1 | 94.315 | **75.395** | 10000 | 0 | 0 | 0 | 10000 | 132.634 | 106.027 | 10000 | True | 4 |
| 10000 | 2 | 97.897 | **79.670** | 10000 | 0 | 0 | 0 | 10000 | 125.518 | 102.148 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 8.244 | **121.300** | 3.768 | 1671 | 4.459 | 1000 | 1000 |
| 1000 | 1 | 7.212 | **138.665** | 3.254 | 1671 | 3.946 | 1000 | 1000 |
| 1000 | 2 | 7.891 | **126.730** | 3.516 | 1671 | 4.361 | 1000 | 1000 |
| 10000 | 0 | 90.719 | **110.231** | 42.445 | 16671 | 48.060 | 10000 | 10000 |
| 10000 | 1 | 83.642 | **119.557** | 40.625 | 16671 | 42.810 | 10000 | 10000 |
| 10000 | 2 | 89.416 | **111.836** | 41.686 | 16671 | 47.525 | 10000 | 10000 |

## Reproduce

```bash
docker compose up postgres -d
bin/benchmark.py all --backend postgres --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/postgres-20260525T132700Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/postgres-20260525T132700Z.jsonl --output docs/benchmarks/postgres.md
```
