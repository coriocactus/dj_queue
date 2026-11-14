# dj_queue mariadb Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-11-14T22:15:37.479454+00:00

## Environment

- backend: `mariadb`
- database: `mysql` `dj_queue_benchmark`
- database version: `12.3.3-MariaDB-ubu2404`
- Python: `3.14.10`
- Django: `6.1.3`
- psycopg implementation: `None`
- dj_queue: `0.17.0`
- platform: `macOS-27.2.1-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `6c83e91ea8cf`
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
| 1000 | 0 | 3.452 | 289.697 | 4 | **4.581** | 1000 | 1000 | 3.449 | 3.450 | 6.315 |
| 1000 | 1 | 2.353 | 424.948 | 4 | **3.096** | 1000 | 1000 | 2.351 | 2.314 | 3.879 |
| 1000 | 2 | 2.211 | 452.191 | 4 | **2.747** | 1000 | 1000 | 2.209 | 2.236 | 3.478 |
| 10000 | 0 | 28.176 | 354.912 | 4 | **4.142** | 10000 | 10000 | 2.815 | 2.498 | 6.538 |
| 10000 | 1 | 24.884 | 401.871 | 4 | **3.526** | 10000 | 10000 | 2.486 | 2.402 | 5.248 |
| 10000 | 2 | 31.164 | 320.878 | 4 | **4.567** | 10000 | 10000 | 3.114 | 2.568 | 6.504 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.169 | **5917.570** | 4 | 1000 | 1000 |
| 1000 | 1 | 0.136 | **7331.390** | 4 | 1000 | 1000 |
| 1000 | 2 | 0.135 | **7407.481** | 4 | 1000 | 1000 |
| 10000 | 0 | 0.830 | **12043.867** | 4 | 10000 | 10000 |
| 10000 | 1 | 0.914 | **10939.517** | 4 | 10000 | 10000 |
| 10000 | 2 | 0.897 | **11151.154** | 4 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.066 | **15128.612** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.079 | **12645.023** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.085 | **11763.489** | 1000 | 1000 | 1000 |
| 10000 | 0 | 0.710 | **14091.168** | 10000 | 10000 | 10000 |
| 10000 | 1 | 0.728 | **13737.993** | 10000 | 10000 | 10000 |
| 10000 | 2 | 0.687 | **14549.116** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | query_count | fired_count | recurring_task_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | **0.001** | 809525.196 | 1 | 0 | 1000 |
| 1000 | 1 | **0.001** | 748946.973 | 1 | 0 | 1000 |
| 1000 | 2 | **0.001** | 741702.211 | 1 | 0 | 1000 |
| 10000 | 0 | **0.010** | 961873.730 | 1 | 0 | 10000 |
| 10000 | 1 | **0.011** | 937093.653 | 1 | 0 | 10000 |
| 10000 | 2 | **0.012** | 817809.203 | 1 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 0.513 | **1947.804** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 0.489 | **2046.146** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 0.514 | **1947.208** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 5.251 | **1904.413** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 4.662 | **2145.146** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 4.704 | **2125.810** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 368.326 | 3000 | **45.159** | 11999 | 6000 | 5999 | 1000 | 22.144 | 2.715 |
| 1000 | 1 | 388.369 | 3000 | **47.751** | 11999 | 6000 | 5999 | 1000 | 20.942 | 2.575 |
| 1000 | 2 | 248.568 | 3000 | **43.388** | 11999 | 6000 | 5999 | 1000 | 23.048 | 4.023 |
| 10000 | 0 | 277.222 | 30000 | **44.253** | 119999 | 60000 | 59999 | 10000 | 225.973 | 36.072 |
| 10000 | 1 | 249.208 | 30000 | **50.739** | 119999 | 60000 | 59999 | 10000 | 197.085 | 40.127 |
| 10000 | 2 | 386.296 | 30000 | **48.185** | 119999 | 60000 | 59999 | 10000 | 207.532 | 25.887 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 338.389 | **67.309** | 1000 | 0 | 0 | 0 | 1000 | 14.857 | 2.955 | 1000 | True | 4 |
| 1000 | 1 | 422.085 | **72.093** | 1000 | 0 | 0 | 0 | 1000 | 13.871 | 2.369 | 1000 | True | 4 |
| 1000 | 2 | 380.831 | **66.348** | 1000 | 0 | 0 | 0 | 1000 | 15.072 | 2.626 | 1000 | True | 4 |
| 10000 | 0 | 373.065 | **98.576** | 10000 | 0 | 0 | 0 | 10000 | 101.445 | 26.805 | 10000 | True | 4 |
| 10000 | 1 | 312.823 | **102.813** | 10000 | 0 | 0 | 0 | 10000 | 97.264 | 31.967 | 10000 | True | 4 |
| 10000 | 2 | 347.930 | **90.601** | 10000 | 0 | 0 | 0 | 10000 | 110.375 | 28.741 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 2.431 | **411.352** | 1.288 | 2005 | 1.138 | 2000 | 1000 |
| 1000 | 1 | 2.682 | **372.868** | 1.421 | 2005 | 1.256 | 2000 | 1000 |
| 1000 | 2 | 2.960 | **337.793** | 1.585 | 2005 | 1.370 | 2000 | 1000 |
| 10000 | 0 | 59.494 | **168.083** | 31.081 | 20005 | 28.297 | 20000 | 10000 |
| 10000 | 1 | 63.824 | **156.680** | 32.423 | 20005 | 31.278 | 20000 | 10000 |
| 10000 | 2 | 64.849 | **154.205** | 33.276 | 20005 | 31.445 | 20000 | 10000 |

## Reproduce

```bash
docker compose up mariadb -d
bin/benchmark.py all --backend mariadb --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mariadb-20261114T214040Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mariadb-20261114T214040Z.jsonl --output docs/benchmarks/mariadb.md
```
