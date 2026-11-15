# dj_queue mariadb Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-11-15T14:34:18.529161+00:00

## Environment

- backend: `mariadb`
- database: `mysql` `dj_queue_benchmark`
- database version: `12.3.3-MariaDB-ubu2404`
- Python: `3.14.10`
- Django: `6.1.3`
- psycopg implementation: `None`
- dj_queue: `0.19.0`
- platform: `macOS-27.2.1-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `ce20c0fc737b`
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
| 1000 | 0 | 2.209 | 452.616 | 4 | **2.680** | 1000 | 1000 | 2.208 | 2.080 | 3.663 |
| 1000 | 1 | 1.910 | 523.690 | 4 | **2.578** | 1000 | 1000 | 1.908 | 1.759 | 3.840 |
| 1000 | 2 | 1.632 | 612.665 | 4 | **2.003** | 1000 | 1000 | 1.631 | 1.584 | 2.561 |
| 10000 | 0 | 17.992 | 555.795 | 4 | **2.519** | 10000 | 10000 | 1.798 | 1.643 | 3.862 |
| 10000 | 1 | 18.102 | 552.416 | 4 | **2.561** | 10000 | 10000 | 1.809 | 1.715 | 2.979 |
| 10000 | 2 | 22.178 | 450.906 | 4 | **2.752** | 10000 | 10000 | 2.216 | 2.220 | 3.104 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.110 | **9129.832** | 4 | 1000 | 1000 |
| 1000 | 1 | 0.094 | **10660.967** | 4 | 1000 | 1000 |
| 1000 | 2 | 0.088 | **11418.675** | 4 | 1000 | 1000 |
| 10000 | 0 | 0.889 | **11242.738** | 4 | 10000 | 10000 |
| 10000 | 1 | 0.891 | **11218.262** | 4 | 10000 | 10000 |
| 10000 | 2 | 0.863 | **11586.763** | 4 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.056 | **17713.785** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.086 | **11654.259** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.060 | **16773.985** | 1000 | 1000 | 1000 |
| 10000 | 0 | 0.642 | **15573.873** | 10000 | 10000 | 10000 |
| 10000 | 1 | 0.660 | **15160.257** | 10000 | 10000 | 10000 |
| 10000 | 2 | 0.688 | **14526.234** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | query_count | fired_count | recurring_task_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | **0.001** | 727118.318 | 1 | 0 | 1000 |
| 1000 | 1 | **0.001** | 776146.829 | 1 | 0 | 1000 |
| 1000 | 2 | **0.001** | 785879.944 | 1 | 0 | 1000 |
| 10000 | 0 | **0.012** | 830421.116 | 1 | 0 | 10000 |
| 10000 | 1 | **0.008** | 1265275.678 | 1 | 0 | 10000 |
| 10000 | 2 | **0.011** | 891186.169 | 1 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 0.534 | **1871.756** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 0.542 | **1845.242** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 0.536 | **1863.936** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 4.910 | **2036.834** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 5.014 | **1994.578** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 5.163 | **1937.044** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 267.022 | 3000 | **55.533** | 11999 | 6000 | 5999 | 1000 | 18.007 | 3.745 |
| 1000 | 1 | 480.090 | 3000 | **109.861** | 11999 | 6000 | 5999 | 1000 | 9.102 | 2.083 |
| 1000 | 2 | 509.074 | 3000 | **76.295** | 11999 | 6000 | 5999 | 1000 | 13.107 | 1.964 |
| 10000 | 0 | 565.142 | 30000 | **74.052** | 119999 | 60000 | 59999 | 10000 | 135.040 | 17.695 |
| 10000 | 1 | 708.369 | 30000 | **89.624** | 119999 | 60000 | 59999 | 10000 | 111.577 | 14.117 |
| 10000 | 2 | 364.034 | 30000 | **112.955** | 119999 | 60000 | 59999 | 10000 | 88.531 | 27.470 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 634.949 | **206.491** | 1000 | 0 | 0 | 0 | 1000 | 4.843 | 1.575 | 1000 | True | 4 |
| 1000 | 1 | 750.164 | **199.352** | 1000 | 0 | 0 | 0 | 1000 | 5.016 | 1.333 | 1000 | True | 4 |
| 1000 | 2 | 474.608 | **205.181** | 1000 | 0 | 0 | 0 | 1000 | 4.874 | 2.107 | 1000 | True | 4 |
| 10000 | 0 | 540.312 | **201.370** | 10000 | 0 | 0 | 0 | 10000 | 49.660 | 18.508 | 10000 | True | 4 |
| 10000 | 1 | 350.263 | **151.642** | 10000 | 0 | 0 | 0 | 10000 | 65.945 | 28.550 | 10000 | True | 4 |
| 10000 | 2 | 557.884 | **160.407** | 10000 | 0 | 0 | 0 | 10000 | 62.341 | 17.925 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 8.371 | **119.459** | 3.415 | 2005 | 4.945 | 2000 | 1000 |
| 1000 | 1 | 4.509 | **221.798** | 2.290 | 2005 | 2.209 | 2000 | 1000 |
| 1000 | 2 | 4.893 | **204.394** | 2.557 | 2005 | 2.326 | 2000 | 1000 |
| 10000 | 0 | 36.514 | **273.868** | 16.430 | 20005 | 20.025 | 20000 | 10000 |
| 10000 | 1 | 48.159 | **207.645** | 24.592 | 20005 | 23.479 | 20000 | 10000 |
| 10000 | 2 | 21.144 | **472.942** | 10.214 | 20005 | 10.895 | 20000 | 10000 |

## Reproduce

```bash
docker compose up mariadb -d
bin/benchmark.py all --backend mariadb --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mariadb-20261115T141233Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mariadb-20261115T141233Z.jsonl --output docs/benchmarks/mariadb.md
```
