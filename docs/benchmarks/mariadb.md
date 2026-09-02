# dj_queue mariadb Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-09-02T21:52:04.735261+00:00

## Environment

- backend: `mariadb`
- database: `mysql` `dj_queue_benchmark`
- database version: `12.3.3-MariaDB-ubu2404`
- Python: `3.14.5`
- Django: `6.1`
- dj_queue: `0.14.0`
- platform: `macOS-26.6.2-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `2ae301b9176e`
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
| 1000 | 0 | 2.308 | 433.293 | 5 | **3.115** | 1000 | 1000 | 2.307 | 2.175 | 3.898 |
| 1000 | 1 | 2.296 | 435.448 | 5 | **3.000** | 1000 | 1000 | 2.296 | 2.186 | 3.791 |
| 1000 | 2 | 2.129 | 469.633 | 5 | **2.942** | 1000 | 1000 | 2.128 | 1.990 | 3.669 |
| 10000 | 0 | 26.763 | 373.650 | 5 | **4.746** | 10000 | 10000 | 2.675 | 2.230 | 6.356 |
| 10000 | 1 | 22.835 | 437.922 | 5 | **3.146** | 10000 | 10000 | 2.283 | 2.125 | 4.118 |
| 10000 | 2 | 21.819 | 458.325 | 5 | **2.909** | 10000 | 10000 | 2.181 | 2.074 | 3.668 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.129 | **7739.149** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.142 | **7062.729** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.169 | **5933.467** | 5 | 1000 | 1000 |
| 10000 | 0 | 1.262 | **7923.578** | 5 | 10000 | 10000 |
| 10000 | 1 | 1.226 | **8158.447** | 5 | 10000 | 10000 |
| 10000 | 2 | 1.141 | **8762.626** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.107 | **9369.967** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.089 | **11254.169** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.107 | **9361.546** | 1000 | 1000 | 1000 |
| 10000 | 0 | 0.977 | **10232.407** | 10000 | 10000 | 10000 |
| 10000 | 1 | 0.945 | **10584.954** | 10000 | 10000 | 10000 |
| 10000 | 2 | 0.928 | **10779.953** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | query_count | fired_count | recurring_task_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | **0.002** | 414865.975 | 1 | 0 | 1000 |
| 1000 | 1 | **0.002** | 425072.296 | 1 | 0 | 1000 |
| 1000 | 2 | **0.004** | 227654.307 | 1 | 0 | 1000 |
| 10000 | 0 | **0.008** | 1183099.425 | 1 | 0 | 10000 |
| 10000 | 1 | **0.007** | 1365692.599 | 1 | 0 | 10000 |
| 10000 | 2 | **0.008** | 1270553.910 | 1 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 0.994 | **1006.160** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 0.996 | **1004.260** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.039 | **962.552** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 9.661 | **1035.075** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 9.660 | **1035.240** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 9.558 | **1046.253** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 422.250 | 3001 | **75.039** | 12998 | 6000 | 6998 | 1000 | 13.326 | 2.368 |
| 1000 | 1 | 449.041 | 3001 | **71.711** | 12998 | 6000 | 6998 | 1000 | 13.945 | 2.227 |
| 1000 | 2 | 399.469 | 3001 | **73.864** | 12998 | 6000 | 6998 | 1000 | 13.538 | 2.503 |
| 10000 | 0 | 465.801 | 30001 | **66.145** | 129998 | 60000 | 69998 | 10000 | 151.184 | 21.468 |
| 10000 | 1 | 433.985 | 30001 | **64.127** | 129998 | 60000 | 69998 | 10000 | 155.940 | 23.042 |
| 10000 | 2 | 421.698 | 30001 | **62.016** | 129998 | 60000 | 69998 | 10000 | 161.249 | 23.714 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 436.238 | **133.690** | 1000 | 0 | 0 | 0 | 1000 | 7.480 | 2.292 | 1000 | True | 4 |
| 1000 | 1 | 444.196 | **142.021** | 1000 | 0 | 0 | 0 | 1000 | 7.041 | 2.251 | 1000 | True | 4 |
| 1000 | 2 | 438.131 | **141.658** | 1000 | 0 | 0 | 0 | 1000 | 7.059 | 2.282 | 1000 | True | 4 |
| 10000 | 0 | 448.592 | **131.654** | 10000 | 0 | 0 | 0 | 10000 | 75.957 | 22.292 | 10000 | True | 4 |
| 10000 | 1 | 449.912 | **132.444** | 10000 | 0 | 0 | 0 | 10000 | 75.503 | 22.227 | 10000 | True | 4 |
| 10000 | 2 | 443.441 | **130.261** | 10000 | 0 | 0 | 0 | 10000 | 76.769 | 22.551 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 6.410 | **156.017** | 2.727 | 2005 | 3.674 | 2000 | 1000 |
| 1000 | 1 | 5.935 | **168.480** | 2.588 | 2005 | 3.339 | 2000 | 1000 |
| 1000 | 2 | 5.428 | **184.227** | 2.340 | 2005 | 3.080 | 2000 | 1000 |
| 10000 | 0 | 65.171 | **153.443** | 27.828 | 20005 | 37.253 | 20000 | 10000 |
| 10000 | 1 | 65.780 | **152.021** | 27.808 | 20005 | 37.884 | 20000 | 10000 |
| 10000 | 2 | 62.540 | **159.898** | 26.867 | 20005 | 35.587 | 20000 | 10000 |

## Reproduce

```bash
docker compose up mariadb -d
bin/benchmark.py all --backend mariadb --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mariadb-20260902T212411Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mariadb-20260902T212411Z.jsonl --output docs/benchmarks/mariadb.md
```
