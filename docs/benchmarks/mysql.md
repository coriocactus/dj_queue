# dj_queue mysql Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-09-02T22:22:31.032206+00:00

## Environment

- backend: `mysql`
- database: `mysql` `dj_queue_benchmark`
- database version: `9.7.2`
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
| 1000 | 0 | 4.320 | 231.500 | 5 | **6.801** | 1000 | 1000 | 4.318 | 3.997 | 9.291 |
| 1000 | 1 | 3.845 | 260.091 | 5 | **6.160** | 1000 | 1000 | 3.843 | 3.382 | 10.541 |
| 1000 | 2 | 4.707 | 212.434 | 5 | **8.611** | 1000 | 1000 | 4.706 | 4.001 | 11.222 |
| 10000 | 0 | 36.375 | 274.912 | 5 | **6.350** | 10000 | 10000 | 3.636 | 3.192 | 10.299 |
| 10000 | 1 | 33.559 | 297.987 | 5 | **5.589** | 10000 | 10000 | 3.355 | 2.972 | 9.106 |
| 10000 | 2 | 36.693 | 272.534 | 5 | **6.592** | 10000 | 10000 | 3.668 | 3.176 | 10.741 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.238 | **4204.262** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.383 | **2612.106** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.405 | **2466.797** | 5 | 1000 | 1000 |
| 10000 | 0 | 1.413 | **7075.170** | 5 | 10000 | 10000 |
| 10000 | 1 | 1.429 | **6996.658** | 5 | 10000 | 10000 |
| 10000 | 2 | 1.439 | **6949.151** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.138 | **7261.393** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.136 | **7374.606** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.243 | **4110.757** | 1000 | 1000 | 1000 |
| 10000 | 0 | 0.973 | **10279.629** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.141 | **8766.763** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.071 | **9336.504** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | query_count | fired_count | recurring_task_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | **0.005** | 182134.168 | 1 | 0 | 1000 |
| 1000 | 1 | **0.002** | 450416.569 | 1 | 0 | 1000 |
| 1000 | 2 | **0.002** | 439955.337 | 1 | 0 | 1000 |
| 10000 | 0 | **0.011** | 939551.600 | 1 | 0 | 10000 |
| 10000 | 1 | **0.011** | 922863.898 | 1 | 0 | 10000 |
| 10000 | 2 | **0.011** | 935515.742 | 1 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 0.960 | **1041.672** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 0.998 | **1001.570** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.086 | **920.605** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 9.327 | **1072.128** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 9.344 | **1070.261** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 9.434 | **1059.948** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 313.509 | 3001 | **63.848** | 12998 | 6000 | 6998 | 1000 | 15.662 | 3.190 |
| 1000 | 1 | 317.518 | 3001 | **67.224** | 12998 | 6000 | 6998 | 1000 | 14.876 | 3.149 |
| 1000 | 2 | 343.476 | 3001 | **61.325** | 12998 | 6000 | 6998 | 1000 | 16.307 | 2.911 |
| 10000 | 0 | 303.512 | 30001 | **61.577** | 129998 | 60000 | 69998 | 10000 | 162.400 | 32.948 |
| 10000 | 1 | 303.412 | 30001 | **60.780** | 129998 | 60000 | 69998 | 10000 | 164.527 | 32.958 |
| 10000 | 2 | 328.671 | 30001 | **62.952** | 129998 | 60000 | 69998 | 10000 | 158.850 | 30.426 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 325.834 | **143.884** | 1000 | 0 | 0 | 0 | 1000 | 6.950 | 3.069 | 1000 | True | 4 |
| 1000 | 1 | 324.570 | **140.815** | 1000 | 0 | 0 | 0 | 1000 | 7.102 | 3.081 | 1000 | True | 4 |
| 1000 | 2 | 324.141 | **150.365** | 1000 | 0 | 0 | 0 | 1000 | 6.650 | 3.085 | 1000 | True | 4 |
| 10000 | 0 | 328.355 | **128.992** | 10000 | 0 | 0 | 0 | 10000 | 77.524 | 30.455 | 10000 | True | 4 |
| 10000 | 1 | 331.849 | **128.051** | 10000 | 0 | 0 | 0 | 10000 | 78.094 | 30.134 | 10000 | True | 4 |
| 10000 | 2 | 331.817 | **128.685** | 10000 | 0 | 0 | 0 | 10000 | 77.709 | 30.137 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 4.420 | **226.231** | 1.839 | 2005 | 2.575 | 2000 | 1000 |
| 1000 | 1 | 6.184 | **161.717** | 2.469 | 2005 | 3.707 | 2000 | 1000 |
| 1000 | 2 | 4.867 | **205.467** | 2.004 | 2005 | 2.856 | 2000 | 1000 |
| 10000 | 0 | 52.331 | **191.093** | 21.541 | 20005 | 30.710 | 20000 | 10000 |
| 10000 | 1 | 50.441 | **198.250** | 20.558 | 20005 | 29.810 | 20000 | 10000 |
| 10000 | 2 | 47.676 | **209.751** | 19.818 | 20005 | 27.787 | 20000 | 10000 |

## Reproduce

```bash
docker compose up mysql -d
bin/benchmark.py all --backend mysql --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mysql-20260902T215204Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mysql-20260902T215204Z.jsonl --output docs/benchmarks/mysql.md
```
