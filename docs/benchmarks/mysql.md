# dj_queue mysql Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-06-03T07:34:27.199078+00:00

## Environment

- backend: `mysql`
- database: `mysql` `dj_queue_benchmark`
- database version: `8.4.8`
- Python: `3.14.5`
- Django: `6.0.5`
- dj_queue: `0.11.0`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `cb4d0997597c`
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
| 1000 | 0 | 4.348 | 229.996 | 5 | **5.919** | 1000 | 1000 | 4.346 | 4.100 | 7.755 |
| 1000 | 1 | 4.218 | 237.074 | 5 | **5.773** | 1000 | 1000 | 4.216 | 4.019 | 6.882 |
| 1000 | 2 | 4.241 | 235.785 | 5 | **6.036** | 1000 | 1000 | 4.240 | 3.996 | 7.388 |
| 10000 | 0 | 50.656 | 197.411 | 5 | **9.414** | 10000 | 10000 | 5.064 | 4.181 | 15.337 |
| 10000 | 1 | 46.036 | 217.222 | 5 | **7.021** | 10000 | 10000 | 4.602 | 4.052 | 11.978 |
| 10000 | 2 | 50.591 | 197.664 | 5 | **9.897** | 10000 | 10000 | 5.057 | 4.092 | 14.986 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.359 | **2787.999** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.307 | **3253.871** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.246 | **4065.435** | 5 | 1000 | 1000 |
| 10000 | 0 | 1.534 | **6517.376** | 5 | 10000 | 10000 |
| 10000 | 1 | 1.397 | **7156.925** | 5 | 10000 | 10000 |
| 10000 | 2 | 1.550 | **6450.342** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.167 | **6003.173** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.172 | **5815.003** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.193 | **5180.285** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.097 | **9116.695** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.005 | **9947.661** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.094 | **9140.350** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.007** | 146084.035 | 0 | 1000 |
| 1000 | 1 | **0.008** | 130884.348 | 0 | 1000 |
| 1000 | 2 | **0.012** | 84934.706 | 0 | 1000 |
| 10000 | 0 | **0.022** | 448222.804 | 0 | 10000 |
| 10000 | 1 | **0.022** | 452368.431 | 0 | 10000 |
| 10000 | 2 | **0.023** | 426809.310 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.414 | **707.300** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.544 | **647.728** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.411 | **708.796** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 13.529 | **739.146** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 14.205 | **703.984** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 13.640 | **733.156** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 253.408 | 3001 | **86.484** | 11998 | 5000 | 6998 | 1000 | 11.563 | 3.946 |
| 1000 | 1 | 234.540 | 3001 | **61.601** | 11998 | 5000 | 6998 | 1000 | 16.234 | 4.264 |
| 1000 | 2 | 257.160 | 3001 | **75.853** | 11998 | 5000 | 6998 | 1000 | 13.183 | 3.889 |
| 10000 | 0 | 225.842 | 30001 | **82.472** | 119998 | 50000 | 69998 | 10000 | 121.254 | 44.279 |
| 10000 | 1 | 234.175 | 30001 | **80.845** | 119998 | 50000 | 69998 | 10000 | 123.694 | 42.703 |
| 10000 | 2 | 221.732 | 30001 | **80.347** | 119998 | 50000 | 69998 | 10000 | 124.459 | 45.100 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 269.813 | **90.859** | 1000 | 0 | 0 | 0 | 1000 | 11.006 | 3.706 | 1000 | True | 4 |
| 1000 | 1 | 254.048 | **73.566** | 1000 | 0 | 0 | 0 | 1000 | 13.593 | 3.936 | 1000 | True | 4 |
| 1000 | 2 | 229.656 | **90.633** | 1000 | 0 | 0 | 0 | 1000 | 11.033 | 4.354 | 1000 | True | 4 |
| 10000 | 0 | 235.377 | **81.085** | 10000 | 0 | 0 | 0 | 10000 | 123.328 | 42.485 | 10000 | True | 4 |
| 10000 | 1 | 232.529 | **83.114** | 10000 | 0 | 0 | 0 | 10000 | 120.316 | 43.005 | 10000 | True | 4 |
| 10000 | 2 | 225.149 | **81.011** | 10000 | 0 | 0 | 0 | 10000 | 123.440 | 44.415 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 6.319 | **158.259** | 2.825 | 1670 | 3.483 | 2000 | 1000 |
| 1000 | 1 | 6.740 | **148.371** | 2.944 | 1670 | 3.785 | 2000 | 1000 |
| 1000 | 2 | 5.908 | **169.249** | 2.699 | 1670 | 3.199 | 2000 | 1000 |
| 10000 | 0 | 88.737 | **112.693** | 46.511 | 16670 | 42.099 | 20000 | 10000 |
| 10000 | 1 | 84.366 | **118.532** | 45.002 | 16670 | 39.243 | 20000 | 10000 |
| 10000 | 2 | 90.656 | **110.307** | 47.052 | 16670 | 43.479 | 20000 | 10000 |

## Reproduce

```bash
docker compose up mysql -d
bin/benchmark.py all --backend mysql --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mysql-20260603T065747Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mysql-20260603T065747Z.jsonl --output docs/benchmarks/mysql.md
```
