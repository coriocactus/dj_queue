# dj_queue mysql Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-11-13T15:17:01.411357+00:00

## Environment

- backend: `mysql`
- database: `mysql` `dj_queue_benchmark`
- database version: `9.7.4`
- Python: `3.14.10`
- Django: `6.1.2`
- psycopg implementation: `None`
- dj_queue: `0.15.0`
- platform: `macOS-27.2.1-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `99e78cea0c28`
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
| 1000 | 0 | 2.419 | 413.355 | 4 | **3.446** | 1000 | 1000 | 2.418 | 2.390 | 5.237 |
| 1000 | 1 | 3.627 | 275.687 | 4 | **5.481** | 1000 | 1000 | 3.626 | 3.002 | 8.868 |
| 1000 | 2 | 2.323 | 430.400 | 4 | **3.352** | 1000 | 1000 | 2.322 | 2.257 | 5.317 |
| 10000 | 0 | 20.615 | 485.078 | 4 | **3.484** | 10000 | 10000 | 2.061 | 1.718 | 5.889 |
| 10000 | 1 | 21.002 | 476.149 | 4 | **3.192** | 10000 | 10000 | 2.099 | 1.814 | 5.198 |
| 10000 | 2 | 26.919 | 371.488 | 4 | **4.219** | 10000 | 10000 | 2.690 | 2.571 | 6.773 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.154 | **6495.554** | 4 | 1000 | 1000 |
| 1000 | 1 | 0.126 | **7956.657** | 4 | 1000 | 1000 |
| 1000 | 2 | 0.142 | **7055.472** | 4 | 1000 | 1000 |
| 10000 | 0 | 0.999 | **10012.942** | 4 | 10000 | 10000 |
| 10000 | 1 | 1.018 | **9825.746** | 4 | 10000 | 10000 |
| 10000 | 2 | 0.995 | **10049.819** | 4 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.074 | **13570.063** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.086 | **11679.133** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.082 | **12245.704** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.021 | **9797.140** | 10000 | 10000 | 10000 |
| 10000 | 1 | 0.766 | **13051.736** | 10000 | 10000 | 10000 |
| 10000 | 2 | 0.726 | **13779.099** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | query_count | fired_count | recurring_task_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | **0.002** | 654503.801 | 1 | 0 | 1000 |
| 1000 | 1 | **0.001** | 696257.619 | 1 | 0 | 1000 |
| 1000 | 2 | **0.001** | 726832.405 | 1 | 0 | 1000 |
| 10000 | 0 | **0.008** | 1331395.354 | 1 | 0 | 10000 |
| 10000 | 1 | **0.007** | 1396290.391 | 1 | 0 | 10000 |
| 10000 | 2 | **0.008** | 1327734.719 | 1 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 0.664 | **1506.684** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 0.721 | **1386.550** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 0.683 | **1463.945** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 6.461 | **1547.800** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 5.718 | **1748.977** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 5.812 | **1720.482** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 536.494 | 3000 | **149.928** | 11999 | 6000 | 5999 | 1000 | 6.670 | 1.864 |
| 1000 | 1 | 250.607 | 3000 | **133.009** | 11999 | 6000 | 5999 | 1000 | 7.518 | 3.990 |
| 1000 | 2 | 372.711 | 3000 | **75.695** | 11999 | 6000 | 5999 | 1000 | 13.211 | 2.683 |
| 10000 | 0 | 469.553 | 30000 | **72.577** | 119999 | 60000 | 59999 | 10000 | 137.784 | 21.297 |
| 10000 | 1 | 335.247 | 30000 | **55.849** | 119999 | 60000 | 59999 | 10000 | 179.055 | 29.829 |
| 10000 | 2 | 331.241 | 30000 | **58.011** | 119999 | 60000 | 59999 | 10000 | 172.380 | 30.189 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 195.453 | **111.104** | 1000 | 0 | 0 | 0 | 1000 | 9.001 | 5.116 | 1000 | True | 4 |
| 1000 | 1 | 342.822 | **122.492** | 1000 | 0 | 0 | 0 | 1000 | 8.164 | 2.917 | 1000 | True | 4 |
| 1000 | 2 | 344.370 | **139.514** | 1000 | 0 | 0 | 0 | 1000 | 7.168 | 2.904 | 1000 | True | 4 |
| 10000 | 0 | 330.647 | **84.794** | 10000 | 0 | 0 | 0 | 10000 | 117.932 | 30.244 | 10000 | True | 4 |
| 10000 | 1 | 259.667 | **79.215** | 10000 | 0 | 0 | 0 | 10000 | 126.238 | 38.511 | 10000 | True | 4 |
| 10000 | 2 | 343.017 | **88.668** | 10000 | 0 | 0 | 0 | 10000 | 112.781 | 29.153 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 3.207 | **311.854** | 1.476 | 2005 | 1.725 | 2000 | 1000 |
| 1000 | 1 | 3.794 | **263.551** | 1.536 | 2005 | 2.254 | 2000 | 1000 |
| 1000 | 2 | 3.080 | **324.706** | 1.272 | 2005 | 1.803 | 2000 | 1000 |
| 10000 | 0 | 45.458 | **219.982** | 18.195 | 20005 | 27.212 | 20000 | 10000 |
| 10000 | 1 | 30.619 | **326.592** | 11.520 | 20005 | 19.063 | 20000 | 10000 |
| 10000 | 2 | 42.792 | **233.689** | 14.896 | 20005 | 27.859 | 20000 | 10000 |

## Reproduce

```bash
docker compose up mysql -d
bin/benchmark.py all --backend mysql --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mysql-20261113T144740Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mysql-20261113T144740Z.jsonl --output docs/benchmarks/mysql.md
```
