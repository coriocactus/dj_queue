# dj_queue mysql Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-06-09T14:54:32.420221+00:00

## Environment

- backend: `mysql`
- database: `mysql` `dj_queue_benchmark`
- database version: `8.4.8`
- Python: `3.14.5`
- Django: `6.0.5`
- dj_queue: `0.12.0`
- platform: `macOS-26.5.1-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `cef37cdd23be`
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
| 1000 | 0 | 4.364 | 229.149 | 5 | **6.637** | 1000 | 1000 | 4.362 | 4.154 | 8.940 |
| 1000 | 1 | 4.603 | 217.259 | 5 | **7.466** | 1000 | 1000 | 4.601 | 4.289 | 10.299 |
| 1000 | 2 | 4.486 | 222.914 | 5 | **6.701** | 1000 | 1000 | 4.484 | 4.195 | 9.036 |
| 10000 | 0 | 43.514 | 229.809 | 5 | **6.860** | 10000 | 10000 | 4.350 | 3.896 | 12.718 |
| 10000 | 1 | 44.674 | 223.842 | 5 | **7.612** | 10000 | 10000 | 4.466 | 3.957 | 12.520 |
| 10000 | 2 | 41.629 | 240.215 | 5 | **5.643** | 10000 | 10000 | 4.162 | 3.936 | 7.707 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.166 | **6030.611** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.244 | **4103.893** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.183 | **5455.854** | 5 | 1000 | 1000 |
| 10000 | 0 | 1.645 | **6077.897** | 5 | 10000 | 10000 |
| 10000 | 1 | 1.344 | **7441.813** | 5 | 10000 | 10000 |
| 10000 | 2 | 1.545 | **6473.333** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.149 | **6699.605** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.157 | **6357.889** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.136 | **7328.761** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.142 | **8758.129** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.231 | **8126.078** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.079 | **9266.068** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | query_count | fired_count | recurring_task_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | **0.001** | 1334149.867 | 1 | 0 | 1000 |
| 1000 | 1 | **0.002** | 659884.370 | 1 | 0 | 1000 |
| 1000 | 2 | **0.001** | 781911.562 | 1 | 0 | 1000 |
| 10000 | 0 | **0.001** | 13056961.301 | 1 | 0 | 10000 |
| 10000 | 1 | **0.001** | 14082840.014 | 1 | 0 | 10000 |
| 10000 | 2 | **0.001** | 13724481.245 | 1 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.224 | **816.748** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.223 | **817.442** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.266 | **790.129** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 10.018 | **998.215** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 10.106 | **989.549** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 9.914 | **1008.632** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 224.725 | 3001 | **67.843** | 11998 | 5000 | 6998 | 1000 | 14.740 | 4.450 |
| 1000 | 1 | 213.549 | 3001 | **75.080** | 11998 | 5000 | 6998 | 1000 | 13.319 | 4.683 |
| 1000 | 2 | 211.358 | 3001 | **89.256** | 11998 | 5000 | 6998 | 1000 | 11.204 | 4.731 |
| 10000 | 0 | 217.871 | 30001 | **63.005** | 119998 | 50000 | 69998 | 10000 | 158.717 | 45.899 |
| 10000 | 1 | 201.015 | 30001 | **63.650** | 119998 | 50000 | 69998 | 10000 | 157.108 | 49.747 |
| 10000 | 2 | 242.737 | 30001 | **88.374** | 119998 | 50000 | 69998 | 10000 | 113.156 | 41.197 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 259.708 | **124.157** | 1000 | 0 | 0 | 0 | 1000 | 8.054 | 3.850 | 1000 | True | 4 |
| 1000 | 1 | 247.383 | **134.323** | 1000 | 0 | 0 | 0 | 1000 | 7.445 | 4.042 | 1000 | True | 4 |
| 1000 | 2 | 236.791 | **127.071** | 1000 | 0 | 0 | 0 | 1000 | 7.870 | 4.223 | 1000 | True | 4 |
| 10000 | 0 | 208.631 | **117.346** | 10000 | 0 | 0 | 0 | 10000 | 85.218 | 47.932 | 10000 | True | 4 |
| 10000 | 1 | 240.521 | **112.472** | 10000 | 0 | 0 | 0 | 10000 | 88.911 | 41.576 | 10000 | True | 4 |
| 10000 | 2 | 212.567 | **110.601** | 10000 | 0 | 0 | 0 | 10000 | 90.415 | 47.044 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 5.492 | **182.081** | 2.325 | 2005 | 3.158 | 2000 | 1000 |
| 1000 | 1 | 7.712 | **129.661** | 3.094 | 2005 | 4.607 | 2000 | 1000 |
| 1000 | 2 | 5.430 | **184.147** | 2.393 | 2005 | 3.028 | 2000 | 1000 |
| 10000 | 0 | 65.278 | **153.191** | 28.168 | 20005 | 36.999 | 20000 | 10000 |
| 10000 | 1 | 70.963 | **140.918** | 31.578 | 20005 | 39.249 | 20000 | 10000 |
| 10000 | 2 | 61.239 | **163.295** | 27.531 | 20005 | 33.600 | 20000 | 10000 |

## Reproduce

```bash
docker compose up mysql -d
bin/benchmark.py all --backend mysql --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mysql-20260609T142128Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mysql-20260609T142128Z.jsonl --output docs/benchmarks/mysql.md
```
