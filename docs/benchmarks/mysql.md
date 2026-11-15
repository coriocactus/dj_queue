# dj_queue mysql Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-11-15T15:03:18.611345+00:00

## Environment

- backend: `mysql`
- database: `mysql` `dj_queue_benchmark`
- database version: `9.7.4`
- Python: `3.14.10`
- Django: `6.1.3`
- psycopg implementation: `None`
- dj_queue: `0.19.0`
- platform: `macOS-27.2.1-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `ce03924126a0`
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
| 1000 | 0 | 1.553 | 644.055 | 4 | **2.051** | 1000 | 1000 | 1.552 | 1.426 | 3.571 |
| 1000 | 1 | 1.661 | 602.083 | 4 | **2.420** | 1000 | 1000 | 1.660 | 1.531 | 4.351 |
| 1000 | 2 | 1.852 | 539.824 | 4 | **2.814** | 1000 | 1000 | 1.851 | 1.702 | 4.560 |
| 10000 | 0 | 26.647 | 375.271 | 4 | **3.572** | 10000 | 10000 | 2.663 | 2.510 | 5.906 |
| 10000 | 1 | 29.269 | 341.664 | 4 | **4.892** | 10000 | 10000 | 2.925 | 2.643 | 7.242 |
| 10000 | 2 | 26.723 | 374.210 | 4 | **3.919** | 10000 | 10000 | 2.671 | 2.521 | 6.757 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.219 | **4557.387** | 4 | 1000 | 1000 |
| 1000 | 1 | 0.223 | **4480.888** | 4 | 1000 | 1000 |
| 1000 | 2 | 0.239 | **4184.236** | 4 | 1000 | 1000 |
| 10000 | 0 | 1.095 | **9134.323** | 4 | 10000 | 10000 |
| 10000 | 1 | 1.135 | **8812.336** | 4 | 10000 | 10000 |
| 10000 | 2 | 1.041 | **9602.412** | 4 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.094 | **10676.195** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.098 | **10159.195** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.085 | **11713.831** | 1000 | 1000 | 1000 |
| 10000 | 0 | 0.745 | **13427.068** | 10000 | 10000 | 10000 |
| 10000 | 1 | 0.695 | **14386.764** | 10000 | 10000 | 10000 |
| 10000 | 2 | 0.719 | **13912.360** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | query_count | fired_count | recurring_task_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | **0.002** | 474580.291 | 1 | 0 | 1000 |
| 1000 | 1 | **0.002** | 447811.387 | 1 | 0 | 1000 |
| 1000 | 2 | **0.001** | 769822.943 | 1 | 0 | 1000 |
| 10000 | 0 | **0.007** | 1420017.444 | 1 | 0 | 10000 |
| 10000 | 1 | **0.007** | 1466311.563 | 1 | 0 | 10000 |
| 10000 | 2 | **0.007** | 1347724.155 | 1 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 0.562 | **1779.452** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 0.556 | **1799.063** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 0.965 | **1035.863** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 5.291 | **1890.040** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 5.193 | **1925.791** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 7.914 | **1263.582** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 568.156 | 3000 | **127.612** | 11999 | 6000 | 5999 | 1000 | 7.836 | 1.760 |
| 1000 | 1 | 416.065 | 3000 | **176.336** | 11999 | 6000 | 5999 | 1000 | 5.671 | 2.403 |
| 1000 | 2 | 417.162 | 3000 | **67.759** | 11999 | 6000 | 5999 | 1000 | 14.758 | 2.397 |
| 10000 | 0 | 258.133 | 30000 | **62.916** | 119999 | 60000 | 59999 | 10000 | 158.942 | 38.740 |
| 10000 | 1 | 346.089 | 30000 | **75.422** | 119999 | 60000 | 59999 | 10000 | 132.587 | 28.894 |
| 10000 | 2 | 323.728 | 30000 | **77.357** | 119999 | 60000 | 59999 | 10000 | 129.270 | 30.890 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 380.018 | **117.069** | 1000 | 0 | 0 | 0 | 1000 | 8.542 | 2.631 | 1000 | True | 4 |
| 1000 | 1 | 311.085 | **60.626** | 1000 | 0 | 0 | 0 | 1000 | 16.494 | 3.215 | 1000 | True | 4 |
| 1000 | 2 | 296.074 | **68.853** | 1000 | 0 | 0 | 0 | 1000 | 14.524 | 3.378 | 1000 | True | 4 |
| 10000 | 0 | 293.047 | **83.939** | 10000 | 0 | 0 | 0 | 10000 | 119.134 | 34.124 | 10000 | True | 4 |
| 10000 | 1 | 379.358 | **102.711** | 10000 | 0 | 0 | 0 | 10000 | 97.361 | 26.360 | 10000 | True | 4 |
| 10000 | 2 | 326.587 | **91.629** | 10000 | 0 | 0 | 0 | 10000 | 109.135 | 30.620 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 5.255 | **190.289** | 1.874 | 2005 | 3.375 | 2000 | 1000 |
| 1000 | 1 | 3.120 | **320.511** | 1.404 | 2005 | 1.710 | 2000 | 1000 |
| 1000 | 2 | 4.808 | **207.979** | 2.193 | 2005 | 2.606 | 2000 | 1000 |
| 10000 | 0 | 41.612 | **240.317** | 17.612 | 20005 | 23.930 | 20000 | 10000 |
| 10000 | 1 | 48.816 | **204.852** | 22.624 | 20005 | 26.116 | 20000 | 10000 |
| 10000 | 2 | 36.149 | **276.636** | 16.434 | 20005 | 19.655 | 20000 | 10000 |

## Reproduce

```bash
docker compose up mysql -d
bin/benchmark.py all --backend mysql --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mysql-20261115T143418Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mysql-20261115T143418Z.jsonl --output docs/benchmarks/mysql.md
```
