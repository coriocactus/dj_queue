# dj_queue mysql Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-26T08:06:33.953244+00:00

## Environment

- backend: `mysql`
- database: `mysql` `dj_queue_benchmark`
- database version: `8.4.8`
- Python: `3.14.5`
- Django: `6.0.5`
- dj_queue: `0.10.4`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `bccceb8adc16`
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
| 1000 | 0 | 4.371 | 228.782 | 5 | **6.271** | 1000 | 1000 | 4.369 | 4.079 | 8.575 |
| 1000 | 1 | 4.159 | 240.454 | 5 | **5.650** | 1000 | 1000 | 4.157 | 3.956 | 7.637 |
| 1000 | 2 | 4.365 | 229.099 | 5 | **5.766** | 1000 | 1000 | 4.363 | 4.193 | 7.456 |
| 10000 | 0 | 53.140 | 188.181 | 5 | **9.515** | 10000 | 10000 | 5.312 | 4.543 | 15.297 |
| 10000 | 1 | 52.633 | 189.994 | 5 | **9.742** | 10000 | 10000 | 5.262 | 4.502 | 16.291 |
| 10000 | 2 | 51.400 | 194.553 | 5 | **8.605** | 10000 | 10000 | 5.138 | 4.495 | 14.604 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.300 | **3335.958** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.337 | **2971.358** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.250 | **3999.910** | 5 | 1000 | 1000 |
| 10000 | 0 | 1.507 | **6635.755** | 5 | 10000 | 10000 |
| 10000 | 1 | 1.869 | **5351.378** | 5 | 10000 | 10000 |
| 10000 | 2 | 1.581 | **6325.016** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.191 | **5228.991** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.241 | **4157.577** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.191 | **5247.412** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.213 | **8247.134** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.173 | **8523.863** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.201 | **8327.340** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.009** | 110719.484 | 0 | 1000 |
| 1000 | 1 | **0.011** | 91590.818 | 0 | 1000 |
| 1000 | 2 | **0.010** | 101106.264 | 0 | 1000 |
| 10000 | 0 | **0.023** | 431397.861 | 0 | 10000 |
| 10000 | 1 | **0.027** | 370206.410 | 0 | 10000 |
| 10000 | 2 | **0.024** | 421451.156 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.360 | **735.049** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.619 | **617.543** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.418 | **705.349** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 13.776 | **725.903** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 14.273 | **700.644** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 13.815 | **723.846** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 186.892 | 3001 | **73.540** | 11998 | 5000 | 6998 | 1000 | 13.598 | 5.351 |
| 1000 | 1 | 214.011 | 3001 | **68.334** | 11998 | 5000 | 6998 | 1000 | 14.634 | 4.673 |
| 1000 | 2 | 207.640 | 3001 | **61.515** | 11998 | 5000 | 6998 | 1000 | 16.256 | 4.816 |
| 10000 | 0 | 207.211 | 30001 | **77.561** | 119998 | 50000 | 69998 | 10000 | 128.931 | 48.260 |
| 10000 | 1 | 188.468 | 30001 | **76.588** | 119998 | 50000 | 69998 | 10000 | 130.569 | 53.059 |
| 10000 | 2 | 195.157 | 30001 | **76.272** | 119998 | 50000 | 69998 | 10000 | 131.110 | 51.241 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 209.973 | **81.915** | 1000 | 0 | 0 | 0 | 1000 | 12.208 | 4.763 | 1000 | True | 4 |
| 1000 | 1 | 210.932 | **72.404** | 1000 | 0 | 0 | 0 | 1000 | 13.811 | 4.741 | 1000 | True | 4 |
| 1000 | 2 | 209.449 | **80.240** | 1000 | 0 | 0 | 0 | 1000 | 12.463 | 4.774 | 1000 | True | 4 |
| 10000 | 0 | 197.700 | **76.052** | 10000 | 0 | 0 | 0 | 10000 | 131.489 | 50.582 | 10000 | True | 4 |
| 10000 | 1 | 186.193 | **68.946** | 10000 | 0 | 0 | 0 | 10000 | 145.041 | 53.708 | 10000 | True | 4 |
| 10000 | 2 | 186.983 | **73.445** | 10000 | 0 | 0 | 0 | 10000 | 136.156 | 53.481 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 6.299 | **158.764** | 2.885 | 2005 | 3.403 | 2000 | 1000 |
| 1000 | 1 | 7.536 | **132.688** | 3.319 | 2005 | 4.206 | 2000 | 1000 |
| 1000 | 2 | 6.333 | **157.907** | 2.898 | 2005 | 3.425 | 2000 | 1000 |
| 10000 | 0 | 76.214 | **131.210** | 38.177 | 20005 | 37.901 | 20000 | 10000 |
| 10000 | 1 | 80.104 | **124.837** | 39.761 | 20005 | 40.203 | 20000 | 10000 |
| 10000 | 2 | 87.154 | **114.740** | 41.741 | 20005 | 45.232 | 20000 | 10000 |

## Reproduce

```bash
docker compose up mysql -d
bin/benchmark.py all --backend mysql --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mysql-20260526T072727Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mysql-20260526T072727Z.jsonl --output docs/benchmarks/mysql.md
```
