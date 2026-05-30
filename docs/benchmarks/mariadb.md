# dj_queue mariadb Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-30T09:21:09.316689+00:00

## Environment

- backend: `mariadb`
- database: `mysql` `dj_queue_benchmark`
- database version: `10.6.25-MariaDB-ubu2204`
- Python: `3.14.5`
- Django: `6.0.5`
- dj_queue: `0.10.6`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `75282c93bac5`
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
| 1000 | 0 | 6.127 | 163.206 | 5 | **9.733** | 1000 | 1000 | 6.124 | 6.247 | 11.684 |
| 1000 | 1 | 5.341 | 187.245 | 5 | **8.929** | 1000 | 1000 | 5.338 | 4.786 | 11.857 |
| 1000 | 2 | 4.392 | 227.667 | 5 | **6.473** | 1000 | 1000 | 4.390 | 4.109 | 9.313 |
| 10000 | 0 | 60.777 | 164.537 | 5 | **10.065** | 10000 | 10000 | 6.075 | 6.095 | 11.431 |
| 10000 | 1 | 60.366 | 165.655 | 5 | **9.863** | 10000 | 10000 | 6.034 | 6.139 | 11.317 |
| 10000 | 2 | 60.082 | 166.438 | 5 | **10.033** | 10000 | 10000 | 6.005 | 6.056 | 11.258 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.213 | **4699.703** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.249 | **4019.141** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.211 | **4749.020** | 5 | 1000 | 1000 |
| 10000 | 0 | 1.330 | **7516.974** | 5 | 10000 | 10000 |
| 10000 | 1 | 1.248 | **8015.745** | 5 | 10000 | 10000 |
| 10000 | 2 | 1.197 | **8353.256** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.107 | **9329.301** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.081 | **12297.942** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.086 | **11668.895** | 1000 | 1000 | 1000 |
| 10000 | 0 | 0.913 | **10946.947** | 10000 | 10000 | 10000 |
| 10000 | 1 | 0.969 | **10320.265** | 10000 | 10000 | 10000 |
| 10000 | 2 | 0.951 | **10511.092** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.008** | 129872.930 | 0 | 1000 |
| 1000 | 1 | **0.006** | 164335.161 | 0 | 1000 |
| 1000 | 2 | **0.008** | 132836.683 | 0 | 1000 |
| 10000 | 0 | **0.012** | 867914.290 | 0 | 10000 |
| 10000 | 1 | **0.034** | 293026.700 | 0 | 10000 |
| 10000 | 2 | **0.029** | 343111.038 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.379 | **725.170** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.529 | **654.206** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.330 | **752.063** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 13.114 | **762.550** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 13.184 | **758.482** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 12.871 | **776.913** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 195.331 | 3001 | **42.929** | 11998 | 5000 | 6998 | 1000 | 23.294 | 5.120 |
| 1000 | 1 | 178.240 | 3001 | **43.199** | 11998 | 5000 | 6998 | 1000 | 23.149 | 5.610 |
| 1000 | 2 | 227.341 | 3001 | **43.318** | 11998 | 5000 | 6998 | 1000 | 23.085 | 4.399 |
| 10000 | 0 | 179.708 | 30001 | **39.040** | 119998 | 50000 | 69998 | 10000 | 256.149 | 55.646 |
| 10000 | 1 | 178.794 | 30001 | **39.570** | 119998 | 50000 | 69998 | 10000 | 252.717 | 55.930 |
| 10000 | 2 | 190.346 | 30001 | **39.645** | 119998 | 50000 | 69998 | 10000 | 252.240 | 52.536 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 188.610 | **99.632** | 1000 | 0 | 0 | 0 | 1000 | 10.037 | 5.302 | 1000 | True | 4 |
| 1000 | 1 | 202.000 | **84.851** | 1000 | 0 | 0 | 0 | 1000 | 11.785 | 4.950 | 1000 | True | 4 |
| 1000 | 2 | 183.261 | **95.426** | 1000 | 0 | 0 | 0 | 1000 | 10.479 | 5.457 | 1000 | True | 4 |
| 10000 | 0 | 183.520 | **67.728** | 10000 | 0 | 0 | 0 | 10000 | 147.650 | 54.490 | 10000 | True | 4 |
| 10000 | 1 | 185.353 | **67.449** | 10000 | 0 | 0 | 0 | 10000 | 148.260 | 53.951 | 10000 | True | 4 |
| 10000 | 2 | 181.138 | **66.864** | 10000 | 0 | 0 | 0 | 10000 | 149.556 | 55.207 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 9.226 | **108.390** | 4.626 | 2005 | 4.580 | 2000 | 1000 |
| 1000 | 1 | 9.466 | **105.636** | 4.746 | 2005 | 4.700 | 2000 | 1000 |
| 1000 | 2 | 8.225 | **121.574** | 4.265 | 2005 | 3.942 | 2000 | 1000 |
| 10000 | 0 | 96.415 | **103.719** | 52.660 | 20005 | 43.551 | 20000 | 10000 |
| 10000 | 1 | 94.805 | **105.480** | 52.332 | 20005 | 42.271 | 20000 | 10000 |
| 10000 | 2 | 95.293 | **104.940** | 52.248 | 20005 | 42.842 | 20000 | 10000 |

## Reproduce

```bash
docker compose up mariadb -d
bin/benchmark.py all --backend mariadb --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mariadb-20260530T083103Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mariadb-20260530T083103Z.jsonl --output docs/benchmarks/mariadb.md
```
