# dj_queue mariadb Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-06-03T15:04:21.532516+00:00

## Environment

- backend: `mariadb`
- database: `mysql` `dj_queue_benchmark`
- database version: `10.6.25-MariaDB-ubu2204`
- Python: `3.14.5`
- Django: `6.0.5`
- dj_queue: `0.12.0`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `411646c33337`
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
| 1000 | 0 | 4.242 | 235.715 | 5 | **6.011** | 1000 | 1000 | 4.240 | 3.767 | 8.043 |
| 1000 | 1 | 5.177 | 193.143 | 5 | **8.258** | 1000 | 1000 | 5.175 | 4.947 | 10.725 |
| 1000 | 2 | 4.420 | 226.240 | 5 | **6.420** | 1000 | 1000 | 4.418 | 3.947 | 8.552 |
| 10000 | 0 | 62.531 | 159.921 | 5 | **9.596** | 10000 | 10000 | 6.250 | 6.340 | 11.728 |
| 10000 | 1 | 62.446 | 160.139 | 5 | **10.867** | 10000 | 10000 | 6.241 | 6.361 | 12.025 |
| 10000 | 2 | 45.734 | 218.654 | 5 | **8.619** | 10000 | 10000 | 4.571 | 4.173 | 11.703 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.098 | **10192.109** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.124 | **8069.210** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.169 | **5926.069** | 5 | 1000 | 1000 |
| 10000 | 0 | 1.188 | **8419.984** | 5 | 10000 | 10000 |
| 10000 | 1 | 1.069 | **9353.542** | 5 | 10000 | 10000 |
| 10000 | 2 | 1.114 | **8980.012** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.110 | **9091.691** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.086 | **11655.046** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.097 | **10293.273** | 1000 | 1000 | 1000 |
| 10000 | 0 | 0.991 | **10088.306** | 10000 | 10000 | 10000 |
| 10000 | 1 | 0.962 | **10399.507** | 10000 | 10000 | 10000 |
| 10000 | 2 | 0.947 | **10559.470** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.008** | 123426.315 | 0 | 1000 |
| 1000 | 1 | **0.009** | 114578.708 | 0 | 1000 |
| 1000 | 2 | **0.006** | 178667.143 | 0 | 1000 |
| 10000 | 0 | **0.011** | 905554.390 | 0 | 10000 |
| 10000 | 1 | **0.033** | 305700.551 | 0 | 10000 |
| 10000 | 2 | **0.012** | 838360.559 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.076 | **929.154** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 0.974 | **1026.202** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 0.988 | **1012.623** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 9.026 | **1107.940** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 8.990 | **1112.372** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 8.917 | **1121.442** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 407.226 | 3001 | **77.807** | 11998 | 5000 | 6998 | 1000 | 12.852 | 2.456 |
| 1000 | 1 | 217.837 | 3001 | **49.129** | 11998 | 5000 | 6998 | 1000 | 20.355 | 4.591 |
| 1000 | 2 | 204.717 | 3001 | **47.960** | 11998 | 5000 | 6998 | 1000 | 20.851 | 4.885 |
| 10000 | 0 | 463.389 | 30001 | **48.781** | 119998 | 50000 | 69998 | 10000 | 204.999 | 21.580 |
| 10000 | 1 | 234.653 | 30001 | **48.118** | 119998 | 50000 | 69998 | 10000 | 207.822 | 42.616 |
| 10000 | 2 | 408.455 | 30001 | **69.247** | 119998 | 50000 | 69998 | 10000 | 144.411 | 24.483 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 462.659 | **138.832** | 1000 | 0 | 0 | 0 | 1000 | 7.203 | 2.161 | 1000 | True | 4 |
| 1000 | 1 | 445.500 | **139.972** | 1000 | 0 | 0 | 0 | 1000 | 7.144 | 2.245 | 1000 | True | 4 |
| 1000 | 2 | 422.731 | **147.272** | 1000 | 0 | 0 | 0 | 1000 | 6.790 | 2.366 | 1000 | True | 4 |
| 10000 | 0 | 430.680 | **88.650** | 10000 | 0 | 0 | 0 | 10000 | 112.804 | 23.219 | 10000 | True | 4 |
| 10000 | 1 | 458.465 | **89.558** | 10000 | 0 | 0 | 0 | 10000 | 111.659 | 21.812 | 10000 | True | 4 |
| 10000 | 2 | 446.103 | **89.147** | 10000 | 0 | 0 | 0 | 10000 | 112.175 | 22.416 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 2.913 | **343.338** | 1.474 | 2005 | 1.433 | 2000 | 1000 |
| 1000 | 1 | 2.716 | **368.165** | 1.379 | 2005 | 1.331 | 2000 | 1000 |
| 1000 | 2 | 3.034 | **329.588** | 1.519 | 2005 | 1.509 | 2000 | 1000 |
| 10000 | 0 | 53.570 | **186.670** | 29.131 | 20005 | 24.328 | 20000 | 10000 |
| 10000 | 1 | 57.077 | **175.203** | 30.750 | 20005 | 26.207 | 20000 | 10000 |
| 10000 | 2 | 55.747 | **179.381** | 30.187 | 20005 | 25.445 | 20000 | 10000 |

## Reproduce

```bash
docker compose up mariadb -d
bin/benchmark.py all --backend mariadb --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mariadb-20260603T143001Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mariadb-20260603T143001Z.jsonl --output docs/benchmarks/mariadb.md
```
