# dj_queue PostgreSQL Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-09-02T21:24:11.916523+00:00

## Environment

- backend: `postgres`
- database: `postgresql` `dj_queue_benchmark`
- database version: `PostgreSQL 18.4 (Debian 18.4-1.pgdg13+1) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit`
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
| 1000 | 0 | 2.232 | 448.047 | 5 | **3.388** | 1000 | 1000 | 2.231 | 2.057 | 4.180 |
| 1000 | 1 | 2.391 | 418.217 | 5 | **3.623** | 1000 | 1000 | 2.390 | 2.345 | 4.270 |
| 1000 | 2 | 3.531 | 283.177 | 5 | **5.533** | 1000 | 1000 | 3.530 | 3.329 | 6.371 |
| 10000 | 0 | 29.483 | 339.182 | 5 | **5.045** | 10000 | 10000 | 2.947 | 2.789 | 6.022 |
| 10000 | 1 | 30.194 | 331.191 | 5 | **5.216** | 10000 | 10000 | 3.019 | 2.830 | 6.249 |
| 10000 | 2 | 33.149 | 301.671 | 5 | **5.616** | 10000 | 10000 | 3.314 | 3.030 | 7.195 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.090 | **11127.287** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.087 | **11439.722** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.088 | **11404.416** | 5 | 1000 | 1000 |
| 10000 | 0 | 0.801 | **12490.362** | 5 | 10000 | 10000 |
| 10000 | 1 | 0.817 | **12232.912** | 5 | 10000 | 10000 |
| 10000 | 2 | 0.797 | **12542.518** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.095 | **10492.753** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.097 | **10362.659** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.107 | **9303.826** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.120 | **8925.698** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.105 | **9048.321** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.122 | **8915.348** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | query_count | fired_count | recurring_task_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | **0.002** | 658111.220 | 1 | 0 | 1000 |
| 1000 | 1 | **0.001** | 697329.645 | 1 | 0 | 1000 |
| 1000 | 2 | **0.001** | 687561.196 | 1 | 0 | 1000 |
| 10000 | 0 | **0.002** | 4765213.186 | 1 | 0 | 10000 |
| 10000 | 1 | **0.002** | 4846038.946 | 1 | 0 | 10000 |
| 10000 | 2 | **0.002** | 4246735.318 | 1 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.252 | **799.016** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.271 | **786.541** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.319 | **758.341** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 12.317 | **811.862** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 12.092 | **827.006** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 12.356 | **809.329** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `held-xmin-worker-drain`: PostgreSQL worker drain under a held repeatable-read snapshot

- key metric: **`jobs_per_second`** - end-to-end worker-drain throughput while a second connection pins xmin; higher is better
- healthy local baseline: compare with `worker-drain` and watch dead tuples and relation bytes during the hold
- use case: PostgreSQL operations where long transactions, replication slots, or prepared transactions can delay vacuum cleanup of queue churn
- mechanics: opens a second PostgreSQL connection, begins a repeatable-read transaction to pin xmin, drains ready jobs through `worker-drain`, then samples queue-table dead tuples and relation bytes before, during, and after releasing the snapshot

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | dead_tuples_before | dead_tuples_during | dead_tuples_after_release | relation_bytes_before | relation_bytes_during | relation_bytes_after_release | claimed_count | completed_count | held_xmin | job_count | live_tuples_after_release | live_tuples_before | live_tuples_during | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.316 | **760.071** | 1000 | 0 | 49332 | 51151 | 51151 | 442368 | 2146304 | 2146304 | 0 | 1000 | True | 1000 | 32561 | 31375 | 32561 | True | 4 |
| 1000 | 1 | 1.313 | **761.575** | 1000 | 0 | 51151 | 28152 | 28152 | 442368 | 2138112 | 2138112 | 0 | 1000 | True | 1000 | 33207 | 32561 | 33207 | True | 4 |
| 1000 | 2 | 1.371 | **729.434** | 1000 | 0 | 28152 | 30696 | 30696 | 442368 | 2146304 | 2146304 | 0 | 1000 | True | 1000 | 34553 | 33207 | 34553 | True | 4 |
| 10000 | 0 | 13.409 | **745.763** | 10000 | 0 | 56878 | 32053 | 32053 | 442368 | 13688832 | 13688832 | 0 | 10000 | True | 10000 | 10040 | 44481 | 10040 | True | 4 |
| 10000 | 1 | 12.551 | **796.739** | 10000 | 0 | 32053 | 40475 | 40475 | 442368 | 13582336 | 13582336 | 0 | 10000 | True | 10000 | 20236 | 10040 | 20236 | True | 4 |
| 10000 | 2 | 12.526 | **798.319** | 10000 | 0 | 40475 | 50667 | 50667 | 442368 | 13697024 | 13697024 | 0 | 10000 | True | 10000 | 30077 | 20236 | 30077 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 471.202 | 3001 | **115.488** | 7000 | 3000 | 4000 | 1000 | 8.659 | 2.122 |
| 1000 | 1 | 339.219 | 3001 | **111.044** | 7000 | 3000 | 4000 | 1000 | 9.005 | 2.948 |
| 1000 | 2 | 465.790 | 3001 | **111.989** | 7000 | 3000 | 4000 | 1000 | 8.929 | 2.147 |
| 10000 | 0 | 283.544 | 30001 | **99.365** | 70000 | 30000 | 40000 | 10000 | 100.639 | 35.268 |
| 10000 | 1 | 292.321 | 30001 | **89.523** | 70000 | 30000 | 40000 | 10000 | 111.703 | 34.209 |
| 10000 | 2 | 353.176 | 30001 | **91.289** | 70000 | 30000 | 40000 | 10000 | 109.542 | 28.315 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 499.015 | **168.917** | 1000 | 0 | 0 | 0 | 1000 | 5.920 | 2.004 | 1000 | True | 4 |
| 1000 | 1 | 498.777 | **158.617** | 1000 | 0 | 0 | 0 | 1000 | 6.304 | 2.005 | 1000 | True | 4 |
| 1000 | 2 | 250.103 | **148.834** | 1000 | 0 | 0 | 0 | 1000 | 6.719 | 3.998 | 1000 | True | 4 |
| 10000 | 0 | 284.157 | **157.248** | 10000 | 0 | 0 | 0 | 10000 | 63.594 | 35.192 | 10000 | True | 4 |
| 10000 | 1 | 358.409 | **157.883** | 10000 | 0 | 0 | 0 | 10000 | 63.338 | 27.901 | 10000 | True | 4 |
| 10000 | 2 | 390.819 | **156.757** | 10000 | 0 | 0 | 0 | 10000 | 63.793 | 25.587 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 4.767 | **209.771** | 1.854 | 1002 | 2.904 | 1000 | 1000 |
| 1000 | 1 | 5.013 | **199.485** | 1.929 | 1002 | 3.075 | 1000 | 1000 |
| 1000 | 2 | 5.158 | **193.860** | 1.990 | 1002 | 3.159 | 1000 | 1000 |
| 10000 | 0 | 53.741 | **186.078** | 23.171 | 10002 | 30.467 | 10000 | 10000 |
| 10000 | 1 | 54.938 | **182.025** | 23.769 | 10002 | 31.066 | 10000 | 10000 |
| 10000 | 2 | 56.122 | **178.183** | 24.380 | 10002 | 31.639 | 10000 | 10000 |

## Reproduce

```bash
docker compose up postgres -d
bin/benchmark.py all --backend postgres --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/postgres-20260902T205902Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/postgres-20260902T205902Z.jsonl --output docs/benchmarks/postgres.md
```
