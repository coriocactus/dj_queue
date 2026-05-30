# dj_queue PostgreSQL Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-30T08:31:03.358502+00:00

## Environment

- backend: `postgres`
- database: `postgresql` `dj_queue_benchmark`
- database version: `PostgreSQL 17.9 (Debian 17.9-1.pgdg13+1) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit`
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
| 1000 | 0 | 1.915 | 522.274 | 5 | **2.756** | 1000 | 1000 | 1.914 | 1.785 | 3.555 |
| 1000 | 1 | 1.712 | 584.182 | 5 | **2.376** | 1000 | 1000 | 1.711 | 1.611 | 2.731 |
| 1000 | 2 | 2.189 | 456.917 | 5 | **3.242** | 1000 | 1000 | 2.188 | 1.875 | 6.146 |
| 10000 | 0 | 22.113 | 452.217 | 5 | **4.069** | 10000 | 10000 | 2.210 | 1.850 | 6.068 |
| 10000 | 1 | 32.753 | 305.318 | 5 | **6.561** | 10000 | 10000 | 3.274 | 2.836 | 8.721 |
| 10000 | 2 | 27.366 | 365.420 | 5 | **4.628** | 10000 | 10000 | 2.736 | 2.235 | 5.715 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.075 | **13317.803** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.072 | **13814.476** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.075 | **13351.974** | 5 | 1000 | 1000 |
| 10000 | 0 | 0.753 | **13281.851** | 5 | 10000 | 10000 |
| 10000 | 1 | 0.699 | **14296.139** | 5 | 10000 | 10000 |
| 10000 | 2 | 0.760 | **13157.277** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.112 | **8908.170** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.082 | **12242.812** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.084 | **11960.167** | 1000 | 1000 | 1000 |
| 10000 | 0 | 0.966 | **10348.757** | 10000 | 10000 | 10000 |
| 10000 | 1 | 0.992 | **10081.571** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.026 | **9750.552** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.006** | 167698.487 | 0 | 1000 |
| 1000 | 1 | **0.006** | 158982.512 | 0 | 1000 |
| 1000 | 2 | **0.005** | 189082.096 | 0 | 1000 |
| 10000 | 0 | **0.015** | 666466.727 | 0 | 10000 |
| 10000 | 1 | **0.009** | 1108770.375 | 0 | 10000 |
| 10000 | 2 | **0.013** | 744749.481 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.668 | **599.543** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.862 | **537.169** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.732 | **577.474** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 17.124 | **583.959** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 17.058 | **586.237** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 16.913 | **591.269** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 373.066 | 3001 | **105.201** | 7000 | 3000 | 4000 | 1000 | 9.506 | 2.680 |
| 1000 | 1 | 395.356 | 3001 | **91.520** | 7000 | 3000 | 4000 | 1000 | 10.927 | 2.529 |
| 1000 | 2 | 186.730 | 3001 | **94.158** | 7000 | 3000 | 4000 | 1000 | 10.620 | 5.355 |
| 10000 | 0 | 437.072 | 30001 | **95.817** | 70000 | 30000 | 40000 | 10000 | 104.365 | 22.880 |
| 10000 | 1 | 182.329 | 30001 | **70.218** | 70000 | 30000 | 40000 | 10000 | 142.414 | 54.846 |
| 10000 | 2 | 185.801 | 30001 | **76.879** | 70000 | 30000 | 40000 | 10000 | 130.075 | 53.821 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 257.644 | **99.756** | 1000 | 0 | 0 | 0 | 1000 | 10.024 | 3.881 | 1000 | True | 4 |
| 1000 | 1 | 512.644 | **97.364** | 1000 | 0 | 0 | 0 | 1000 | 10.271 | 1.951 | 1000 | True | 4 |
| 1000 | 2 | 265.923 | **95.485** | 1000 | 0 | 0 | 0 | 1000 | 10.473 | 3.760 | 1000 | True | 4 |
| 10000 | 0 | 203.484 | **93.602** | 10000 | 0 | 0 | 0 | 10000 | 106.836 | 49.144 | 10000 | True | 4 |
| 10000 | 1 | 326.761 | **96.484** | 10000 | 0 | 0 | 0 | 10000 | 103.644 | 30.603 | 10000 | True | 4 |
| 10000 | 2 | 163.311 | **93.203** | 10000 | 0 | 0 | 0 | 10000 | 107.292 | 61.233 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 8.002 | **124.977** | 3.912 | 1337 | 4.071 | 1000 | 1000 |
| 1000 | 1 | 7.368 | **135.727** | 3.628 | 1337 | 3.722 | 1000 | 1000 |
| 1000 | 2 | 7.804 | **128.143** | 3.844 | 1337 | 3.941 | 1000 | 1000 |
| 10000 | 0 | 80.449 | **124.302** | 39.476 | 13337 | 40.780 | 10000 | 10000 |
| 10000 | 1 | 80.314 | **124.511** | 39.627 | 13337 | 40.489 | 10000 | 10000 |
| 10000 | 2 | 80.336 | **124.476** | 38.785 | 13337 | 41.360 | 10000 | 10000 |

## Reproduce

```bash
docker compose up postgres -d
bin/benchmark.py all --backend postgres --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/postgres-20260530T075916Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/postgres-20260530T075916Z.jsonl --output docs/benchmarks/postgres.md
```
