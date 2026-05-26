# dj_queue PostgreSQL Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-26T06:36:00.661231+00:00

## Environment

- backend: `postgres`
- database: `postgresql` `dj_queue_benchmark`
- database version: `PostgreSQL 17.9 (Debian 17.9-1.pgdg13+1) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit`
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
| 1000 | 0 | 2.171 | 460.613 | 5 | **3.117** | 1000 | 1000 | 2.170 | 1.981 | 3.860 |
| 1000 | 1 | 7.572 | 132.074 | 5 | **11.994** | 1000 | 1000 | 7.568 | 7.153 | 13.610 |
| 1000 | 2 | 7.585 | 131.840 | 5 | **11.959** | 1000 | 1000 | 7.581 | 6.791 | 13.246 |
| 10000 | 0 | 80.870 | 123.655 | 5 | **12.250** | 10000 | 10000 | 8.084 | 7.620 | 14.257 |
| 10000 | 1 | 85.931 | 116.372 | 5 | **12.186** | 10000 | 10000 | 8.589 | 8.748 | 14.083 |
| 10000 | 2 | 86.152 | 116.074 | 5 | **12.062** | 10000 | 10000 | 8.611 | 8.805 | 13.853 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.115 | **8718.678** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.110 | **9106.348** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.113 | **8859.286** | 5 | 1000 | 1000 |
| 10000 | 0 | 0.902 | **11082.845** | 5 | 10000 | 10000 |
| 10000 | 1 | 0.917 | **10910.777** | 5 | 10000 | 10000 |
| 10000 | 2 | 0.893 | **11203.960** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.128 | **7827.972** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.127 | **7902.954** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.177 | **5656.567** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.237 | **8087.294** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.332 | **7509.985** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.293 | **7733.341** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.006** | 166511.700 | 0 | 1000 |
| 1000 | 1 | **0.006** | 166443.604 | 0 | 1000 |
| 1000 | 2 | **0.005** | 182917.082 | 0 | 1000 |
| 10000 | 0 | **0.042** | 239188.912 | 0 | 10000 |
| 10000 | 1 | **0.009** | 1059191.094 | 0 | 10000 |
| 10000 | 2 | **0.007** | 1387499.849 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.638 | **610.486** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.684 | **593.741** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.680 | **595.098** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 15.825 | **631.911** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 15.699 | **636.991** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 15.940 | **627.363** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 88.660 | 4999 | **52.033** | 8999 | 4000 | 4999 | 1000 | 19.219 | 11.279 |
| 1000 | 1 | 94.008 | 4999 | **52.616** | 8999 | 4000 | 4999 | 1000 | 19.006 | 10.637 |
| 1000 | 2 | 104.749 | 4999 | **50.654** | 8999 | 4000 | 4999 | 1000 | 19.742 | 9.547 |
| 10000 | 0 | 111.222 | 49999 | **48.539** | 89999 | 40000 | 49999 | 10000 | 206.021 | 89.910 |
| 10000 | 1 | 91.282 | 49999 | **47.082** | 89999 | 40000 | 49999 | 10000 | 212.393 | 109.550 |
| 10000 | 2 | 89.323 | 49999 | **47.082** | 89999 | 40000 | 49999 | 10000 | 212.395 | 111.953 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 82.275 | **65.740** | 1000 | 0 | 0 | 0 | 1000 | 15.211 | 12.154 | 1000 | True | 4 |
| 1000 | 1 | 93.790 | **64.662** | 1000 | 0 | 0 | 0 | 1000 | 15.465 | 10.662 | 1000 | True | 4 |
| 1000 | 2 | 94.780 | **65.488** | 1000 | 0 | 0 | 0 | 1000 | 15.270 | 10.551 | 1000 | True | 4 |
| 10000 | 0 | 93.402 | **72.700** | 10000 | 0 | 0 | 0 | 10000 | 137.551 | 107.064 | 10000 | True | 4 |
| 10000 | 1 | 92.437 | **74.199** | 10000 | 0 | 0 | 0 | 10000 | 134.774 | 108.182 | 10000 | True | 4 |
| 10000 | 2 | 92.860 | **73.416** | 10000 | 0 | 0 | 0 | 10000 | 136.209 | 107.689 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 9.702 | **103.072** | 4.437 | 1671 | 5.243 | 1000 | 1000 |
| 1000 | 1 | 9.468 | **105.623** | 4.355 | 1671 | 5.091 | 1000 | 1000 |
| 1000 | 2 | 9.494 | **105.327** | 4.386 | 1671 | 5.087 | 1000 | 1000 |
| 10000 | 0 | 96.065 | **104.097** | 44.401 | 16671 | 51.442 | 10000 | 10000 |
| 10000 | 1 | 96.717 | **103.395** | 44.639 | 16671 | 51.857 | 10000 | 10000 |
| 10000 | 2 | 96.723 | **103.388** | 44.349 | 16671 | 52.153 | 10000 | 10000 |

## Reproduce

```bash
docker compose up postgres -d
bin/benchmark.py all --backend postgres --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/postgres-20260526T054046Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/postgres-20260526T054046Z.jsonl --output docs/benchmarks/postgres.md
```
