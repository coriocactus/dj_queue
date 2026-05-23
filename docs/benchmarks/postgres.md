# dj_queue PostgreSQL Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-23T12:06:14.589724+00:00

## Environment

- backend: `postgres`
- database: `postgresql` `dj_queue_benchmark`
- database version: `PostgreSQL 17.9 (Debian 17.9-1.pgdg13+1) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit`
- Python: `3.14.4`
- Django: `6.0.5`
- dj_queue: `0.9.2`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `b0af38279ead`
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

| size | run | duration_seconds | jobs_per_second | **latency_p95_ms** | ready_count | job_count | latency_mean_ms | latency_p50_ms | latency_p99_ms |
|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 6.212 | 160.982 | **9.389** | 1000 | 1000 | 6.211 | 5.760 | 10.844 |
| 1000 | 1 | 6.282 | 159.183 | **9.425** | 1000 | 1000 | 6.281 | 5.859 | 10.979 |
| 1000 | 2 | 6.039 | 165.603 | **9.630** | 1000 | 1000 | 6.037 | 5.714 | 11.853 |
| 10000 | 0 | 79.539 | 125.724 | **15.672** | 10000 | 10000 | 7.951 | 6.327 | 18.614 |
| 10000 | 1 | 111.929 | 89.343 | **17.609** | 10000 | 10000 | 11.189 | 11.082 | 20.339 |
| 10000 | 2 | 128.618 | 77.750 | **17.856** | 10000 | 10000 | 12.858 | 12.587 | 20.424 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.168 | **5959.051** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.143 | **6987.516** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.151 | **6604.015** | 5 | 1000 | 1000 |
| 10000 | 0 | 1.198 | **8348.178** | 5 | 10000 | 10000 |
| 10000 | 1 | 1.238 | **8078.019** | 5 | 10000 | 10000 |
| 10000 | 2 | 1.184 | **8448.150** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.132 | **7601.229** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.144 | **6964.411** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.134 | **7449.899** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.280 | **7813.460** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.262 | **7926.995** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.284 | **7789.936** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.004** | 228754.432 | 0 | 1000 |
| 1000 | 1 | **0.005** | 221717.200 | 0 | 1000 |
| 1000 | 2 | **0.004** | 243882.631 | 0 | 1000 |
| 10000 | 0 | **0.011** | 883444.203 | 0 | 10000 |
| 10000 | 1 | **0.007** | 1404379.191 | 0 | 10000 |
| 10000 | 2 | **0.008** | 1263217.678 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 2.952 | **338.769** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 2.934 | **340.781** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 3.016 | **331.514** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 29.223 | **342.196** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 28.875 | **346.316** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 28.718 | **348.208** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 61.204 | **23.472** | 15995 | 4000 | 11995 | 1000 | 42.604 | 16.339 |
| 1000 | 1 | 60.812 | **29.745** | 15995 | 4000 | 11995 | 1000 | 33.619 | 16.444 |
| 1000 | 2 | 55.497 | **23.059** | 15995 | 4000 | 11995 | 1000 | 43.367 | 18.019 |
| 10000 | 0 | 62.067 | **27.996** | 159995 | 40000 | 119995 | 10000 | 357.199 | 161.117 |
| 10000 | 1 | 83.413 | **28.080** | 159995 | 40000 | 119995 | 10000 | 356.128 | 119.885 |
| 10000 | 2 | 64.731 | **26.735** | 159995 | 40000 | 119995 | 10000 | 374.043 | 154.487 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_query_count | finished_count |
|---|---|---|---|---|---|
| 1000 | 0 | 16.140 | **61.959** | 1336 | 1000 |
| 1000 | 1 | 16.851 | **59.344** | 1336 | 1000 |
| 1000 | 2 | 16.497 | **60.618** | 1336 | 1000 |
| 10000 | 0 | 129.535 | **77.199** | 13336 | 10000 |
| 10000 | 1 | 128.259 | **77.967** | 13336 | 10000 |
| 10000 | 2 | 124.934 | **80.042** | 13336 | 10000 |

## Reproduce

```bash
docker compose up postgres -d
bin/benchmark.py all --backend postgres --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/postgres-20260523T070030Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/postgres-20260523T070030Z.jsonl --output docs/benchmarks/postgres.md
```
