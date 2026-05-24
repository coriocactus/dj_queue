# dj_queue PostgreSQL Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-24T21:19:42.870603+00:00

## Environment

- backend: `postgres`
- database: `postgresql` `dj_queue_benchmark`
- database version: `PostgreSQL 17.9 (Debian 17.9-1.pgdg13+1) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit`
- Python: `3.14.4`
- Django: `6.0.5`
- dj_queue: `0.10.3`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `5dbf01bdd08f`
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
| 1000 | 0 | 7.625 | 131.142 | **12.406** | 1000 | 1000 | 7.622 | 6.980 | 13.747 |
| 1000 | 1 | 7.394 | 135.252 | **12.272** | 1000 | 1000 | 7.391 | 6.852 | 13.812 |
| 1000 | 2 | 8.291 | 120.609 | **12.737** | 1000 | 1000 | 8.288 | 7.618 | 14.405 |
| 10000 | 0 | 76.616 | 130.521 | **12.402** | 10000 | 10000 | 7.659 | 7.035 | 13.769 |
| 10000 | 1 | 74.642 | 133.972 | **12.321** | 10000 | 10000 | 7.461 | 6.921 | 13.522 |
| 10000 | 2 | 75.410 | 132.608 | **12.024** | 10000 | 10000 | 7.538 | 7.002 | 13.414 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.107 | **9330.940** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.112 | **8900.377** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.111 | **9000.941** | 5 | 1000 | 1000 |
| 10000 | 0 | 0.887 | **11267.653** | 5 | 10000 | 10000 |
| 10000 | 1 | 0.926 | **10800.781** | 5 | 10000 | 10000 |
| 10000 | 2 | 0.913 | **10949.561** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.124 | **8042.966** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.122 | **8195.694** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.133 | **7514.519** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.152 | **8682.682** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.312 | **7622.420** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.333 | **7500.261** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.006** | 172958.014 | 0 | 1000 |
| 1000 | 1 | **0.006** | 173508.181 | 0 | 1000 |
| 1000 | 2 | **0.005** | 214291.486 | 0 | 1000 |
| 10000 | 0 | **0.013** | 793753.162 | 0 | 10000 |
| 10000 | 1 | **0.020** | 489718.963 | 0 | 10000 |
| 10000 | 2 | **0.020** | 501746.706 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 2.065 | **484.245** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 2.104 | **475.286** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 2.116 | **472.635** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 20.469 | **488.532** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 20.524 | **487.232** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 20.843 | **479.772** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 99.722 | **42.111** | 11998 | 5000 | 6998 | 1000 | 23.747 | 10.028 |
| 1000 | 1 | 110.690 | **42.151** | 11998 | 5000 | 6998 | 1000 | 23.724 | 9.034 |
| 1000 | 2 | 91.877 | **41.587** | 11998 | 5000 | 6998 | 1000 | 24.046 | 10.884 |
| 10000 | 0 | 91.649 | **40.021** | 119998 | 50000 | 69998 | 10000 | 249.867 | 109.112 |
| 10000 | 1 | 97.377 | **39.708** | 119998 | 50000 | 69998 | 10000 | 251.839 | 102.693 |
| 10000 | 2 | 93.868 | **39.872** | 119998 | 50000 | 69998 | 10000 | 250.801 | 106.532 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_query_count | finished_count |
|---|---|---|---|---|---|
| 1000 | 0 | 10.465 | **95.558** | 2005 | 1000 |
| 1000 | 1 | 10.664 | **93.777** | 2005 | 1000 |
| 1000 | 2 | 10.613 | **94.221** | 2005 | 1000 |
| 10000 | 0 | 111.453 | **89.724** | 20005 | 10000 |
| 10000 | 1 | 112.279 | **89.064** | 20005 | 10000 |
| 10000 | 2 | 111.808 | **89.439** | 20005 | 10000 |

## Reproduce

```bash
docker compose up postgres -d
bin/benchmark.py all --backend postgres --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/postgres-20260524T203736Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/postgres-20260524T203736Z.jsonl --output docs/benchmarks/postgres.md
```
