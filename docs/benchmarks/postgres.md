# dj_queue PostgreSQL Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-23T18:13:03.863100+00:00

## Environment

- backend: `postgres`
- database: `postgresql` `dj_queue_benchmark`
- database version: `PostgreSQL 17.9 (Debian 17.9-1.pgdg13+1) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit`
- Python: `3.14.4`
- Django: `6.0.5`
- dj_queue: `0.10.2`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `c939c210c215`
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
| 1000 | 0 | 8.866 | 112.788 | **12.523** | 1000 | 1000 | 8.862 | 8.437 | 14.772 |
| 1000 | 1 | 9.090 | 110.009 | **13.479** | 1000 | 1000 | 9.086 | 8.734 | 15.987 |
| 1000 | 2 | 9.023 | 110.832 | **12.518** | 1000 | 1000 | 9.019 | 8.666 | 14.507 |
| 10000 | 0 | 86.562 | 115.525 | **12.786** | 10000 | 10000 | 8.652 | 8.175 | 14.608 |
| 10000 | 1 | 88.269 | 113.289 | **12.535** | 10000 | 10000 | 8.823 | 8.301 | 14.410 |
| 10000 | 2 | 94.792 | 105.494 | **12.478** | 10000 | 10000 | 9.475 | 10.381 | 14.387 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.117 | **8557.745** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.093 | **10717.664** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.108 | **9262.815** | 5 | 1000 | 1000 |
| 10000 | 0 | 0.863 | **11583.573** | 5 | 10000 | 10000 |
| 10000 | 1 | 0.907 | **11027.354** | 5 | 10000 | 10000 |
| 10000 | 2 | 0.914 | **10944.536** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.135 | **7402.393** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.137 | **7274.879** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.133 | **7528.606** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.256 | **7964.431** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.221 | **8188.208** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.244 | **8041.739** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.007** | 137204.005 | 0 | 1000 |
| 1000 | 1 | **0.005** | 201673.893 | 0 | 1000 |
| 1000 | 2 | **0.004** | 282316.407 | 0 | 1000 |
| 10000 | 0 | **0.008** | 1267956.002 | 0 | 10000 |
| 10000 | 1 | **0.011** | 933641.436 | 0 | 10000 |
| 10000 | 2 | **0.011** | 892618.049 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 2.218 | **450.789** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 2.288 | **437.130** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 2.229 | **448.603** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 21.845 | **457.761** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 21.896 | **456.697** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 21.835 | **457.984** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 71.948 | **29.778** | 10998 | 4000 | 6998 | 1000 | 33.581 | 13.899 |
| 1000 | 1 | 63.992 | **35.002** | 10998 | 4000 | 6998 | 1000 | 28.570 | 15.627 |
| 1000 | 2 | 70.243 | **34.331** | 10998 | 4000 | 6998 | 1000 | 29.128 | 14.236 |
| 10000 | 0 | 83.618 | **40.470** | 109998 | 40000 | 69998 | 10000 | 247.099 | 119.591 |
| 10000 | 1 | 96.132 | **37.833** | 109998 | 40000 | 69998 | 10000 | 264.317 | 104.024 |
| 10000 | 2 | 95.402 | **39.924** | 109998 | 40000 | 69998 | 10000 | 250.477 | 104.819 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_query_count | finished_count |
|---|---|---|---|---|---|
| 1000 | 0 | 15.032 | **66.525** | 1671 | 1000 |
| 1000 | 1 | 16.309 | **61.317** | 1671 | 1000 |
| 1000 | 2 | 13.603 | **73.516** | 1671 | 1000 |
| 10000 | 0 | 126.374 | **79.130** | 16671 | 10000 |
| 10000 | 1 | 131.303 | **76.160** | 16671 | 10000 |
| 10000 | 2 | 143.631 | **69.623** | 16671 | 10000 |

## Reproduce

```bash
docker compose up postgres -d
bin/benchmark.py all --backend postgres --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/postgres-20260523T172701Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/postgres-20260523T172701Z.jsonl --output docs/benchmarks/postgres.md
```
