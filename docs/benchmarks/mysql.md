# dj_queue mysql Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-27T06:06:35.987539+00:00

## Environment

- backend: `mysql`
- database: `mysql` `dj_queue_benchmark`
- database version: `8.4.8`
- Python: `3.14.5`
- Django: `6.0.5`
- dj_queue: `0.10.6`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `8a524c8f1d1f`
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
| 1000 | 0 | 3.888 | 257.231 | 5 | **5.102** | 1000 | 1000 | 3.886 | 3.761 | 6.239 |
| 1000 | 1 | 4.195 | 238.399 | 5 | **5.461** | 1000 | 1000 | 4.193 | 4.057 | 8.124 |
| 1000 | 2 | 4.352 | 229.754 | 5 | **5.893** | 1000 | 1000 | 4.351 | 4.157 | 8.407 |
| 10000 | 0 | 54.291 | 184.192 | 5 | **10.194** | 10000 | 10000 | 5.427 | 4.397 | 16.128 |
| 10000 | 1 | 48.095 | 207.922 | 5 | **7.828** | 10000 | 10000 | 4.808 | 4.127 | 12.494 |
| 10000 | 2 | 47.324 | 211.309 | 5 | **7.472** | 10000 | 10000 | 4.731 | 4.089 | 12.007 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.318 | **3140.840** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.406 | **2464.608** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.387 | **2586.647** | 5 | 1000 | 1000 |
| 10000 | 0 | 1.406 | **7113.719** | 5 | 10000 | 10000 |
| 10000 | 1 | 1.676 | **5966.469** | 5 | 10000 | 10000 |
| 10000 | 2 | 1.397 | **7158.583** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.140 | **7151.541** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.169 | **5906.333** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.158 | **6320.089** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.088 | **9194.345** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.114 | **8977.705** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.051 | **9517.227** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.021** | 47334.383 | 0 | 1000 |
| 1000 | 1 | **0.008** | 119138.618 | 0 | 1000 |
| 1000 | 2 | **0.007** | 137691.262 | 0 | 1000 |
| 10000 | 0 | **0.022** | 456859.176 | 0 | 10000 |
| 10000 | 1 | **0.023** | 435035.591 | 0 | 10000 |
| 10000 | 2 | **0.026** | 383179.082 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.507 | **663.570** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.659 | **602.745** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.708 | **585.316** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 14.661 | **682.075** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 14.902 | **671.043** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 14.681 | **681.169** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 230.066 | 3001 | **62.074** | 11998 | 5000 | 6998 | 1000 | 16.110 | 4.347 |
| 1000 | 1 | 151.636 | 3001 | **58.201** | 11998 | 5000 | 6998 | 1000 | 17.182 | 6.595 |
| 1000 | 2 | 216.971 | 3001 | **60.478** | 11998 | 5000 | 6998 | 1000 | 16.535 | 4.609 |
| 10000 | 0 | 192.008 | 30001 | **56.078** | 119998 | 50000 | 69998 | 10000 | 178.323 | 52.081 |
| 10000 | 1 | 221.331 | 30001 | **71.270** | 119998 | 50000 | 69998 | 10000 | 140.311 | 45.181 |
| 10000 | 2 | 209.459 | 30001 | **55.576** | 119998 | 50000 | 69998 | 10000 | 179.934 | 47.742 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 227.053 | **70.734** | 1000 | 0 | 0 | 0 | 1000 | 14.137 | 4.404 | 1000 | True | 4 |
| 1000 | 1 | 223.080 | **78.803** | 1000 | 0 | 0 | 0 | 1000 | 12.690 | 4.483 | 1000 | True | 4 |
| 1000 | 2 | 224.670 | **64.572** | 1000 | 0 | 0 | 0 | 1000 | 15.487 | 4.451 | 1000 | True | 4 |
| 10000 | 0 | 185.973 | **68.072** | 10000 | 0 | 0 | 0 | 10000 | 146.904 | 53.771 | 10000 | True | 4 |
| 10000 | 1 | 161.176 | **65.086** | 10000 | 0 | 0 | 0 | 10000 | 153.643 | 62.044 | 10000 | True | 4 |
| 10000 | 2 | 202.696 | **68.446** | 10000 | 0 | 0 | 0 | 10000 | 146.100 | 49.335 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 6.959 | **143.694** | 3.255 | 2005 | 3.693 | 2000 | 1000 |
| 1000 | 1 | 7.054 | **141.759** | 3.354 | 2005 | 3.688 | 2000 | 1000 |
| 1000 | 2 | 6.680 | **149.703** | 3.261 | 2005 | 3.407 | 2000 | 1000 |
| 10000 | 0 | 89.651 | **111.544** | 40.131 | 20005 | 49.374 | 20000 | 10000 |
| 10000 | 1 | 78.917 | **126.715** | 37.076 | 20005 | 41.705 | 20000 | 10000 |
| 10000 | 2 | 81.262 | **123.059** | 36.888 | 20005 | 44.243 | 20000 | 10000 |

## Reproduce

```bash
docker compose up mysql -d
bin/benchmark.py all --backend mysql --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mysql-20260527T052423Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mysql-20260527T052423Z.jsonl --output docs/benchmarks/mysql.md
```
