# dj_queue mysql Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-30T09:57:16.126093+00:00

## Environment

- backend: `mysql`
- database: `mysql` `dj_queue_benchmark`
- database version: `8.4.8`
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
| 1000 | 0 | 4.832 | 206.961 | 5 | **6.480** | 1000 | 1000 | 4.830 | 4.707 | 8.468 |
| 1000 | 1 | 5.102 | 196.010 | 5 | **6.802** | 1000 | 1000 | 5.099 | 5.051 | 8.344 |
| 1000 | 2 | 4.721 | 211.836 | 5 | **6.270** | 1000 | 1000 | 4.718 | 4.598 | 7.276 |
| 10000 | 0 | 49.521 | 201.933 | 5 | **6.748** | 10000 | 10000 | 4.950 | 4.734 | 9.533 |
| 10000 | 1 | 51.024 | 195.986 | 5 | **7.114** | 10000 | 10000 | 5.100 | 4.765 | 12.569 |
| 10000 | 2 | 48.170 | 207.598 | 5 | **6.446** | 10000 | 10000 | 4.815 | 4.682 | 7.734 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.212 | **4706.763** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.322 | **3105.918** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.173 | **5775.463** | 5 | 1000 | 1000 |
| 10000 | 0 | 1.324 | **7552.365** | 5 | 10000 | 10000 |
| 10000 | 1 | 1.298 | **7704.971** | 5 | 10000 | 10000 |
| 10000 | 2 | 1.233 | **8108.021** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.154 | **6493.389** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.128 | **7818.838** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.138 | **7256.964** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.027 | **9734.797** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.037 | **9644.297** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.061 | **9421.808** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.009** | 112791.491 | 0 | 1000 |
| 1000 | 1 | **0.009** | 105569.677 | 0 | 1000 |
| 1000 | 2 | **0.009** | 112407.962 | 0 | 1000 |
| 10000 | 0 | **0.023** | 429694.761 | 0 | 10000 |
| 10000 | 1 | **0.026** | 382187.509 | 0 | 10000 |
| 10000 | 2 | **0.025** | 392571.880 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.428 | **700.524** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.460 | **684.812** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.481 | **675.235** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 18.705 | **534.622** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 14.625 | **683.783** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 14.770 | **677.032** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 204.240 | 3001 | **56.077** | 11998 | 5000 | 6998 | 1000 | 17.833 | 4.896 |
| 1000 | 1 | 187.854 | 3001 | **55.913** | 11998 | 5000 | 6998 | 1000 | 17.885 | 5.323 |
| 1000 | 2 | 191.827 | 3001 | **53.981** | 11998 | 5000 | 6998 | 1000 | 18.525 | 5.213 |
| 10000 | 0 | 207.060 | 30001 | **76.297** | 119998 | 50000 | 69998 | 10000 | 131.067 | 48.295 |
| 10000 | 1 | 231.502 | 30001 | **88.909** | 119998 | 50000 | 69998 | 10000 | 112.475 | 43.196 |
| 10000 | 2 | 225.117 | 30001 | **85.993** | 119998 | 50000 | 69998 | 10000 | 116.289 | 44.421 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 243.657 | **83.339** | 1000 | 0 | 0 | 0 | 1000 | 11.999 | 4.104 | 1000 | True | 4 |
| 1000 | 1 | 231.825 | **86.412** | 1000 | 0 | 0 | 0 | 1000 | 11.572 | 4.314 | 1000 | True | 4 |
| 1000 | 2 | 237.367 | **84.073** | 1000 | 0 | 0 | 0 | 1000 | 11.894 | 4.213 | 1000 | True | 4 |
| 10000 | 0 | 214.510 | **79.277** | 10000 | 0 | 0 | 0 | 10000 | 126.140 | 46.618 | 10000 | True | 4 |
| 10000 | 1 | 225.398 | **80.204** | 10000 | 0 | 0 | 0 | 10000 | 124.682 | 44.366 | 10000 | True | 4 |
| 10000 | 2 | 221.047 | **77.334** | 10000 | 0 | 0 | 0 | 10000 | 129.309 | 45.239 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 6.367 | **157.063** | 2.796 | 2005 | 3.559 | 2000 | 1000 |
| 1000 | 1 | 4.374 | **228.637** | 1.802 | 2005 | 2.565 | 2000 | 1000 |
| 1000 | 2 | 5.600 | **178.579** | 2.418 | 2005 | 3.173 | 2000 | 1000 |
| 10000 | 0 | 53.882 | **185.590** | 23.604 | 20005 | 30.189 | 20000 | 10000 |
| 10000 | 1 | 56.626 | **176.596** | 26.117 | 20005 | 30.405 | 20000 | 10000 |
| 10000 | 2 | 56.339 | **177.496** | 24.963 | 20005 | 31.280 | 20000 | 10000 |

## Reproduce

```bash
docker compose up mysql -d
bin/benchmark.py all --backend mysql --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mysql-20260530T092109Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mysql-20260530T092109Z.jsonl --output docs/benchmarks/mysql.md
```
