# dj_queue mysql Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-23T19:18:32.601045+00:00

## Environment

- backend: `mysql`
- database: `mysql` `dj_queue_benchmark`
- database version: `8.4.8`
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
| 1000 | 0 | 4.791 | 208.729 | **6.435** | 1000 | 1000 | 4.789 | 4.550 | 8.173 |
| 1000 | 1 | 5.058 | 197.725 | **6.830** | 1000 | 1000 | 5.055 | 4.815 | 9.476 |
| 1000 | 2 | 4.856 | 205.931 | **7.308** | 1000 | 1000 | 4.854 | 4.634 | 8.987 |
| 10000 | 0 | 57.104 | 175.120 | **10.108** | 10000 | 10000 | 5.708 | 5.085 | 14.626 |
| 10000 | 1 | 52.843 | 189.241 | **8.873** | 10000 | 10000 | 5.282 | 4.637 | 13.650 |
| 10000 | 2 | 56.529 | 176.900 | **9.047** | 10000 | 10000 | 5.651 | 5.004 | 14.221 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.302 | **3306.879** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.335 | **2981.964** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.327 | **3054.101** | 5 | 1000 | 1000 |
| 10000 | 0 | 1.743 | **5736.253** | 5 | 10000 | 10000 |
| 10000 | 1 | 1.784 | **5606.226** | 5 | 10000 | 10000 |
| 10000 | 2 | 1.811 | **5522.729** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.221 | **4522.483** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.169 | **5923.062** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.144 | **6961.076** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.265 | **7908.014** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.187 | **8424.705** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.288 | **7766.653** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.006** | 160816.950 | 0 | 1000 |
| 1000 | 1 | **0.007** | 140065.831 | 0 | 1000 |
| 1000 | 2 | **0.006** | 172015.873 | 0 | 1000 |
| 10000 | 0 | **0.021** | 481341.008 | 0 | 10000 |
| 10000 | 1 | **0.020** | 489301.640 | 0 | 10000 |
| 10000 | 2 | **0.022** | 450285.650 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.793 | **557.656** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.593 | **627.561** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.815 | **550.868** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 19.835 | **504.162** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 20.230 | **494.326** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 19.249 | **519.514** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 214.462 | **49.070** | 10998 | 4000 | 6998 | 1000 | 20.379 | 4.663 |
| 1000 | 1 | 156.222 | **64.193** | 10998 | 4000 | 6998 | 1000 | 15.578 | 6.401 |
| 1000 | 2 | 134.560 | **88.053** | 10998 | 4000 | 6998 | 1000 | 11.357 | 7.432 |
| 10000 | 0 | 144.052 | **56.660** | 109998 | 40000 | 69998 | 10000 | 176.493 | 69.419 |
| 10000 | 1 | 146.963 | **53.931** | 109998 | 40000 | 69998 | 10000 | 185.423 | 68.044 |
| 10000 | 2 | 141.596 | **68.551** | 109998 | 40000 | 69998 | 10000 | 145.877 | 70.623 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_query_count | finished_count |
|---|---|---|---|---|---|
| 1000 | 0 | 6.726 | **148.683** | 1671 | 1000 |
| 1000 | 1 | 6.415 | **155.874** | 1671 | 1000 |
| 1000 | 2 | 6.636 | **150.704** | 1671 | 1000 |
| 10000 | 0 | 119.141 | **83.934** | 16671 | 10000 |
| 10000 | 1 | 111.917 | **89.352** | 16671 | 10000 |
| 10000 | 2 | 118.295 | **84.535** | 16671 | 10000 |

## Reproduce

```bash
docker compose up mysql -d
bin/benchmark.py all --backend mysql --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mysql-20260523T184601Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mysql-20260523T184601Z.jsonl --output docs/benchmarks/mysql.md
```
