# dj_queue mysql Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-11-14T22:56:05.996708+00:00

## Environment

- backend: `mysql`
- database: `mysql` `dj_queue_benchmark`
- database version: `9.7.4`
- Python: `3.14.10`
- Django: `6.1.3`
- psycopg implementation: `None`
- dj_queue: `0.17.0`
- platform: `macOS-27.2.1-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `322d6e4f5f22`
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
| 1000 | 0 | 2.658 | 376.165 | 4 | **3.358** | 1000 | 1000 | 2.656 | 2.607 | 5.290 |
| 1000 | 1 | 2.671 | 374.351 | 4 | **3.317** | 1000 | 1000 | 2.669 | 2.623 | 5.396 |
| 1000 | 2 | 2.698 | 370.666 | 4 | **3.402** | 1000 | 1000 | 2.696 | 2.610 | 5.671 |
| 10000 | 0 | 28.648 | 349.059 | 4 | **4.683** | 10000 | 10000 | 2.863 | 2.597 | 7.181 |
| 10000 | 1 | 27.900 | 358.426 | 4 | **3.914** | 10000 | 10000 | 2.788 | 2.639 | 6.207 |
| 10000 | 2 | 32.177 | 310.785 | 4 | **4.882** | 10000 | 10000 | 3.216 | 2.705 | 10.645 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.232 | **4308.754** | 4 | 1000 | 1000 |
| 1000 | 1 | 0.220 | **4545.724** | 4 | 1000 | 1000 |
| 1000 | 2 | 0.261 | **3833.486** | 4 | 1000 | 1000 |
| 10000 | 0 | 1.083 | **9231.787** | 4 | 10000 | 10000 |
| 10000 | 1 | 1.114 | **8977.417** | 4 | 10000 | 10000 |
| 10000 | 2 | 1.019 | **9815.368** | 4 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.095 | **10498.210** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.106 | **9418.576** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.095 | **10530.371** | 1000 | 1000 | 1000 |
| 10000 | 0 | 0.782 | **12781.194** | 10000 | 10000 | 10000 |
| 10000 | 1 | 0.766 | **13061.539** | 10000 | 10000 | 10000 |
| 10000 | 2 | 0.806 | **12405.774** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | query_count | fired_count | recurring_task_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | **0.002** | 466962.414 | 1 | 0 | 1000 |
| 1000 | 1 | **0.002** | 530926.460 | 1 | 0 | 1000 |
| 1000 | 2 | **0.002** | 626468.289 | 1 | 0 | 1000 |
| 10000 | 0 | **0.008** | 1203369.434 | 1 | 0 | 10000 |
| 10000 | 1 | **0.007** | 1441813.661 | 1 | 0 | 10000 |
| 10000 | 2 | **0.007** | 1435406.626 | 1 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 0.519 | **1928.613** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 0.537 | **1861.292** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 0.530 | **1885.212** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 5.016 | **1993.795** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 4.796 | **2085.241** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 7.929 | **1261.271** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 321.685 | 3000 | **50.481** | 11999 | 6000 | 5999 | 1000 | 19.810 | 3.109 |
| 1000 | 1 | 320.586 | 3000 | **52.942** | 11999 | 6000 | 5999 | 1000 | 18.889 | 3.119 |
| 1000 | 2 | 210.388 | 3000 | **53.645** | 11999 | 6000 | 5999 | 1000 | 18.641 | 4.753 |
| 10000 | 0 | 259.368 | 30000 | **51.194** | 119999 | 60000 | 59999 | 10000 | 195.335 | 38.555 |
| 10000 | 1 | 301.555 | 30000 | **55.329** | 119999 | 60000 | 59999 | 10000 | 180.736 | 33.161 |
| 10000 | 2 | 185.887 | 30000 | **51.963** | 119999 | 60000 | 59999 | 10000 | 192.444 | 53.796 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 182.367 | **63.250** | 1000 | 0 | 0 | 0 | 1000 | 15.810 | 5.483 | 1000 | True | 4 |
| 1000 | 1 | 313.980 | **65.329** | 1000 | 0 | 0 | 0 | 1000 | 15.307 | 3.185 | 1000 | True | 4 |
| 1000 | 2 | 292.020 | **62.481** | 1000 | 0 | 0 | 0 | 1000 | 16.005 | 3.424 | 1000 | True | 4 |
| 10000 | 0 | 172.389 | **48.834** | 10000 | 0 | 0 | 0 | 10000 | 204.774 | 58.008 | 10000 | True | 4 |
| 10000 | 1 | 286.534 | **62.403** | 10000 | 0 | 0 | 0 | 10000 | 160.249 | 34.900 | 10000 | True | 4 |
| 10000 | 2 | 279.706 | **63.256** | 10000 | 0 | 0 | 0 | 10000 | 158.087 | 35.752 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 7.442 | **134.379** | 3.368 | 2005 | 4.060 | 2000 | 1000 |
| 1000 | 1 | 7.645 | **130.805** | 3.492 | 2005 | 4.139 | 2000 | 1000 |
| 1000 | 2 | 7.186 | **139.164** | 3.301 | 2005 | 3.872 | 2000 | 1000 |
| 10000 | 0 | 48.625 | **205.654** | 23.771 | 20005 | 24.769 | 20000 | 10000 |
| 10000 | 1 | 54.644 | **183.003** | 26.936 | 20005 | 27.610 | 20000 | 10000 |
| 10000 | 2 | 63.816 | **156.701** | 30.413 | 20005 | 33.290 | 20000 | 10000 |

## Reproduce

```bash
docker compose up mysql -d
bin/benchmark.py all --backend mysql --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mysql-20261114T221537Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mysql-20261114T221537Z.jsonl --output docs/benchmarks/mysql.md
```
