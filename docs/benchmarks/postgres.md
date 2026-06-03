# dj_queue PostgreSQL Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-06-03T06:08:47.226772+00:00

## Environment

- backend: `postgres`
- database: `postgresql` `dj_queue_benchmark`
- database version: `PostgreSQL 17.9 (Debian 17.9-1.pgdg13+1) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit`
- Python: `3.14.5`
- Django: `6.0.5`
- dj_queue: `0.11.0`
- platform: `macOS-26.5-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `cb4d0997597c`
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
| 1000 | 0 | 3.952 | 253.034 | 5 | **6.243** | 1000 | 1000 | 3.950 | 3.566 | 10.116 |
| 1000 | 1 | 4.557 | 219.423 | 5 | **8.037** | 1000 | 1000 | 4.555 | 4.092 | 11.544 |
| 1000 | 2 | 3.940 | 253.831 | 5 | **7.865** | 1000 | 1000 | 3.938 | 3.748 | 12.078 |
| 10000 | 0 | 31.331 | 319.175 | 5 | **5.276** | 10000 | 10000 | 3.132 | 2.764 | 10.842 |
| 10000 | 1 | 61.558 | 162.449 | 5 | **11.771** | 10000 | 10000 | 6.153 | 4.943 | 13.927 |
| 10000 | 2 | 71.476 | 139.906 | 5 | **11.868** | 10000 | 10000 | 7.144 | 6.475 | 13.143 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.118 | **8471.032** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.100 | **10008.637** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.099 | **10053.876** | 5 | 1000 | 1000 |
| 10000 | 0 | 0.831 | **12031.706** | 5 | 10000 | 10000 |
| 10000 | 1 | 0.840 | **11902.329** | 5 | 10000 | 10000 |
| 10000 | 2 | 0.821 | **12177.845** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.123 | **8098.286** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.122 | **8167.607** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.082 | **12239.053** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.144 | **8743.100** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.216 | **8220.523** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.209 | **8268.052** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.004** | 231577.489 | 0 | 1000 |
| 1000 | 1 | **0.005** | 191986.189 | 0 | 1000 |
| 1000 | 2 | **0.005** | 191145.199 | 0 | 1000 |
| 10000 | 0 | **0.008** | 1242120.307 | 0 | 10000 |
| 10000 | 1 | **0.021** | 478999.833 | 0 | 10000 |
| 10000 | 2 | **0.018** | 542856.248 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.605 | **623.230** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.571 | **636.719** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.595 | **626.845** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 15.406 | **649.101** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 15.348 | **651.531** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 15.472 | **646.337** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 148.144 | 3001 | **58.226** | 7000 | 3000 | 4000 | 1000 | 17.174 | 6.750 |
| 1000 | 1 | 129.963 | 3001 | **50.043** | 7000 | 3000 | 4000 | 1000 | 19.983 | 7.694 |
| 1000 | 2 | 124.886 | 3001 | **45.626** | 7000 | 3000 | 4000 | 1000 | 21.917 | 8.007 |
| 10000 | 0 | 126.792 | 30001 | **46.258** | 70000 | 30000 | 40000 | 10000 | 216.180 | 78.869 |
| 10000 | 1 | 131.891 | 30001 | **50.164** | 70000 | 30000 | 40000 | 10000 | 199.346 | 75.820 |
| 10000 | 2 | 119.921 | 30001 | **51.399** | 70000 | 30000 | 40000 | 10000 | 194.556 | 83.388 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 115.111 | **77.332** | 1000 | 0 | 0 | 0 | 1000 | 12.931 | 8.687 | 1000 | True | 4 |
| 1000 | 1 | 132.467 | **81.812** | 1000 | 0 | 0 | 0 | 1000 | 12.223 | 7.549 | 1000 | True | 4 |
| 1000 | 2 | 121.313 | **75.392** | 1000 | 0 | 0 | 0 | 1000 | 13.264 | 8.243 | 1000 | True | 4 |
| 10000 | 0 | 121.116 | **85.910** | 10000 | 0 | 0 | 0 | 10000 | 116.401 | 82.565 | 10000 | True | 4 |
| 10000 | 1 | 168.213 | **90.365** | 10000 | 0 | 0 | 0 | 10000 | 110.663 | 59.448 | 10000 | True | 4 |
| 10000 | 2 | 124.790 | **94.747** | 10000 | 0 | 0 | 0 | 10000 | 105.545 | 80.135 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 7.748 | **129.058** | 3.605 | 1002 | 4.124 | 1000 | 1000 |
| 1000 | 1 | 7.910 | **126.422** | 3.723 | 1002 | 4.168 | 1000 | 1000 |
| 1000 | 2 | 8.263 | **121.020** | 3.763 | 1002 | 4.480 | 1000 | 1000 |
| 10000 | 0 | 80.899 | **123.611** | 50.094 | 10002 | 30.638 | 10000 | 10000 |
| 10000 | 1 | 78.392 | **127.564** | 48.536 | 10002 | 29.696 | 10000 | 10000 |
| 10000 | 2 | 80.694 | **123.925** | 50.033 | 10002 | 30.500 | 10000 | 10000 |

## Reproduce

```bash
docker compose up postgres -d
bin/benchmark.py all --backend postgres --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/postgres-20260603T052247Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/postgres-20260603T052247Z.jsonl --output docs/benchmarks/postgres.md
```
