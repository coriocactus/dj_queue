# dj_queue PostgreSQL Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-27T04:31:44.383901+00:00

## Environment

- backend: `postgres`
- database: `postgresql` `dj_queue_benchmark`
- database version: `PostgreSQL 17.9 (Debian 17.9-1.pgdg13+1) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit`
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
| 1000 | 0 | 8.168 | 122.423 | 5 | **11.427** | 1000 | 1000 | 8.164 | 8.157 | 12.650 |
| 1000 | 1 | 8.090 | 123.612 | 5 | **11.552** | 1000 | 1000 | 8.086 | 7.981 | 13.101 |
| 1000 | 2 | 7.971 | 125.450 | 5 | **11.486** | 1000 | 1000 | 7.968 | 7.694 | 12.712 |
| 10000 | 0 | 79.126 | 126.380 | 5 | **11.500** | 10000 | 10000 | 7.909 | 7.620 | 12.950 |
| 10000 | 1 | 79.613 | 125.608 | 5 | **11.465** | 10000 | 10000 | 7.958 | 7.654 | 13.262 |
| 10000 | 2 | 80.080 | 124.876 | 5 | **11.493** | 10000 | 10000 | 8.004 | 7.737 | 13.824 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.110 | **9082.989** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.091 | **11024.994** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.098 | **10240.664** | 5 | 1000 | 1000 |
| 10000 | 0 | 0.738 | **13542.179** | 5 | 10000 | 10000 |
| 10000 | 1 | 0.719 | **13901.561** | 5 | 10000 | 10000 |
| 10000 | 2 | 0.732 | **13667.625** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.120 | **8318.799** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.116 | **8651.932** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.094 | **10677.738** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.146 | **8722.764** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.057 | **9458.869** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.144 | **8743.241** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.006** | 181076.066 | 0 | 1000 |
| 1000 | 1 | **0.005** | 189414.890 | 0 | 1000 |
| 1000 | 2 | **0.006** | 164911.051 | 0 | 1000 |
| 10000 | 0 | **0.014** | 703861.872 | 0 | 10000 |
| 10000 | 1 | **0.021** | 475506.471 | 0 | 10000 |
| 10000 | 2 | **0.018** | 549593.987 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.659 | **602.871** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.623 | **616.068** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.642 | **608.943** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 15.797 | **633.038** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 15.824 | **631.940** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 15.954 | **626.802** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 123.621 | 3001 | **55.605** | 7000 | 3000 | 4000 | 1000 | 17.984 | 8.089 |
| 1000 | 1 | 120.308 | 3001 | **52.910** | 7000 | 3000 | 4000 | 1000 | 18.900 | 8.312 |
| 1000 | 2 | 125.614 | 3001 | **53.781** | 7000 | 3000 | 4000 | 1000 | 18.594 | 7.961 |
| 10000 | 0 | 119.747 | 30001 | **51.640** | 70000 | 30000 | 40000 | 10000 | 193.649 | 83.509 |
| 10000 | 1 | 118.182 | 30001 | **51.284** | 70000 | 30000 | 40000 | 10000 | 194.991 | 84.615 |
| 10000 | 2 | 120.805 | 30001 | **51.803** | 70000 | 30000 | 40000 | 10000 | 193.037 | 82.778 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 190.472 | **89.003** | 1000 | 0 | 0 | 0 | 1000 | 11.236 | 5.250 | 1000 | True | 4 |
| 1000 | 1 | 135.516 | **84.589** | 1000 | 0 | 0 | 0 | 1000 | 11.822 | 7.379 | 1000 | True | 4 |
| 1000 | 2 | 130.582 | **85.316** | 1000 | 0 | 0 | 0 | 1000 | 11.721 | 7.658 | 1000 | True | 4 |
| 10000 | 0 | 131.532 | **92.166** | 10000 | 0 | 0 | 0 | 10000 | 108.500 | 76.027 | 10000 | True | 4 |
| 10000 | 1 | 158.243 | **87.484** | 10000 | 0 | 0 | 0 | 10000 | 114.306 | 63.194 | 10000 | True | 4 |
| 10000 | 2 | 116.063 | **81.737** | 10000 | 0 | 0 | 0 | 10000 | 122.344 | 86.160 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 10.549 | **94.797** | 5.007 | 1337 | 5.517 | 1000 | 1000 |
| 1000 | 1 | 11.305 | **88.453** | 5.315 | 1337 | 5.963 | 1000 | 1000 |
| 1000 | 2 | 11.489 | **87.039** | 5.476 | 1337 | 5.986 | 1000 | 1000 |
| 10000 | 0 | 76.733 | **130.322** | 34.106 | 13337 | 42.456 | 10000 | 10000 |
| 10000 | 1 | 95.474 | **104.741** | 45.400 | 13337 | 49.833 | 10000 | 10000 |
| 10000 | 2 | 100.149 | **99.852** | 48.244 | 13337 | 51.654 | 10000 | 10000 |

## Reproduce

```bash
docker compose up postgres -d
bin/benchmark.py all --backend postgres --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/postgres-20260527T034307Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/postgres-20260527T034307Z.jsonl --output docs/benchmarks/postgres.md
```
