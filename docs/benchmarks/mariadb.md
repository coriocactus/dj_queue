# dj_queue mariadb Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-26T07:27:27.599166+00:00

## Environment

- backend: `mariadb`
- database: `mysql` `dj_queue_benchmark`
- database version: `10.6.25-MariaDB-ubu2204`
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
| 1000 | 0 | 7.637 | 130.938 | 5 | **11.461** | 1000 | 1000 | 7.634 | 6.957 | 15.770 |
| 1000 | 1 | 5.913 | 169.131 | 5 | **10.993** | 1000 | 1000 | 5.910 | 4.807 | 13.321 |
| 1000 | 2 | 7.739 | 129.214 | 5 | **11.624** | 1000 | 1000 | 7.736 | 7.023 | 15.424 |
| 10000 | 0 | 77.995 | 128.213 | 5 | **11.488** | 10000 | 10000 | 7.796 | 7.295 | 14.015 |
| 10000 | 1 | 77.596 | 128.872 | 5 | **11.472** | 10000 | 10000 | 7.756 | 7.074 | 14.057 |
| 10000 | 2 | 77.715 | 128.675 | 5 | **11.558** | 10000 | 10000 | 7.768 | 7.118 | 14.519 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.243 | **4119.783** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.255 | **3923.983** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.241 | **4152.549** | 5 | 1000 | 1000 |
| 10000 | 0 | 1.364 | **7332.151** | 5 | 10000 | 10000 |
| 10000 | 1 | 1.385 | **7219.563** | 5 | 10000 | 10000 |
| 10000 | 2 | 1.355 | **7380.226** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- healthy local baseline: `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.164 | **6083.417** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.150 | **6654.878** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.161 | **6201.552** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.353 | **7389.325** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.276 | **7838.711** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.389 | **7200.013** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- healthy local baseline: `<= 0.025 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.014** | 72252.597 | 0 | 1000 |
| 1000 | 1 | **0.010** | 96669.335 | 0 | 1000 |
| 1000 | 2 | **0.009** | 107040.114 | 0 | 1000 |
| 10000 | 0 | **0.034** | 292423.312 | 0 | 10000 |
| 10000 | 1 | **0.032** | 308231.713 | 0 | 10000 |
| 10000 | 2 | **0.033** | 299419.503 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- healthy local baseline: `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.386 | **721.572** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.313 | **761.440** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.381 | **723.883** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 12.702 | **787.254** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 12.709 | **786.848** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 12.902 | **775.071** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | enqueue_query_count | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 146.847 | 3001 | **42.681** | 11998 | 5000 | 6998 | 1000 | 23.430 | 6.810 |
| 1000 | 1 | 149.569 | 3001 | **43.312** | 11998 | 5000 | 6998 | 1000 | 23.088 | 6.686 |
| 1000 | 2 | 136.649 | 3001 | **43.321** | 11998 | 5000 | 6998 | 1000 | 23.084 | 7.318 |
| 10000 | 0 | 133.465 | 30001 | **47.900** | 119998 | 50000 | 69998 | 10000 | 208.767 | 74.926 |
| 10000 | 1 | 144.574 | 30001 | **47.442** | 119998 | 50000 | 69998 | 10000 | 210.782 | 69.169 |
| 10000 | 2 | 137.678 | 30001 | **48.627** | 119998 | 50000 | 69998 | 10000 | 205.649 | 72.633 |

### `runtime-hot-key-contention`: async runtime drain throughput for one hot concurrency key

- key metric: **`drain_jobs_per_second`** - real worker-runtime hot-key drain throughput after enqueue; higher is better
- healthy local baseline: `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes
- use case: runtime behavior for per-tenant, per-account, or external API limits under worker polling
- mechanics: enqueues jobs sharing one concurrency key, starts `AsyncSupervisor`, and waits for workers to drain through claim, execution, completion, semaphore handoff, and polling

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | finished_count | ready_count | blocked_count | claimed_count | completed_count | drain_duration_seconds | enqueue_duration_seconds | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 132.292 | **76.863** | 1000 | 0 | 0 | 0 | 1000 | 13.010 | 7.559 | 1000 | True | 4 |
| 1000 | 1 | 148.705 | **79.329** | 1000 | 0 | 0 | 0 | 1000 | 12.606 | 6.725 | 1000 | True | 4 |
| 1000 | 2 | 139.843 | **77.454** | 1000 | 0 | 0 | 0 | 1000 | 12.911 | 7.151 | 1000 | True | 4 |
| 10000 | 0 | 142.463 | **72.010** | 10000 | 0 | 0 | 0 | 10000 | 138.870 | 70.193 | 10000 | True | 4 |
| 10000 | 1 | 134.997 | **73.976** | 10000 | 0 | 0 | 0 | 10000 | 135.179 | 74.076 | 10000 | True | 4 |
| 10000 | 2 | 139.798 | **73.260** | 10000 | 0 | 0 | 0 | 10000 | 136.500 | 71.532 | 10000 | True | 4 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- healthy local baseline: `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_duration_seconds | claim_query_count | execute_duration_seconds | execute_query_count | finished_count |
|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 10.622 | **94.141** | 4.890 | 2005 | 5.709 | 2000 | 1000 |
| 1000 | 1 | 9.988 | **100.123** | 4.674 | 2005 | 5.292 | 2000 | 1000 |
| 1000 | 2 | 10.520 | **95.060** | 4.897 | 2005 | 5.599 | 2000 | 1000 |
| 10000 | 0 | 109.749 | **91.117** | 56.883 | 20005 | 52.643 | 20000 | 10000 |
| 10000 | 1 | 109.517 | **91.310** | 57.172 | 20005 | 52.121 | 20000 | 10000 |
| 10000 | 2 | 108.724 | **91.976** | 56.777 | 20005 | 51.723 | 20000 | 10000 |

## Reproduce

```bash
docker compose up mariadb -d
bin/benchmark.py all --backend mariadb --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mariadb-20260526T063600Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mariadb-20260526T063600Z.jsonl --output docs/benchmarks/mariadb.md
```
