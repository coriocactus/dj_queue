# dj_queue mysql Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-23T11:43:11.689387+00:00

## Environment

- backend: `mysql`
- database: `mysql` `dj_queue_benchmark`
- database version: `8.4.8`
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
- good number: `<= 20 ms` for request-path enqueue on the 10k local benchmark
- use case: web requests, admin actions, and small fan-out paths that submit tasks one at a time
- mechanics: calls the public `Task.enqueue()` path once per job, including validation, job insert, ready-row insert, result mapping, and ready wakeup registration

| size | run | duration_seconds | jobs_per_second | **latency_p95_ms** | ready_count | job_count | latency_mean_ms | latency_p50_ms | latency_p99_ms |
|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 5.270 | 189.737 | **7.454** | 1000 | 1000 | 5.269 | 5.099 | 9.355 |
| 1000 | 1 | 4.562 | 219.211 | **6.235** | 1000 | 1000 | 4.561 | 4.319 | 8.231 |
| 1000 | 2 | 4.628 | 216.087 | **6.037** | 1000 | 1000 | 4.626 | 4.477 | 8.534 |
| 10000 | 0 | 52.306 | 191.183 | **9.050** | 10000 | 10000 | 5.229 | 4.660 | 14.264 |
| 10000 | 1 | 68.354 | 146.297 | **12.014** | 10000 | 10000 | 6.833 | 6.039 | 19.301 |
| 10000 | 2 | 76.628 | 130.501 | **11.193** | 10000 | 10000 | 7.660 | 7.184 | 16.716 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

- key metric: **`jobs_per_second`** - bulk enqueue throughput; `query_count` should stay nearly fixed as size grows
- good number: `>= 5,000 jobs/sec` for 10k independent immediate jobs
- use case: imports, backfills, and fan-out jobs that enqueue many independent tasks
- mechanics: calls `DjQueueBackend.enqueue_all()` for immediate unconstrained jobs, including bulk job and ready-row inserts plus batched result creation

| size | run | duration_seconds | **jobs_per_second** | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.464 | **2156.670** | 5 | 1000 | 1000 |
| 1000 | 1 | 0.294 | **3406.779** | 5 | 1000 | 1000 |
| 1000 | 2 | 0.295 | **3391.900** | 5 | 1000 | 1000 |
| 10000 | 0 | 1.949 | **5130.698** | 5 | 10000 | 10000 |
| 10000 | 1 | 1.874 | **5337.023** | 5 | 10000 | 10000 |
| 10000 | 2 | 2.214 | **4516.435** | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

- key metric: **`rows_per_second`** - due scheduled-row promotion throughput; higher is better
- good number: `>= 5,000 rows/sec` for a 10k due-row promotion burst
- use case: delayed-job bursts where the dispatcher must move due work into ready state
- mechanics: seeds equal due and future scheduled backlogs, then calls `promote_scheduled_jobs()` in batches until no due rows remain

| size | run | duration_seconds | **rows_per_second** | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.282 | **3544.046** | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.262 | **3812.347** | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.226 | **4430.093** | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.152 | **8678.565** | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.095 | **9132.643** | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.179 | **8482.426** | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

- key metric: **`duration_seconds`** - scheduler no-op poll duration over persisted not-due recurring rows; lower is better
- good number: `<= 0.050 seconds` for a no-op poll over 10k not-due schedules
- use case: large recurring-task catalogs where most scheduler ticks should be cheap no-ops
- mechanics: seeds dynamic recurring definitions with future `next_run_at` values, then runs one `Scheduler.poll_once()` without firing jobs

| size | run | **duration_seconds** | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | **0.008** | 130267.700 | 0 | 1000 |
| 1000 | 1 | **0.011** | 94594.331 | 0 | 1000 |
| 1000 | 2 | **0.007** | 137731.561 | 0 | 1000 |
| 10000 | 0 | **0.021** | 465785.168 | 0 | 10000 |
| 10000 | 1 | **0.020** | 506682.920 | 0 | 10000 |
| 10000 | 2 | **0.020** | 496243.043 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

- key metric: **`jobs_per_second`** - end-to-end ready-job drain throughput through the async runtime; higher is better
- good number: `>= 250 jobs/sec` for draining 10k no-op ready jobs
- use case: steady ready-queue processing by embedded or standalone async workers
- mechanics: seeds ready rows directly, starts `AsyncSupervisor`, and drains no-op jobs through worker claim, execution, completion, and finished-job retention

| size | run | duration_seconds | **jobs_per_second** | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 2.321 | **430.906** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 2.350 | **425.618** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 2.344 | **426.599** | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 24.793 | **403.336** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 21.899 | **456.646** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 21.894 | **456.736** | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

- key metric: **`drain_jobs_per_second`** - serialized hot-key drain throughput after enqueue; higher is better
- good number: `>= 25 jobs/sec` for a 10k serialized hot-key drain
- use case: per-tenant, per-account, or external API limits where one hot key must serialize work
- mechanics: enqueues jobs sharing one concurrency key so all but one block, then drains with `claim_ready_jobs()` and `execute_claimed_job()` to cover semaphore handoff and unblock

| size | run | enqueue_jobs_per_second | **drain_jobs_per_second** | drain_query_count | claim_query_count | execute_query_count | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 130.618 | **81.781** | 15995 | 4000 | 11995 | 1000 | 12.228 | 7.656 |
| 1000 | 1 | 135.535 | **81.289** | 15995 | 4000 | 11995 | 1000 | 12.302 | 7.378 |
| 1000 | 2 | 203.915 | **73.300** | 15995 | 4000 | 11995 | 1000 | 13.643 | 4.904 |
| 10000 | 0 | 178.976 | **73.892** | 159995 | 40000 | 119995 | 10000 | 135.332 | 55.874 |
| 10000 | 1 | 171.262 | **75.995** | 159995 | 40000 | 119995 | 10000 | 131.588 | 58.390 |
| 10000 | 2 | 188.033 | **75.737** | 159995 | 40000 | 119995 | 10000 | 132.035 | 53.182 |

### `ordered-selector-claim`: ordered exact-queue claiming and drain throughput

- key metric: **`jobs_per_second`** - selector-heavy claim and drain throughput; higher is better
- good number: `>= 50 jobs/sec` for a 10k exact-selector drain
- use case: workers with ordered queue preferences, priority lanes, or queue-isolated tenants
- mechanics: seeds three queues and drains with exact ordered selectors to cover selector ordering, claim locking, query shape, and completion

| size | run | duration_seconds | **jobs_per_second** | claim_query_count | finished_count |
|---|---|---|---|---|---|
| 1000 | 0 | 9.206 | **108.623** | 1336 | 1000 |
| 1000 | 1 | 9.161 | **109.163** | 1336 | 1000 |
| 1000 | 2 | 10.049 | **99.509** | 1336 | 1000 |
| 10000 | 0 | 183.002 | **54.644** | 13336 | 10000 |
| 10000 | 1 | 182.073 | **54.923** | 13336 | 10000 |
| 10000 | 2 | 178.791 | **55.931** | 13336 | 10000 |

## Reproduce

```bash
docker compose up mysql -d
bin/benchmark.py all --backend mysql --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mysql-20260523T084351Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/mysql-20260523T084351Z.jsonl --output docs/benchmarks/mysql.md
```
