# dj_queue mariadb Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-14T19:59:30.667589+00:00

## Environment

- backend: `mariadb`
- database: `mysql` `dj_queue_benchmark`
- database version: `10.6.25-MariaDB-ubu2204`
- Python: `3.14.4`
- Django: `6.0.5`
- dj_queue: `0.9.1`
- platform: `macOS-26.4.1-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `8f99214ddef4`
- benchmark worker count: `4`
- benchmark worker threads: `8`
- preserve finished jobs: `True`
- database CONN_MAX_AGE: `0`

## Results

### `single-enqueue`: one-by-one immediate enqueue latency and throughput

| size | run | duration_seconds | jobs_per_second | latency_p95_ms | ready_count | job_count | latency_mean_ms | latency_p50_ms | latency_p99_ms |
|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 7.612 | 131.367 | 11.716 | 1000 | 1000 | 7.608 | 6.770 | 14.986 |
| 1000 | 1 | 7.988 | 125.189 | 12.358 | 1000 | 1000 | 7.984 | 7.456 | 15.310 |
| 1000 | 2 | 7.595 | 131.669 | 11.514 | 1000 | 1000 | 7.591 | 6.845 | 14.086 |
| 10000 | 0 | 78.098 | 128.043 | 12.344 | 10000 | 10000 | 7.806 | 6.981 | 15.561 |
| 10000 | 1 | 75.830 | 131.873 | 11.976 | 10000 | 10000 | 7.579 | 6.806 | 13.778 |
| 10000 | 2 | 74.460 | 134.300 | 11.979 | 10000 | 10000 | 7.442 | 6.738 | 13.918 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

| size | run | duration_seconds | jobs_per_second | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.277 | 3603.903 | 5 | 1000 | 1000 |
| 1000 | 1 | 0.267 | 3747.717 | 5 | 1000 | 1000 |
| 1000 | 2 | 0.286 | 3490.642 | 5 | 1000 | 1000 |
| 10000 | 0 | 1.658 | 6032.245 | 5 | 10000 | 10000 |
| 10000 | 1 | 1.637 | 6108.083 | 5 | 10000 | 10000 |
| 10000 | 2 | 1.720 | 5812.679 | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

| size | run | duration_seconds | rows_per_second | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.165 | 6063.039 | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.148 | 6750.329 | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.168 | 5947.849 | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.228 | 8145.447 | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.212 | 8250.589 | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.188 | 8419.304 | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

| size | run | duration_seconds | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | 0.008 | 123177.359 | 0 | 1000 |
| 1000 | 1 | 0.008 | 131337.736 | 0 | 1000 |
| 1000 | 2 | 0.015 | 66254.598 | 0 | 1000 |
| 10000 | 0 | 0.023 | 437422.205 | 0 | 10000 |
| 10000 | 1 | 0.018 | 553913.051 | 0 | 10000 |
| 10000 | 2 | 0.031 | 318358.542 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

| size | run | duration_seconds | jobs_per_second | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 2.286 | 437.475 | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 2.244 | 445.695 | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 2.254 | 443.677 | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 22.430 | 445.835 | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 22.627 | 441.942 | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 22.412 | 446.192 | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

| size | run | enqueue_jobs_per_second | drain_jobs_per_second | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|
| 1000 | 0 | 155.851 | 29.692 | 1000 | 33.679 | 6.416 |
| 1000 | 1 | 146.556 | 29.822 | 1000 | 33.532 | 6.823 |
| 1000 | 2 | 160.633 | 29.810 | 1000 | 33.546 | 6.225 |
| 10000 | 0 | 368.622 | 37.938 | 10000 | 263.588 | 27.128 |
| 10000 | 1 | 415.493 | 36.072 | 10000 | 277.226 | 24.068 |
| 10000 | 2 | 174.284 | 36.064 | 10000 | 277.283 | 57.378 |

## Reproduce

```bash
docker compose up mariadb -d
bin/benchmark.py all --backend mariadb --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mariadb-20260514T192741Z.jsonl
bin/benchmark.py report benchmark-results/mariadb-20260514T192741Z.jsonl --output docs/benchmarks/mariadb.md
```
