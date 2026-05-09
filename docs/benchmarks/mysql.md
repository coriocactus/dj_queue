# dj_queue mysql Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-09T20:34:34.610110+00:00

## Environment

- backend: `mysql`
- database: `mysql` `dj_queue_benchmark`
- database version: `8.4.8`
- Python: `3.14.4`
- Django: `6.0.5`
- dj_queue: `0.6.4`
- platform: `macOS-26.4.1-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `ba09f796a9b1`
- benchmark worker count: `4`
- benchmark worker threads: `8`
- preserve finished jobs: `True`
- database CONN_MAX_AGE: `0`

## Results

### `single-enqueue`: one-by-one immediate enqueue latency and throughput

| size | run | duration_seconds | jobs_per_second | latency_p95_ms | ready_count | job_count | latency_mean_ms | latency_p50_ms | latency_p99_ms |
|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 4.556 | 219.478 | 7.545 | 1000 | 1000 | 4.555 | 4.031 | 9.993 |
| 1000 | 1 | 4.712 | 212.223 | 6.984 | 1000 | 1000 | 4.710 | 4.340 | 9.144 |
| 1000 | 2 | 4.516 | 221.446 | 5.998 | 1000 | 1000 | 4.514 | 4.369 | 7.491 |
| 10000 | 0 | 49.264 | 202.988 | 8.752 | 10000 | 10000 | 4.925 | 4.292 | 13.882 |
| 10000 | 1 | 48.195 | 207.490 | 7.617 | 10000 | 10000 | 4.818 | 4.364 | 13.198 |
| 10000 | 2 | 51.813 | 193.003 | 9.384 | 10000 | 10000 | 5.179 | 4.463 | 15.283 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

| size | run | duration_seconds | jobs_per_second | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.368 | 2717.398 | 5 | 1000 | 1000 |
| 1000 | 1 | 0.349 | 2865.142 | 5 | 1000 | 1000 |
| 1000 | 2 | 0.332 | 3013.148 | 5 | 1000 | 1000 |
| 10000 | 0 | 1.867 | 5356.948 | 5 | 10000 | 10000 |
| 10000 | 1 | 1.996 | 5011.107 | 5 | 10000 | 10000 |
| 10000 | 2 | 2.033 | 4919.093 | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

| size | run | duration_seconds | rows_per_second | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.142 | 7024.233 | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.140 | 7152.240 | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.125 | 8025.916 | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.063 | 9407.936 | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.031 | 9697.480 | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.060 | 9438.012 | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

| size | run | duration_seconds | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | 0.007 | 140317.221 | 0 | 1000 |
| 1000 | 1 | 0.007 | 136282.338 | 0 | 1000 |
| 1000 | 2 | 0.007 | 148865.835 | 0 | 1000 |
| 10000 | 0 | 0.020 | 488319.996 | 0 | 10000 |
| 10000 | 1 | 0.020 | 500782.472 | 0 | 10000 |
| 10000 | 2 | 0.020 | 494107.766 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

| size | run | duration_seconds | jobs_per_second | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 2.278 | 439.051 | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 2.280 | 438.503 | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 2.275 | 439.499 | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 22.944 | 435.842 | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 24.353 | 410.621 | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 23.201 | 431.015 | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

| size | run | enqueue_jobs_per_second | drain_jobs_per_second | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|
| 1000 | 0 | 156.083 | 55.381 | 1000 | 18.057 | 6.407 |
| 1000 | 1 | 218.558 | 48.633 | 1000 | 20.562 | 4.575 |
| 1000 | 2 | 169.506 | 48.403 | 1000 | 20.660 | 5.899 |
| 10000 | 0 | 205.275 | 49.475 | 10000 | 202.122 | 48.715 |
| 10000 | 1 | 204.663 | 49.076 | 10000 | 203.766 | 48.861 |
| 10000 | 2 | 201.079 | 49.047 | 10000 | 203.885 | 49.732 |

## Reproduce

```bash
docker compose up mysql -d
bin/benchmark.py all --backend mysql --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mysql-20260509T200949Z.jsonl
bin/benchmark.py report benchmark-results/mysql-20260509T200949Z.jsonl --output docs/benchmarks/mysql.md
```
