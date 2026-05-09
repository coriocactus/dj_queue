# dj_queue PostgreSQL Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-09T19:41:18.559984+00:00

## Environment

- backend: `postgres`
- database: `postgresql` `dj_queue_benchmark`
- database version: `PostgreSQL 17.9 (Debian 17.9-1.pgdg13+1) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit`
- Python: `3.14.4`
- Django: `6.0.5`
- dj_queue: `0.6.4`
- platform: `macOS-26.4.1-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `ba09f796a9b1`
- benchmark worker count: `4`
- benchmark worker threads: `8`
- preserve finished jobs: `True`
- database CONN_MAX_AGE: `60`

## Results

### `single-enqueue`: one-by-one immediate enqueue latency and throughput

| size | run | duration_seconds | jobs_per_second | latency_p95_ms | ready_count | job_count | latency_mean_ms | latency_p50_ms | latency_p99_ms |
|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 8.840 | 113.128 | 13.020 | 1000 | 1000 | 8.836 | 8.298 | 15.119 |
| 1000 | 1 | 8.754 | 114.232 | 13.252 | 1000 | 1000 | 8.750 | 8.197 | 15.109 |
| 1000 | 2 | 9.325 | 107.237 | 12.635 | 1000 | 1000 | 9.322 | 9.849 | 13.774 |
| 10000 | 0 | 91.984 | 108.714 | 12.382 | 10000 | 10000 | 9.195 | 10.009 | 14.064 |
| 10000 | 1 | 89.989 | 111.125 | 12.822 | 10000 | 10000 | 8.995 | 8.670 | 14.664 |
| 10000 | 2 | 83.325 | 120.012 | 12.534 | 10000 | 10000 | 8.328 | 7.952 | 15.403 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

| size | run | duration_seconds | jobs_per_second | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.152 | 6591.351 | 5 | 1000 | 1000 |
| 1000 | 1 | 0.145 | 6914.137 | 5 | 1000 | 1000 |
| 1000 | 2 | 0.146 | 6846.369 | 5 | 1000 | 1000 |
| 10000 | 0 | 1.248 | 8012.108 | 5 | 10000 | 10000 |
| 10000 | 1 | 1.262 | 7925.743 | 5 | 10000 | 10000 |
| 10000 | 2 | 1.220 | 8194.150 | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

| size | run | duration_seconds | rows_per_second | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.106 | 9404.709 | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.115 | 8675.265 | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.098 | 10237.921 | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.090 | 9175.298 | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.146 | 8723.085 | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.069 | 9353.140 | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

| size | run | duration_seconds | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | 0.004 | 259920.312 | 0 | 1000 |
| 1000 | 1 | 0.005 | 215532.756 | 0 | 1000 |
| 1000 | 2 | 0.006 | 165122.086 | 0 | 1000 |
| 10000 | 0 | 0.007 | 1338710.490 | 0 | 10000 |
| 10000 | 1 | 0.008 | 1225333.874 | 0 | 10000 |
| 10000 | 2 | 0.012 | 862936.887 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

| size | run | duration_seconds | jobs_per_second | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 2.478 | 403.504 | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 2.484 | 402.597 | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 2.587 | 386.502 | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 26.446 | 378.134 | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 28.199 | 354.619 | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 26.032 | 384.148 | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

| size | run | enqueue_jobs_per_second | drain_jobs_per_second | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|
| 1000 | 0 | 99.663 | 25.307 | 1000 | 39.514 | 10.034 |
| 1000 | 1 | 100.560 | 25.702 | 1000 | 38.907 | 9.944 |
| 1000 | 2 | 91.452 | 25.598 | 1000 | 39.066 | 10.935 |
| 10000 | 0 | 371.859 | 29.061 | 10000 | 344.101 | 26.892 |
| 10000 | 1 | 277.886 | 27.501 | 10000 | 363.617 | 35.986 |
| 10000 | 2 | 393.634 | 28.249 | 10000 | 353.998 | 25.404 |

## Reproduce

```bash
docker compose up postgres -d
bin/benchmark.py all --backend postgres --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/postgres-20260509T190146Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/postgres-20260509T190146Z.jsonl --output docs/benchmarks/postgres.md
```
