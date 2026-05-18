# dj_queue PostgreSQL Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-18T14:07:58.890939+00:00

## Environment

- backend: `postgres`
- database: `postgresql` `dj_queue_benchmark`
- database version: `PostgreSQL 17.9 (Debian 17.9-1.pgdg13+1) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit`
- Python: `3.14.4`
- Django: `6.0.5`
- dj_queue: `0.9.1`
- platform: `macOS-26.4.1-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `464f2b27b6b3`
- benchmark worker count: `4`
- benchmark worker threads: `8`
- preserve finished jobs: `True`
- database CONN_MAX_AGE: `60`

## Results

### `single-enqueue`: one-by-one immediate enqueue latency and throughput

| size | run | duration_seconds | jobs_per_second | latency_p95_ms | ready_count | job_count | latency_mean_ms | latency_p50_ms | latency_p99_ms |
|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 8.349 | 119.774 | 12.456 | 1000 | 1000 | 8.345 | 8.172 | 14.256 |
| 1000 | 1 | 8.266 | 120.984 | 12.439 | 1000 | 1000 | 8.262 | 7.984 | 14.954 |
| 1000 | 2 | 8.232 | 121.482 | 12.564 | 1000 | 1000 | 8.228 | 8.008 | 14.534 |
| 10000 | 0 | 83.703 | 119.470 | 12.426 | 10000 | 10000 | 8.366 | 8.128 | 14.954 |
| 10000 | 1 | 83.037 | 120.428 | 12.345 | 10000 | 10000 | 8.300 | 8.090 | 14.553 |
| 10000 | 2 | 83.862 | 119.243 | 12.412 | 10000 | 10000 | 8.382 | 8.162 | 14.464 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

| size | run | duration_seconds | jobs_per_second | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.141 | 7078.617 | 5 | 1000 | 1000 |
| 1000 | 1 | 0.118 | 8465.895 | 5 | 1000 | 1000 |
| 1000 | 2 | 0.131 | 7613.049 | 5 | 1000 | 1000 |
| 10000 | 0 | 1.148 | 8710.978 | 5 | 10000 | 10000 |
| 10000 | 1 | 1.205 | 8299.300 | 5 | 10000 | 10000 |
| 10000 | 2 | 1.192 | 8392.539 | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

| size | run | duration_seconds | rows_per_second | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.116 | 8642.305 | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.103 | 9709.649 | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.117 | 8559.528 | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.178 | 8490.271 | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.187 | 8426.451 | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.162 | 8604.503 | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

| size | run | duration_seconds | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | 0.004 | 285018.680 | 0 | 1000 |
| 1000 | 1 | 0.004 | 238352.999 | 0 | 1000 |
| 1000 | 2 | 0.004 | 244180.323 | 0 | 1000 |
| 10000 | 0 | 0.007 | 1533536.520 | 0 | 10000 |
| 10000 | 1 | 0.006 | 1650936.827 | 0 | 10000 |
| 10000 | 2 | 0.006 | 1572759.789 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

| size | run | duration_seconds | jobs_per_second | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.948 | 513.280 | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.980 | 505.047 | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.978 | 505.479 | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 19.530 | 512.030 | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 19.221 | 520.277 | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 19.719 | 507.118 | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

| size | run | enqueue_jobs_per_second | drain_jobs_per_second | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|
| 1000 | 0 | 93.369 | 25.292 | 1000 | 39.539 | 10.710 |
| 1000 | 1 | 92.860 | 24.888 | 1000 | 40.179 | 10.769 |
| 1000 | 2 | 90.743 | 24.878 | 1000 | 40.197 | 11.020 |
| 10000 | 0 | 92.805 | 21.672 | 10000 | 461.432 | 107.753 |
| 10000 | 1 | 89.478 | 25.827 | 10000 | 387.196 | 111.759 |
| 10000 | 2 | 93.527 | 27.199 | 10000 | 367.657 | 106.921 |

## Reproduce

```bash
docker compose up postgres -d
bin/benchmark.py all --backend postgres --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/postgres-20260518T132211Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/postgres-20260518T132211Z.jsonl --output docs/benchmarks/postgres.md
```
