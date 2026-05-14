# dj_queue mysql Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-14T20:25:12.369843+00:00

## Environment

- backend: `mysql`
- database: `mysql` `dj_queue_benchmark`
- database version: `8.4.8`
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
| 1000 | 0 | 4.330 | 230.956 | 6.233 | 1000 | 1000 | 4.328 | 4.003 | 9.115 |
| 1000 | 1 | 4.900 | 204.075 | 7.490 | 1000 | 1000 | 4.899 | 4.693 | 8.936 |
| 1000 | 2 | 4.864 | 205.610 | 7.248 | 1000 | 1000 | 4.862 | 4.687 | 9.017 |
| 10000 | 0 | 44.342 | 225.520 | 6.243 | 10000 | 10000 | 4.433 | 4.123 | 9.218 |
| 10000 | 1 | 48.864 | 204.650 | 8.545 | 10000 | 10000 | 4.885 | 4.183 | 15.127 |
| 10000 | 2 | 50.607 | 197.603 | 9.285 | 10000 | 10000 | 5.059 | 4.270 | 15.532 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

| size | run | duration_seconds | jobs_per_second | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.439 | 2276.848 | 5 | 1000 | 1000 |
| 1000 | 1 | 0.485 | 2061.412 | 5 | 1000 | 1000 |
| 1000 | 2 | 0.462 | 2165.792 | 5 | 1000 | 1000 |
| 10000 | 0 | 2.046 | 4886.983 | 5 | 10000 | 10000 |
| 10000 | 1 | 2.107 | 4746.478 | 5 | 10000 | 10000 |
| 10000 | 2 | 2.174 | 4598.763 | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

| size | run | duration_seconds | rows_per_second | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.174 | 5734.593 | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.144 | 6935.404 | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.170 | 5867.768 | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.042 | 9597.340 | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.118 | 8944.211 | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.102 | 9074.611 | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

| size | run | duration_seconds | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | 0.009 | 114110.740 | 0 | 1000 |
| 1000 | 1 | 0.007 | 139361.498 | 0 | 1000 |
| 1000 | 2 | 0.007 | 144971.303 | 0 | 1000 |
| 10000 | 0 | 0.020 | 499917.739 | 0 | 10000 |
| 10000 | 1 | 0.020 | 502566.228 | 0 | 10000 |
| 10000 | 2 | 0.021 | 473411.056 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

| size | run | duration_seconds | jobs_per_second | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 2.377 | 420.725 | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 2.303 | 434.162 | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 2.337 | 427.984 | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 23.001 | 434.755 | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 23.451 | 426.419 | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 23.203 | 430.976 | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

| size | run | enqueue_jobs_per_second | drain_jobs_per_second | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|
| 1000 | 0 | 251.381 | 48.864 | 1000 | 20.465 | 3.978 |
| 1000 | 1 | 188.392 | 48.599 | 1000 | 20.577 | 5.308 |
| 1000 | 2 | 228.223 | 46.309 | 1000 | 21.594 | 4.382 |
| 10000 | 0 | 200.919 | 46.005 | 10000 | 217.366 | 49.771 |
| 10000 | 1 | 197.361 | 47.402 | 10000 | 210.963 | 50.669 |
| 10000 | 2 | 204.809 | 45.412 | 10000 | 220.205 | 48.826 |

## Reproduce

```bash
docker compose up mysql -d
bin/benchmark.py all --backend mysql --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mysql-20260514T195930Z.jsonl
bin/benchmark.py report benchmark-results/mysql-20260514T195930Z.jsonl --output docs/benchmarks/mysql.md
```
