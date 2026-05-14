# dj_queue sqlite Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-14T20:32:34.784510+00:00

## Environment

- backend: `sqlite`
- database: `sqlite` `benchmark-results/dj_queue_benchmark.sqlite3`
- database version: `3.50.4`
- Python: `3.14.4`
- Django: `6.0.5`
- dj_queue: `0.9.1`
- platform: `macOS-26.4.1-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `8f99214ddef4`
- benchmark worker count: `1`
- benchmark worker threads: `1`
- preserve finished jobs: `True`
- database CONN_MAX_AGE: `0`

## Results

### `single-enqueue`: one-by-one immediate enqueue latency and throughput

| size | run | duration_seconds | jobs_per_second | latency_p95_ms | ready_count | job_count | latency_mean_ms | latency_p50_ms | latency_p99_ms |
|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 0.973 | 1027.920 | 1.158 | 1000 | 1000 | 0.972 | 0.923 | 1.764 |
| 1000 | 1 | 1.128 | 886.286 | 1.471 | 1000 | 1000 | 1.128 | 0.980 | 2.262 |
| 1000 | 2 | 1.149 | 870.010 | 1.278 | 1000 | 1000 | 1.149 | 1.006 | 1.543 |
| 10000 | 0 | 10.050 | 995.063 | 1.178 | 10000 | 10000 | 1.004 | 0.975 | 1.383 |
| 10000 | 1 | 9.867 | 1013.470 | 1.148 | 10000 | 10000 | 0.986 | 0.956 | 1.287 |
| 10000 | 2 | 10.076 | 992.493 | 1.181 | 10000 | 10000 | 1.007 | 0.982 | 1.352 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

| size | run | duration_seconds | jobs_per_second | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.103 | 9746.387 | 5 | 1000 | 1000 |
| 1000 | 1 | 0.097 | 10349.561 | 5 | 1000 | 1000 |
| 1000 | 2 | 0.101 | 9858.737 | 5 | 1000 | 1000 |
| 10000 | 0 | 0.995 | 10047.269 | 10 | 10000 | 10000 |
| 10000 | 1 | 1.001 | 9985.193 | 10 | 10000 | 10000 |
| 10000 | 2 | 1.002 | 9975.080 | 10 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

| size | run | duration_seconds | rows_per_second | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.095 | 10502.657 | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.098 | 10183.049 | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.101 | 9870.097 | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.021 | 9794.910 | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.017 | 9836.292 | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.029 | 9719.756 | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

| size | run | duration_seconds | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | 0.002 | 427632.232 | 0 | 1000 |
| 1000 | 1 | 0.002 | 496493.515 | 0 | 1000 |
| 1000 | 2 | 0.002 | 514072.749 | 0 | 1000 |
| 10000 | 0 | 0.005 | 1875073.242 | 0 | 10000 |
| 10000 | 1 | 0.005 | 1840123.811 | 0 | 10000 |
| 10000 | 2 | 0.005 | 1909444.599 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

| size | run | duration_seconds | jobs_per_second | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 2.857 | 349.981 | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 1 | 2.809 | 356.036 | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 2 | 2.851 | 350.802 | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 10000 | 0 | 28.979 | 345.080 | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 1 | 28.718 | 348.213 | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 2 | 28.920 | 345.782 | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

| size | run | enqueue_jobs_per_second | drain_jobs_per_second | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|
| 1000 | 0 | 874.649 | 226.585 | 1000 | 4.413 | 1.143 |
| 1000 | 1 | 855.069 | 228.558 | 1000 | 4.375 | 1.169 |
| 1000 | 2 | 865.460 | 225.669 | 1000 | 4.431 | 1.155 |
| 10000 | 0 | 847.076 | 222.387 | 10000 | 44.967 | 11.805 |
| 10000 | 1 | 831.706 | 221.403 | 10000 | 45.166 | 12.023 |
| 10000 | 2 | 840.306 | 221.510 | 10000 | 45.145 | 11.900 |

## Reproduce

```bash
bin/benchmark.py all --backend sqlite --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/sqlite-20260514T202512Z.jsonl
bin/benchmark.py report benchmark-results/sqlite-20260514T202512Z.jsonl --output docs/benchmarks/sqlite.md
```
