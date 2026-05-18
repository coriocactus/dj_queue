# dj_queue sqlite Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-18T15:11:57.645539+00:00

## Environment

- backend: `sqlite`
- database: `sqlite` `benchmark-results/dj_queue_benchmark.sqlite3`
- database version: `3.50.4`
- Python: `3.14.4`
- Django: `6.0.5`
- dj_queue: `0.9.1`
- platform: `macOS-26.4.1-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `3b590c28bbfd`
- benchmark worker count: `1`
- benchmark worker threads: `1`
- preserve finished jobs: `True`
- database CONN_MAX_AGE: `0`

## Results

### `single-enqueue`: one-by-one immediate enqueue latency and throughput

| size | run | duration_seconds | jobs_per_second | latency_p95_ms | ready_count | job_count | latency_mean_ms | latency_p50_ms | latency_p99_ms |
|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.115 | 896.774 | 1.340 | 1000 | 1000 | 1.115 | 0.973 | 1.793 |
| 1000 | 1 | 1.078 | 927.738 | 1.251 | 1000 | 1000 | 1.077 | 0.954 | 1.775 |
| 1000 | 2 | 1.005 | 994.995 | 1.360 | 1000 | 1000 | 1.005 | 0.945 | 1.628 |
| 10000 | 0 | 12.552 | 796.692 | 1.559 | 10000 | 10000 | 1.255 | 1.099 | 2.367 |
| 10000 | 1 | 12.023 | 831.747 | 1.574 | 10000 | 10000 | 1.202 | 1.062 | 4.458 |
| 10000 | 2 | 11.515 | 868.417 | 1.326 | 10000 | 10000 | 1.151 | 0.995 | 1.735 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

| size | run | duration_seconds | jobs_per_second | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.105 | 9521.142 | 5 | 1000 | 1000 |
| 1000 | 1 | 0.097 | 10279.376 | 5 | 1000 | 1000 |
| 1000 | 2 | 0.102 | 9804.662 | 5 | 1000 | 1000 |
| 10000 | 0 | 1.025 | 9754.419 | 10 | 10000 | 10000 |
| 10000 | 1 | 1.028 | 9731.396 | 10 | 10000 | 10000 |
| 10000 | 2 | 1.020 | 9800.908 | 10 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

| size | run | duration_seconds | rows_per_second | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.098 | 10249.927 | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.125 | 7970.845 | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.100 | 10021.061 | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.063 | 9411.279 | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.099 | 9102.621 | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.112 | 8991.928 | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

| size | run | duration_seconds | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | 0.002 | 414600.927 | 0 | 1000 |
| 1000 | 1 | 0.002 | 441639.157 | 0 | 1000 |
| 1000 | 2 | 0.002 | 499105.850 | 0 | 1000 |
| 10000 | 0 | 0.006 | 1686791.200 | 0 | 10000 |
| 10000 | 1 | 0.006 | 1629007.252 | 0 | 10000 |
| 10000 | 2 | 0.006 | 1593212.919 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

| size | run | duration_seconds | jobs_per_second | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 2.742 | 364.751 | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 1 | 2.746 | 364.190 | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 2 | 2.938 | 340.332 | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 10000 | 0 | 28.508 | 350.776 | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 1 | 29.213 | 342.318 | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 2 | 28.907 | 345.942 | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

| size | run | enqueue_jobs_per_second | drain_jobs_per_second | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|
| 1000 | 0 | 731.008 | 219.303 | 1000 | 4.560 | 1.368 |
| 1000 | 1 | 827.774 | 215.837 | 1000 | 4.633 | 1.208 |
| 1000 | 2 | 775.515 | 206.810 | 1000 | 4.835 | 1.289 |
| 10000 | 0 | 730.148 | 218.668 | 10000 | 45.732 | 13.696 |
| 10000 | 1 | 759.276 | 199.572 | 10000 | 50.107 | 13.170 |
| 10000 | 2 | 756.379 | 217.492 | 10000 | 45.979 | 13.221 |

## Reproduce

```bash
bin/benchmark.py all --backend sqlite --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/sqlite-20260518T150407Z.jsonl
bin/benchmark.py report benchmark-results/sqlite-20260518T150407Z.jsonl --output docs/benchmarks/sqlite.md
```
