# dj_queue sqlite Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-09T20:41:57.840938+00:00

## Environment

- backend: `sqlite`
- database: `sqlite` `benchmark-results/dj_queue_benchmark.sqlite3`
- database version: `3.50.4`
- Python: `3.14.4`
- Django: `6.0.5`
- dj_queue: `0.6.4`
- platform: `macOS-26.4.1-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `ba09f796a9b1`
- benchmark worker count: `1`
- benchmark worker threads: `1`
- preserve finished jobs: `True`
- database CONN_MAX_AGE: `0`

## Results

### `single-enqueue`: one-by-one immediate enqueue latency and throughput

| size | run | duration_seconds | jobs_per_second | latency_p95_ms | ready_count | job_count | latency_mean_ms | latency_p50_ms | latency_p99_ms |
|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.076 | 929.575 | 1.151 | 1000 | 1000 | 1.075 | 0.935 | 1.892 |
| 1000 | 1 | 0.951 | 1051.157 | 1.113 | 1000 | 1000 | 0.951 | 0.917 | 1.688 |
| 1000 | 2 | 1.730 | 578.030 | 1.379 | 1000 | 1000 | 1.729 | 0.931 | 2.365 |
| 10000 | 0 | 10.122 | 987.988 | 1.198 | 10000 | 10000 | 1.012 | 0.974 | 1.441 |
| 10000 | 1 | 10.074 | 992.635 | 1.177 | 10000 | 10000 | 1.007 | 0.964 | 1.403 |
| 10000 | 2 | 10.231 | 977.441 | 1.193 | 10000 | 10000 | 1.022 | 0.984 | 1.426 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

| size | run | duration_seconds | jobs_per_second | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.103 | 9704.722 | 5 | 1000 | 1000 |
| 1000 | 1 | 0.098 | 10244.034 | 5 | 1000 | 1000 |
| 1000 | 2 | 0.103 | 9695.775 | 5 | 1000 | 1000 |
| 10000 | 0 | 1.022 | 9781.721 | 10 | 10000 | 10000 |
| 10000 | 1 | 1.017 | 9828.850 | 10 | 10000 | 10000 |
| 10000 | 2 | 1.013 | 9875.730 | 10 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

| size | run | duration_seconds | rows_per_second | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.101 | 9925.074 | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.098 | 10157.466 | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.100 | 9977.252 | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.035 | 9659.786 | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.037 | 9639.144 | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.055 | 9479.563 | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

| size | run | duration_seconds | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | 0.002 | 502681.060 | 0 | 1000 |
| 1000 | 1 | 0.002 | 509424.354 | 0 | 1000 |
| 1000 | 2 | 0.002 | 513303.282 | 0 | 1000 |
| 10000 | 0 | 0.006 | 1666643.612 | 0 | 10000 |
| 10000 | 1 | 0.005 | 1823306.305 | 0 | 10000 |
| 10000 | 2 | 0.006 | 1736186.476 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

| size | run | duration_seconds | jobs_per_second | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 2.887 | 346.426 | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 1 | 2.822 | 354.413 | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 1000 | 2 | 2.858 | 349.909 | 1000 | 0 | 0 | 1000 | 1000 | True | 1 |
| 10000 | 0 | 28.901 | 346.011 | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 1 | 28.852 | 346.596 | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |
| 10000 | 2 | 28.548 | 350.286 | 10000 | 0 | 0 | 10000 | 10000 | True | 1 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

| size | run | enqueue_jobs_per_second | drain_jobs_per_second | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|
| 1000 | 0 | 883.049 | 226.194 | 1000 | 4.421 | 1.132 |
| 1000 | 1 | 862.825 | 226.187 | 1000 | 4.421 | 1.159 |
| 1000 | 2 | 886.768 | 228.481 | 1000 | 4.377 | 1.128 |
| 10000 | 0 | 849.922 | 223.521 | 10000 | 44.739 | 11.766 |
| 10000 | 1 | 833.470 | 224.291 | 10000 | 44.585 | 11.998 |
| 10000 | 2 | 831.553 | 222.888 | 10000 | 44.866 | 12.026 |

## Reproduce

```bash
bin/benchmark.py all --backend sqlite --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/sqlite-20260509T203434Z.jsonl
bin/benchmark.py report benchmark-results/sqlite-20260509T203434Z.jsonl --output docs/benchmarks/sqlite.md
```
