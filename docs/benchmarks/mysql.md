# dj_queue mysql Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-18T15:04:07.466959+00:00

## Environment

- backend: `mysql`
- database: `mysql` `dj_queue_benchmark`
- database version: `8.4.8`
- Python: `3.14.4`
- Django: `6.0.5`
- dj_queue: `0.9.1`
- platform: `macOS-26.4.1-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `3b590c28bbfd`
- benchmark worker count: `4`
- benchmark worker threads: `8`
- preserve finished jobs: `True`
- database CONN_MAX_AGE: `0`

## Results

### `single-enqueue`: one-by-one immediate enqueue latency and throughput

| size | run | duration_seconds | jobs_per_second | latency_p95_ms | ready_count | job_count | latency_mean_ms | latency_p50_ms | latency_p99_ms |
|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 6.221 | 160.754 | 9.336 | 1000 | 1000 | 6.217 | 5.875 | 11.766 |
| 1000 | 1 | 5.633 | 177.512 | 11.015 | 1000 | 1000 | 5.631 | 4.699 | 13.628 |
| 1000 | 2 | 7.453 | 134.171 | 11.700 | 1000 | 1000 | 7.450 | 6.959 | 14.699 |
| 10000 | 0 | 62.822 | 159.181 | 10.399 | 10000 | 10000 | 6.279 | 5.680 | 16.447 |
| 10000 | 1 | 62.869 | 159.060 | 9.967 | 10000 | 10000 | 6.284 | 5.821 | 15.058 |
| 10000 | 2 | 59.926 | 166.873 | 9.095 | 10000 | 10000 | 5.990 | 5.483 | 13.674 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

| size | run | duration_seconds | jobs_per_second | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.569 | 1757.316 | 5 | 1000 | 1000 |
| 1000 | 1 | 0.542 | 1843.712 | 5 | 1000 | 1000 |
| 1000 | 2 | 0.288 | 3474.947 | 5 | 1000 | 1000 |
| 10000 | 0 | 1.898 | 5269.203 | 5 | 10000 | 10000 |
| 10000 | 1 | 1.749 | 5716.155 | 5 | 10000 | 10000 |
| 10000 | 2 | 1.939 | 5158.580 | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

| size | run | duration_seconds | rows_per_second | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.228 | 4383.723 | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.200 | 4994.965 | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.150 | 6667.280 | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.044 | 9582.308 | 10000 | 10000 | 10000 |
| 10000 | 1 | 0.996 | 10044.266 | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.035 | 9664.701 | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

| size | run | duration_seconds | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | 0.012 | 83767.002 | 0 | 1000 |
| 1000 | 1 | 0.014 | 73676.581 | 0 | 1000 |
| 1000 | 2 | 0.017 | 60350.635 | 0 | 1000 |
| 10000 | 0 | 0.021 | 469294.463 | 0 | 10000 |
| 10000 | 1 | 0.018 | 556809.646 | 0 | 10000 |
| 10000 | 2 | 0.022 | 464728.089 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

| size | run | duration_seconds | jobs_per_second | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.857 | 538.537 | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.859 | 537.903 | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.919 | 521.017 | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 19.259 | 519.243 | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 19.300 | 518.141 | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 18.966 | 527.268 | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

| size | run | enqueue_jobs_per_second | drain_jobs_per_second | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|
| 1000 | 0 | 216.723 | 44.811 | 1000 | 22.316 | 4.614 |
| 1000 | 1 | 235.967 | 51.693 | 1000 | 19.345 | 4.238 |
| 1000 | 2 | 260.569 | 42.694 | 1000 | 23.422 | 3.838 |
| 10000 | 0 | 175.556 | 33.354 | 10000 | 299.817 | 56.962 |
| 10000 | 1 | 164.641 | 37.498 | 10000 | 266.679 | 60.738 |
| 10000 | 2 | 201.563 | 44.486 | 10000 | 224.791 | 49.612 |

## Reproduce

```bash
docker compose up mysql -d
bin/benchmark.py all --backend mysql --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mysql-20260518T143352Z.jsonl
bin/benchmark.py report benchmark-results/mysql-20260518T143352Z.jsonl --output docs/benchmarks/mysql.md
```
