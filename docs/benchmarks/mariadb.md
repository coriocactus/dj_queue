# dj_queue mariadb Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-18T14:33:52.837118+00:00

## Environment

- backend: `mariadb`
- database: `mysql` `dj_queue_benchmark`
- database version: `10.6.25-MariaDB-ubu2204`
- Python: `3.14.4`
- Django: `6.0.5`
- dj_queue: `0.9.1`
- platform: `macOS-26.4.1-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `464f2b27b6b3`
- benchmark worker count: `4`
- benchmark worker threads: `8`
- preserve finished jobs: `True`
- database CONN_MAX_AGE: `0`

## Results

### `single-enqueue`: one-by-one immediate enqueue latency and throughput

| size | run | duration_seconds | jobs_per_second | latency_p95_ms | ready_count | job_count | latency_mean_ms | latency_p50_ms | latency_p99_ms |
|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 6.535 | 153.029 | 12.420 | 1000 | 1000 | 6.531 | 6.104 | 15.190 |
| 1000 | 1 | 1.924 | 519.855 | 3.116 | 1000 | 1000 | 1.923 | 1.690 | 3.668 |
| 1000 | 2 | 1.964 | 509.271 | 3.074 | 1000 | 1000 | 1.963 | 1.778 | 3.710 |
| 10000 | 0 | 23.283 | 429.494 | 3.492 | 10000 | 10000 | 2.327 | 2.077 | 4.279 |
| 10000 | 1 | 22.460 | 445.244 | 3.318 | 10000 | 10000 | 2.245 | 2.005 | 3.756 |
| 10000 | 2 | 39.886 | 250.716 | 9.604 | 10000 | 10000 | 3.987 | 2.661 | 13.642 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

| size | run | duration_seconds | jobs_per_second | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.169 | 5925.563 | 5 | 1000 | 1000 |
| 1000 | 1 | 0.215 | 4658.876 | 5 | 1000 | 1000 |
| 1000 | 2 | 0.248 | 4027.774 | 5 | 1000 | 1000 |
| 10000 | 0 | 1.531 | 6532.975 | 5 | 10000 | 10000 |
| 10000 | 1 | 1.500 | 6664.655 | 5 | 10000 | 10000 |
| 10000 | 2 | 1.514 | 6603.868 | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

| size | run | duration_seconds | rows_per_second | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.151 | 6614.251 | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.134 | 7448.378 | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.135 | 7410.104 | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.192 | 8392.627 | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.071 | 9336.306 | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.073 | 9316.487 | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

| size | run | duration_seconds | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | 0.009 | 111923.590 | 0 | 1000 |
| 1000 | 1 | 0.009 | 110834.026 | 0 | 1000 |
| 1000 | 2 | 0.010 | 104647.663 | 0 | 1000 |
| 10000 | 0 | 0.021 | 472004.247 | 0 | 10000 |
| 10000 | 1 | 0.024 | 418488.837 | 0 | 10000 |
| 10000 | 2 | 0.024 | 414662.464 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

| size | run | duration_seconds | jobs_per_second | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 1.887 | 529.956 | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 1.962 | 509.695 | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 1.890 | 529.110 | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 18.851 | 530.462 | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 18.824 | 531.245 | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 18.814 | 531.521 | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

| size | run | enqueue_jobs_per_second | drain_jobs_per_second | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|
| 1000 | 0 | 285.636 | 38.720 | 1000 | 25.826 | 3.501 |
| 1000 | 1 | 232.826 | 47.513 | 1000 | 21.047 | 4.295 |
| 1000 | 2 | 490.473 | 72.029 | 1000 | 13.883 | 2.039 |
| 10000 | 0 | 462.228 | 65.167 | 10000 | 153.451 | 21.634 |
| 10000 | 1 | 466.074 | 27.427 | 10000 | 364.598 | 21.456 |
| 10000 | 2 | 132.587 | 28.992 | 10000 | 344.925 | 75.422 |

## Reproduce

```bash
docker compose up mariadb -d
bin/benchmark.py all --backend mariadb --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mariadb-20260518T140758Z.jsonl
bin/benchmark.py report benchmark-results/mariadb-20260518T140758Z.jsonl --output docs/benchmarks/mariadb.md
```
