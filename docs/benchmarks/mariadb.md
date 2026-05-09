# dj_queue mariadb Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-09T20:09:49.810350+00:00

## Environment

- backend: `mariadb`
- database: `mysql` `dj_queue_benchmark`
- database version: `10.6.25-MariaDB-ubu2204`
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
| 1000 | 0 | 5.038 | 198.505 | 11.066 | 1000 | 1000 | 5.035 | 4.083 | 13.528 |
| 1000 | 1 | 6.638 | 150.657 | 12.273 | 1000 | 1000 | 6.635 | 5.944 | 16.094 |
| 1000 | 2 | 6.374 | 156.894 | 11.788 | 1000 | 1000 | 6.371 | 5.506 | 14.401 |
| 10000 | 0 | 68.917 | 145.101 | 11.748 | 10000 | 10000 | 6.888 | 6.441 | 14.270 |
| 10000 | 1 | 69.499 | 143.887 | 12.044 | 10000 | 10000 | 6.947 | 6.520 | 14.323 |
| 10000 | 2 | 67.298 | 148.594 | 11.881 | 10000 | 10000 | 6.727 | 6.254 | 14.473 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

| size | run | duration_seconds | jobs_per_second | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.250 | 3992.478 | 5 | 1000 | 1000 |
| 1000 | 1 | 0.238 | 4196.872 | 5 | 1000 | 1000 |
| 1000 | 2 | 0.270 | 3704.900 | 5 | 1000 | 1000 |
| 10000 | 0 | 1.496 | 6685.005 | 5 | 10000 | 10000 |
| 10000 | 1 | 1.435 | 6968.071 | 5 | 10000 | 10000 |
| 10000 | 2 | 1.555 | 6431.928 | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

| size | run | duration_seconds | rows_per_second | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.118 | 8476.402 | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.113 | 8875.047 | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.143 | 7012.510 | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.109 | 9017.230 | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.130 | 8849.988 | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.065 | 9391.746 | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

| size | run | duration_seconds | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | 0.008 | 128818.934 | 0 | 1000 |
| 1000 | 1 | 0.009 | 117067.465 | 0 | 1000 |
| 1000 | 2 | 0.011 | 90603.305 | 0 | 1000 |
| 10000 | 0 | 0.028 | 361732.873 | 0 | 10000 |
| 10000 | 1 | 0.029 | 347498.737 | 0 | 10000 |
| 10000 | 2 | 0.029 | 345867.179 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

| size | run | duration_seconds | jobs_per_second | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 2.190 | 456.563 | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 2.241 | 446.213 | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 2.248 | 444.865 | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 22.106 | 452.359 | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 22.031 | 453.899 | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 22.205 | 450.349 | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

| size | run | enqueue_jobs_per_second | drain_jobs_per_second | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|
| 1000 | 0 | 275.204 | 34.276 | 1000 | 29.175 | 3.634 |
| 1000 | 1 | 422.231 | 38.434 | 1000 | 26.019 | 2.368 |
| 1000 | 2 | 409.245 | 32.652 | 1000 | 30.626 | 2.444 |
| 10000 | 0 | 404.177 | 39.218 | 10000 | 254.986 | 24.742 |
| 10000 | 1 | 426.613 | 37.662 | 10000 | 265.519 | 23.440 |
| 10000 | 2 | 395.213 | 37.067 | 10000 | 269.785 | 25.303 |

## Reproduce

```bash
docker compose up mariadb -d
bin/benchmark.py all --backend mariadb --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/mariadb-20260509T194118Z.jsonl
bin/benchmark.py report benchmark-results/mariadb-20260509T194118Z.jsonl --output docs/benchmarks/mariadb.md
```
