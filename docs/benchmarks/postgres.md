# dj_queue PostgreSQL Benchmark Report

> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.

Generated: 2026-05-14T19:27:41.445179+00:00

## Environment

- backend: `postgres`
- database: `postgresql` `dj_queue_benchmark`
- database version: `PostgreSQL 17.9 (Debian 17.9-1.pgdg13+1) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit`
- Python: `3.14.4`
- Django: `6.0.5`
- dj_queue: `0.9.1`
- platform: `macOS-26.4.1-arm64-arm-64bit-Mach-O`
- machine: `arm64`
- revision: `8f99214ddef4`
- benchmark worker count: `4`
- benchmark worker threads: `8`
- preserve finished jobs: `True`
- database CONN_MAX_AGE: `60`

## Results

### `single-enqueue`: one-by-one immediate enqueue latency and throughput

| size | run | duration_seconds | jobs_per_second | latency_p95_ms | ready_count | job_count | latency_mean_ms | latency_p50_ms | latency_p99_ms |
|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 5.230 | 191.194 | 8.343 | 1000 | 1000 | 5.228 | 4.680 | 9.644 |
| 1000 | 1 | 5.883 | 169.973 | 9.073 | 1000 | 1000 | 5.881 | 5.765 | 11.646 |
| 1000 | 2 | 6.296 | 158.832 | 9.505 | 1000 | 1000 | 6.293 | 6.525 | 12.512 |
| 10000 | 0 | 78.196 | 127.884 | 12.351 | 10000 | 10000 | 7.816 | 7.560 | 13.831 |
| 10000 | 1 | 81.354 | 122.919 | 12.830 | 10000 | 10000 | 8.131 | 7.736 | 15.283 |
| 10000 | 2 | 98.314 | 101.715 | 16.882 | 10000 | 10000 | 9.827 | 9.225 | 19.996 |

### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count

| size | run | duration_seconds | jobs_per_second | query_count | ready_count | job_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.144 | 6937.637 | 5 | 1000 | 1000 |
| 1000 | 1 | 0.146 | 6830.556 | 5 | 1000 | 1000 |
| 1000 | 2 | 0.156 | 6409.757 | 5 | 1000 | 1000 |
| 10000 | 0 | 1.287 | 7770.422 | 5 | 10000 | 10000 |
| 10000 | 1 | 1.298 | 7704.040 | 5 | 10000 | 10000 |
| 10000 | 2 | 1.261 | 7931.408 | 5 | 10000 | 10000 |

### `scheduled-promotion`: due scheduled-row promotion from a mixed due/future backlog

| size | run | duration_seconds | rows_per_second | ready_count | promoted_count | future_scheduled_count |
|---|---|---|---|---|---|---|
| 1000 | 0 | 0.120 | 8339.058 | 1000 | 1000 | 1000 |
| 1000 | 1 | 0.123 | 8113.969 | 1000 | 1000 | 1000 |
| 1000 | 2 | 0.126 | 7944.784 | 1000 | 1000 | 1000 |
| 10000 | 0 | 1.243 | 8044.279 | 10000 | 10000 | 10000 |
| 10000 | 1 | 1.263 | 7917.430 | 10000 | 10000 | 10000 |
| 10000 | 2 | 1.306 | 7655.160 | 10000 | 10000 | 10000 |

### `recurring-scale`: scheduler poll cost for persisted not-due recurring rows

| size | run | duration_seconds | rows_per_second | fired_count | recurring_task_count |
|---|---|---|---|---|---|
| 1000 | 0 | 0.004 | 261039.831 | 0 | 1000 |
| 1000 | 1 | 0.004 | 261161.933 | 0 | 1000 |
| 1000 | 2 | 0.004 | 267469.074 | 0 | 1000 |
| 10000 | 0 | 0.011 | 885867.097 | 0 | 10000 |
| 10000 | 1 | 0.011 | 885043.858 | 0 | 10000 |
| 10000 | 2 | 0.006 | 1610046.684 | 0 | 10000 |

### `worker-drain`: async supervisor drain throughput for no-op ready jobs

| size | run | duration_seconds | jobs_per_second | finished_count | ready_count | claimed_count | completed_count | job_count | preserve_finished_jobs | runner_count |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 0 | 2.547 | 392.623 | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 1 | 2.571 | 389.016 | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 1000 | 2 | 2.566 | 389.706 | 1000 | 0 | 0 | 1000 | 1000 | True | 4 |
| 10000 | 0 | 24.926 | 401.193 | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 1 | 25.093 | 398.517 | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |
| 10000 | 2 | 25.139 | 397.792 | 10000 | 0 | 0 | 10000 | 10000 | True | 4 |

### `concurrency-contention`: one hot concurrency key through enqueue, block, release, and unblock

| size | run | enqueue_jobs_per_second | drain_jobs_per_second | finished_count | drain_duration_seconds | enqueue_duration_seconds |
|---|---|---|---|---|---|---|
| 1000 | 0 | 84.961 | 21.363 | 1000 | 46.810 | 11.770 |
| 1000 | 1 | 82.276 | 20.704 | 1000 | 48.299 | 12.154 |
| 1000 | 2 | 82.096 | 20.695 | 1000 | 48.320 | 12.181 |
| 10000 | 0 | 127.469 | 28.017 | 10000 | 356.929 | 78.450 |
| 10000 | 1 | 271.727 | 27.709 | 10000 | 360.891 | 36.802 |
| 10000 | 2 | 197.154 | 24.727 | 10000 | 404.409 | 50.722 |

## Reproduce

```bash
docker compose up postgres -d
bin/benchmark.py all --backend postgres --sizes 1000,10000 --warmups 1 --runs 3 --output benchmark-results/postgres-20260514T184353Z.jsonl --conn-max-age 60
bin/benchmark.py report benchmark-results/postgres-20260514T184353Z.jsonl --output docs/benchmarks/postgres.md
```
