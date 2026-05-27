# dj_queue Four Horsemen Benchmark Report

> Median key metric on the 10k workload across PostgreSQL, MariaDB, MySQL, and SQLite.

Generated: 2026-05-27T06:10:06.893404+00:00

## Metadata

| metadata | postgres | mariadb | mysql | sqlite |
|---|---|---|---|---|
| database | `postgresql` | `mysql` | `mysql` | `sqlite` |
| database name | `dj_queue_benchmark` | `dj_queue_benchmark` | `dj_queue_benchmark` | `benchmark-results/dj_queue_benchmark.sqlite3` |
| database version | `PostgreSQL 17.9 (Debian 17.9-1.pgdg13+1) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit` | `10.6.25-MariaDB-ubu2204` | `8.4.8` | `3.50.4` |
| Python | `3.14.5` | `3.14.5` | `3.14.5` | `3.14.5` |
| Django | `6.0.5` | `6.0.5` | `6.0.5` | `6.0.5` |
| dj_queue | `0.10.6` | `0.10.6` | `0.10.6` | `0.10.6` |
| platform | `macOS-26.5-arm64-arm-64bit-Mach-O` | `macOS-26.5-arm64-arm-64bit-Mach-O` | `macOS-26.5-arm64-arm-64bit-Mach-O` | `macOS-26.5-arm64-arm-64bit-Mach-O` |
| machine | `arm64` | `arm64` | `arm64` | `arm64` |
| revision | `8a524c8f1d1f` | `8a524c8f1d1f` | `8a524c8f1d1f` | `8a524c8f1d1f` |
| workers | `4` | `4` | `4` | `1` |
| worker threads | `8` | `8` | `8` | `1` |
| preserve finished jobs | `True` | `True` | `True` | `True` |
| CONN_MAX_AGE | `60` | `60` | `60` | `0` |

## Scenario keys

| scenario | key metric | meaning | healthy local baseline |
|---|---|---|---|
| `single-enqueue` | `latency_p95_ms` | enqueue tail latency for individual task submissions; lower is better | `<= 15 ms` p95 for request-path enqueue on the 10k local benchmark |
| `bulk-enqueue` | `jobs_per_second` | bulk enqueue throughput; `query_count` should stay nearly fixed as size grows | `>= 6,000 jobs/sec` for 10k independent immediate jobs in under 2 seconds |
| `scheduled-promotion` | `rows_per_second` | due scheduled-row promotion throughput; higher is better | `>= 6,000 rows/sec` for a 10k due-row promotion burst in under 2 seconds |
| `recurring-scale` | `duration_seconds` | scheduler no-op poll duration over persisted not-due recurring rows; lower is better | `<= 0.025 seconds` for a no-op poll over 10k not-due schedules |
| `worker-drain` | `jobs_per_second` | end-to-end ready-job drain throughput through the async runtime; higher is better | `>= 300 jobs/sec` for draining 10k no-op ready jobs in under 35 seconds |
| `concurrency-contention` | `drain_jobs_per_second` | serialized hot-key drain throughput after enqueue; higher is better | `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes |
| `runtime-hot-key-contention` | `drain_jobs_per_second` | real worker-runtime hot-key drain throughput after enqueue; higher is better | `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes |
| `ordered-selector-claim` | `jobs_per_second` | selector-heavy claim and drain throughput; higher is better | `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes |

## 10k median key metric comparison

| scenario | key metric | postgres | mariadb | mysql | sqlite |
|---|---|---|---|---|---|
| `single-enqueue` | `latency_p95_ms` | 11.493 | 9.968 | 7.828 | 1.293 |
| `bulk-enqueue` | `jobs_per_second` | 13667.625 | 8147.801 | 7113.719 | 14937.038 |
| `scheduled-promotion` | `rows_per_second` | 8743.241 | 9698.549 | 9194.345 | 10431.558 |
| `recurring-scale` | `duration_seconds` | 0.018 | 0.029 | 0.023 | 0.006 |
| `worker-drain` | `jobs_per_second` | 631.940 | 775.122 | 681.169 | 300.701 |
| `concurrency-contention` | `drain_jobs_per_second` | 51.640 | 34.929 | 56.078 | not supported |
| `runtime-hot-key-contention` | `drain_jobs_per_second` | 87.484 | 69.666 | 68.072 | not supported |
| `ordered-selector-claim` | `jobs_per_second` | 104.741 | 93.979 | 123.059 | not supported |