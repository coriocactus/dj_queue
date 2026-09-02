# dj_queue Four Horsemen Benchmark Report

> Median key metric on the 10k workload across PostgreSQL, MariaDB, MySQL, and SQLite.

Generated: 2026-09-02T22:25:20.330216+00:00

## Metadata

| metadata | postgres | mariadb | mysql | sqlite |
|---|---|---|---|---|
| database | `postgresql` | `mysql` | `mysql` | `sqlite` |
| database name | `dj_queue_benchmark` | `dj_queue_benchmark` | `dj_queue_benchmark` | `benchmark-results/dj_queue_benchmark.sqlite3` |
| database version | `PostgreSQL 18.4 (Debian 18.4-1.pgdg13+1) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit` | `12.3.3-MariaDB-ubu2404` | `9.7.2` | `3.50.4` |
| Python | `3.14.5` | `3.14.5` | `3.14.5` | `3.14.5` |
| Django | `6.1` | `6.1` | `6.1` | `6.1` |
| dj_queue | `0.14.0` | `0.14.0` | `0.14.0` | `0.14.0` |
| platform | `macOS-26.6.2-arm64-arm-64bit-Mach-O` | `macOS-26.6.2-arm64-arm-64bit-Mach-O` | `macOS-26.6.2-arm64-arm-64bit-Mach-O` | `macOS-26.6.2-arm64-arm-64bit-Mach-O` |
| machine | `arm64` | `arm64` | `arm64` | `arm64` |
| revision | `2ae301b9176e` | `2ae301b9176e` | `2ae301b9176e` | `2ae301b9176e` |
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
| `held-xmin-worker-drain` | `jobs_per_second` | end-to-end worker-drain throughput while a second connection pins xmin; higher is better | compare with `worker-drain` and watch dead tuples and relation bytes during the hold |
| `concurrency-contention` | `drain_jobs_per_second` | serialized hot-key drain throughput after enqueue; higher is better | `>= 30 jobs/sec` for a 10k serialized hot-key drain in under 6 minutes |
| `runtime-hot-key-contention` | `drain_jobs_per_second` | real worker-runtime hot-key drain throughput after enqueue; higher is better | `>= 30 jobs/sec` for a 10k hot-key runtime drain in under 6 minutes |
| `ordered-selector-claim` | `jobs_per_second` | selector-heavy claim and drain throughput; higher is better | `>= 60 jobs/sec` for a 10k exact-selector drain in under 3 minutes |

## 10k median key metric comparison

| scenario | key metric | postgres | mariadb | mysql | sqlite |
|---|---|---|---|---|---|
| `single-enqueue` | `latency_p95_ms` | 5.216 | 3.146 | 6.350 | 1.282 |
| `bulk-enqueue` | `jobs_per_second` | 12490.362 | 8158.447 | 6996.658 | 13032.834 |
| `scheduled-promotion` | `rows_per_second` | 8925.698 | 10584.954 | 9336.504 | 9663.570 |
| `recurring-scale` | `duration_seconds` | 0.002 | 0.008 | 0.011 | 0.003 |
| `worker-drain` | `jobs_per_second` | 811.862 | 1035.240 | 1070.261 | 437.047 |
| `held-xmin-worker-drain` | `jobs_per_second` | 796.739 | not supported | not supported | not supported |
| `concurrency-contention` | `drain_jobs_per_second` | 91.289 | 64.127 | 61.577 | not supported |
| `runtime-hot-key-contention` | `drain_jobs_per_second` | 157.248 | 131.654 | 128.685 | not supported |
| `ordered-selector-claim` | `jobs_per_second` | 182.025 | 153.443 | 198.250 | not supported |
