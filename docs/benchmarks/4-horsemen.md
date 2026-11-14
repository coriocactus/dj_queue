# dj_queue Four Horsemen Benchmark Report

> Median key metric on the 10k workload across PostgreSQL, MariaDB, MySQL, and SQLite.

Generated: 2026-11-14T22:58:12.395078+00:00

## Metadata

| metadata | postgres | mariadb | mysql | sqlite |
|---|---|---|---|---|
| database | `postgresql` | `mysql` | `mysql` | `sqlite` |
| database name | `dj_queue_benchmark` | `dj_queue_benchmark` | `dj_queue_benchmark` | `benchmark-results/dj_queue_benchmark.sqlite3` |
| database version | `PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit` | `12.3.3-MariaDB-ubu2404` | `9.7.4` | `3.54.1` |
| Python | `3.14.10` | `3.14.10` | `3.14.10` | `3.14.10` |
| Django | `6.1.3` | `6.1.3` | `6.1.3` | `6.1.3` |
| dj_queue | `0.17.0` | `0.17.0` | `0.17.0` | `0.17.0` |
| platform | `macOS-27.2.1-arm64-arm-64bit-Mach-O` | `macOS-27.2.1-arm64-arm-64bit-Mach-O` | `macOS-27.2.1-arm64-arm-64bit-Mach-O` | `macOS-27.2.1-arm64-arm-64bit-Mach-O` |
| machine | `arm64` | `arm64` | `arm64` | `arm64` |
| revision | `9602450ee9bb` | `6c83e91ea8cf` | `322d6e4f5f22` | `4d0c3b99d972` |
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
| `single-enqueue` | `latency_p95_ms` | 5.286 | 4.142 | 4.683 | 1.944 |
| `bulk-enqueue` | `jobs_per_second` | 18546.636 | 11151.154 | 9231.787 | 19000.701 |
| `scheduled-promotion` | `rows_per_second` | 20638.009 | 14091.168 | 12781.194 | 14093.043 |
| `recurring-scale` | `duration_seconds` | 0.002 | 0.011 | 0.007 | 0.002 |
| `worker-drain` | `jobs_per_second` | 4110.608 | 2125.810 | 1993.795 | 710.752 |
| `held-xmin-worker-drain` | `jobs_per_second` | 3982.636 | not supported | not supported | not supported |
| `concurrency-contention` | `drain_jobs_per_second` | 133.011 | 48.185 | 51.963 | not supported |
| `runtime-hot-key-contention` | `drain_jobs_per_second` | 114.532 | 98.576 | 62.403 | not supported |
| `ordered-selector-claim` | `jobs_per_second` | 192.563 | 156.680 | 183.003 | not supported |