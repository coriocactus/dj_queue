# dj_queue examples

Self-contained examples demonstrating `dj_queue` features.

`DB_BACKEND` in this directory is an example harness variable, not a public
`dj_queue` setting. It switches the examples between SQLite, PostgreSQL, MySQL,
and MariaDB.

## Quickstart

From project root:

```bash
# single-database examples default to sqlite (`:memory:`, no docker needed)
examples/ex01_basic_enqueue.py
```

With a specific backend:

```bash
docker compose up postgres -d
DB_BACKEND=postgres examples/ex01_basic_enqueue.py

docker compose up mysql -d
DB_BACKEND=mysql examples/ex01_basic_enqueue.py

docker compose up mariadb -d
DB_BACKEND=mariadb examples/ex01_basic_enqueue.py
```

## Example order

| Example | Demonstrates | Look for in output |
|---|---|---|
| `ex01_basic_enqueue.py` | enqueue one task and inspect the job row | `ready=True` and one ready execution |
| `ex02_basic_result.py` | claim, execute, and read the stored return value | `status=successful` and `return_value=10` |
| `ex03_basic_scheduled.py` | defer work with `run_after` | one scheduled execution and one immediate ready job |
| `ex04_basic_priority.py` | priority ordering in claim | the highest priority job appears first |
| `ex05_basic_queues.py` | named queues and queue selectors | email work is claimed separately from export work |
| `ex06_basic_bulk_enqueue.py` | `enqueue_all()` batch submission | five task results and five ready executions |
| `ex07_basic_enqueue_on_commit.py` | safe enqueue inside a transaction | zero ready rows before commit, one after |
| `ex08_basic_recurring.py` | static recurring tasks via settings | recurring rows with `static=True` |
| `ex20_advanced_concurrency.py` | `concurrency_key`, `concurrency_limit`, `on_conflict` | one blocked job and a discarded singleton conflict |
| `ex21_advanced_queue_control.py` | `QueueInfo` pause, resume, clear, size, latency | paused queues refuse claims and clear removes ready jobs |
| `ex22_advanced_dynamic_recurring.py` | runtime `schedule_recurring_task()` / `unschedule_recurring_task()` | one row updated in place, then deleted |
| `ex23_advanced_error_handling.py` | failed job inspection, retry, discard | failed metadata, then a ready retry, then full discard |
| `ex24_advanced_multi_db.py` | `database_alias` plus `DjQueueRouter` on a real two-db setup | queue tables stay off `default` and work lands on `queue` |
| `ex25_advanced_asgi.py` | real ASGI lifespan startup and shutdown | `lifespan.startup.complete`, successful job execution, `lifespan.shutdown.complete` |
| `ex26_advanced_uvicorn.py` | a real `uvicorn` process with embedded `dj_queue` | HTTP enqueue followed by a successful HTTP result poll |
| `ex27_advanced_gunicorn.py` | a real `gunicorn` worker with embedded `dj_queue` | HTTP enqueue followed by a successful HTTP result poll |

## Demo server

`bin/dev_admin.py` launches a development server with the Django admin,
embedded `dj_queue` workers, and seeded demo data:

```bash
bin/dev_admin.py                # sqlite, http://127.0.0.1:17777/admin/
bin/dev_admin.py --port 8000    # custom port
bin/dev_admin.py --no-seed      # keep existing rows instead of reseeding
bin/dev_admin.py --no-reload    # disable code reload
```

Default login is `admin` / `password`. The server auto-reloads on Python and
template changes. Hit `/enqueue/` or `/enqueue-burst/` to submit live jobs, and
`/seed/` to reset the demo data without restarting.
