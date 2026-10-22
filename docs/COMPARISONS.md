# Comparisons

`dj_queue` is a database-backed task queue for Django:

- **`django.tasks` backend** with scheduling, priorities, and result storage
- **State-table data model** with `SKIP LOCKED` claiming
- **Supervised runtime** with fork and async modes
- **Recurring tasks, concurrency controls, and queue operations**

## How dj_queue compares

*Last updated: October 2026*

**Celery (Redis / RabbitMQ / other brokers)**

- ✅ Battle-tested, horizontally scalable, rich ecosystem.
- ✅ Periodic tasks, retries, priorities, and multiple worker models.
- 🔁 Typical deployments require Redis or RabbitMQ.
- 🔁 Not a `django.tasks` backend without a third-party adapter. Tasks and application data usually live in different systems.

**Celery (SQL database broker)**

- ✅ Lets Celery use a SQL database instead of Redis or RabbitMQ.
- 🔁 Uses `FOR UPDATE` without `SKIP LOCKED`. Workers block on locked rows instead of skipping them.
- 🔁 Celery's current docs document SQLAlchemy as a result backend rather than a primary broker option.
- 🔁 Celery 5.7 (release candidate) adds a PostgreSQL broker via PGMQ.

**RQ**

- ✅ Simple API, low barrier to entry, Django integration via `django-rq`.
- ✅ Scheduling, repeating, cron scheduling, and worker pools.
- 🔁 Built on Redis or Valkey.
- 🔁 Not a `django.tasks` backend without the `django-tasks-rq` adapter.

**django-tasks-db**

- ✅ Database-backed `django.tasks` backend.
- ✅ `SKIP LOCKED` claiming with a single-table status model.
- 🔁 One `db_worker` command rather than separate worker, dispatcher, and scheduler actors.
- 🔁 No recurring tasks or recovery of tasks orphaned by dead workers.

**Steady Queue**

- ✅ Also inspired by Solid Queue. State-table architecture, `SKIP LOCKED`, fork supervisor, thread pool workers.
- ✅ `django.tasks` backend with PostgreSQL, MySQL, and SQLite support.
- 🔁 No result fetching or `async def` tasks.
- 🔁 Recurring work is defined with decorators rather than settings- and API-driven configuration.

**django-ox**

- ✅ Database-backed `django.tasks` backend with automatic retries and backoff.
- ✅ Leases and a reaper requeue tasks from dead workers.
- ✅ Optional unique tasks, batches, rate limits, and workflows.
- 🔁 Single task table, polling only, no concurrency limits or queue pausing.

**django-database-task**

- ✅ Database-backed `django.tasks` backend with optional `LISTEN/NOTIFY`, SQS, and Cloud Tasks brokers.
- ✅ Built for cron, systemd timers, and enterprise job schedulers.
- 🔁 Single task table. No automatic retries or recurring tasks.
- 🔁 Stale-task recovery is a separate command run on a schedule.

**django-absurd**

- ✅ `django.tasks` backend on Absurd, a PostgreSQL-native durable workflow engine.
- ✅ Automatic retries that resume from checkpointed steps.
- 🔁 PostgreSQL with psycopg 3 only. No task priorities.
- 🔁 Read-only admin. Cron needs exactly one beat process or `pg_cron`.

**Procrastinate**

- ✅ Mature PostgreSQL queue with `SKIP LOCKED` + `LISTEN/NOTIFY`.
- ✅ Asyncio-native workers for high-concurrency I/O workloads.
- 🔁 PostgreSQL only.
- 🔁 Not a `django.tasks` backend. Framework-agnostic with optional Django integration.

**Huey**

- ✅ Lightweight queue with a `django.tasks` backend and Redis, PostgreSQL, SQLite, or file storage.
- ✅ Thread, process, and greenlet consumers with retries, locks, and rate limits.
- ✅ PostgreSQL storage uses `SKIP LOCKED` + `LISTEN/NOTIFY`.
- 🔁 Tasks running when a consumer crashes are lost, not recovered.
- 🔁 No `async def` tasks through `django.tasks`.

**Django-Q2**

- ✅ Multiprocessing cluster with multiple broker options including ORM.
- ✅ Maintained fork of Django Q with current Django and Python support.
- 🔁 Not a `django.tasks` backend.
- 🔁 ORM mode uses receipt and lock-timeout updates rather than `SKIP LOCKED` row claiming.

**Solid Queue**

- 👑 State-table data model, `SKIP LOCKED` claiming, fork/async supervision, semaphore concurrency, recurring tasks, batches.
- 🔁 dj_queue adds result storage, `async def` tasks, and PostgreSQL `LISTEN/NOTIFY` wakeups, but no batches.
- 🔁 dj_queue centralizes state transitions in a single operations layer rather than spreading them across models.

### When to use something else

- **Celery**: you need canvas workflows, broad broker support, or already run Redis/RabbitMQ
- **Procrastinate**: you're PostgreSQL-only, want async workers, and don't need `django.tasks`
- **RQ**: you already run Redis or Valkey and want simplicity over tight Django integration
- **Huey**: you want a lightweight queue with pipelines, chords, and rate limits
- **django-tasks-db**: you want the smallest possible database-backed `django.tasks` backend
- **django-ox**: you want automatic retries and crash recovery in a single-table database queue
- **django-database-task**: you run workers from cron or an enterprise job scheduler
- **django-absurd**: you're PostgreSQL-only and want durable, checkpointed workflows
- **Solid Queue**: you are a Railshead
