# Comparisons

`dj_queue` is a database-backed task queue for Django:

- **`django.tasks` backend** with scheduling, priorities, and result storage
- **State-table data model** with `SKIP LOCKED` claiming
- **Supervised runtime** with fork and async modes
- **Recurring tasks, concurrency controls, and queue operations**

## How dj_queue compares

*Last updated: April 2026*

**Celery (Redis / RabbitMQ / other brokers)**

- ✅ Battle-tested, horizontally scalable, rich ecosystem.
- ✅ Periodic tasks, retries, priorities, and multiple worker models.
- 🔁 Typical deployments require Redis or RabbitMQ.
- 🔁 Not a `django.tasks` backend. Tasks and application data usually live in different systems.

**Celery (SQL database broker)**

- ✅ Lets Celery use a SQL database instead of Redis or RabbitMQ.
- 🔁 Uses `FOR UPDATE` without `SKIP LOCKED`. Workers block on locked rows instead of skipping them.
- 🔁 Celery's current docs document SQLAlchemy as a result backend rather than a primary broker option.

**RQ**

- ✅ Simple API, low barrier to entry, Django integration via `django-rq`.
- ✅ Scheduling, repeating, cron scheduling, and worker pools.
- 🔁 Built on Redis or Valkey.
- 🔁 Not a `django.tasks` backend.

**django-tasks-db**

- ✅ Database-backed `django.tasks` backend.
- ✅ `SKIP LOCKED` claiming with a single-table status model.
- 🔁 One `db_worker` command rather than separate worker, dispatcher, and scheduler actors.

**Steady Queue**

- ✅ Also inspired by Solid Queue. State-table architecture, `SKIP LOCKED`, fork supervisor, thread pool workers.
- ✅ `django.tasks` backend with PostgreSQL, MySQL, and SQLite support.
- 🔁 No result fetching or async enqueueing.
- 🔁 Recurring work is defined with decorators rather than settings- and API-driven configuration.

**Procrastinate**

- ✅ Mature PostgreSQL queue with `SKIP LOCKED` + `LISTEN/NOTIFY`.
- ✅ Asyncio-native workers for high-concurrency I/O workloads.
- 🔁 PostgreSQL only.
- 🔁 Not a `django.tasks` backend. Framework-agnostic with optional Django integration.

**Huey**

- ✅ Lightweight with minimal dependencies.
- ✅ Thread, process, and greenlet consumers with retries, task locking, and rate limits.
- 🔁 Not a `django.tasks` backend.
- 🔁 No SQL row-locking design like `SKIP LOCKED` state tables.

**Django-Q2**

- ✅ Multiprocessing cluster with multiple broker options including ORM.
- ✅ Maintained fork of Django Q with current Django and Python support.
- 🔁 Not a `django.tasks` backend.
- 🔁 ORM mode uses receipt and lock-timeout updates rather than `SKIP LOCKED` row claiming.

### When to use something else

- **Celery**: you need canvas workflows, broad broker support, or already run Redis/RabbitMQ
- **Procrastinate**: you're PostgreSQL-only, want async workers, and don't need `django.tasks`
- **RQ**: you already run Redis or Valkey and want simplicity over tight Django integration
- **django-tasks-db**: you want the smallest possible database-backed `django.tasks` backend
