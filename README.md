# dj_queue

[![CI](https://github.com/coriocactus/dj_queue/actions/workflows/ci.yml/badge.svg)](https://github.com/coriocactus/dj_queue/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/dj-queue.svg)](https://pypi.org/project/dj-queue/)
[![Latest on Django Packages](https://img.shields.io/badge/pypi/dj-queue-tags.svg)](https://djangopackages.org/packages/p/dj-queue/)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dj-queue.svg)
![PyPI - Status](https://img.shields.io/pypi/status/dj-queue.svg)
[![PyPI - License](https://img.shields.io/pypi/l/dj-queue.svg)](https://github.com/coriocactus/dj_queue/blob/main/LICENSE)

`dj_queue` is a database-backed task queue backend for the `django.tasks` framework.

It keeps the queue, live execution state, runtime metadata, and task results in your database.

- no Redis, RabbitMQ, or separate result store
- PostgreSQL is the first-class production backend
- MySQL 8+, MariaDB 10.6+, and SQLite are supported
- immediate, scheduled, recurring, and concurrency-limited work

`dj_queue` is inspired by Rails' [Solid Queue](https://github.com/rails/solid_queue),
but shaped to fit Django's [task backend API](https://docs.djangoproject.com/en/6.0/topics/tasks/).

## Why dj_queue

Django applications already depend on the database as the durable system of
record. `dj_queue` lets background work follow the same model.

It has a narrow, explicit shape:

- application code uses Django's `@task` API
- `DjQueueBackend` stores jobs and results in Django-managed tables
- workers, dispatchers, and schedulers all share one operations layer
- PostgreSQL can use `LISTEN/NOTIFY` and `SKIP LOCKED` as optimizations
- polling remains the correctness path on every supported database

For detailed comparisons with Celery, RQ, Procrastinate, and other alternatives,
see [COMPARISONS.md](COMPARISONS.md).

## Installation

`dj_queue` requires Python 3.12+ and Django 6.0+.

Install the package:

```bash
pip install dj-queue
```

Backend-specific extras are available when you want `dj_queue` to install a
database adapter for you:

```bash
pip install "dj-queue[postgres]"
```

Notes:

- `postgres` installs `psycopg`, which Django's PostgreSQL backend and
  `dj_queue`'s optional `LISTEN/NOTIFY` wakeups use
- for MySQL or MariaDB, install and configure a Django-compatible driver in
  your application following Django's database docs

Add `dj_queue` to `INSTALLED_APPS`, register the router, and point Django's task
backend at `DjQueueBackend`:

```python
# settings.py

INSTALLED_APPS = [
  # ...
  "dj_queue",
]

DATABASE_ROUTERS = ["dj_queue.routers.DjQueueRouter"]

TASKS = {
  "default": {
    "BACKEND": "dj_queue.backend.DjQueueBackend",
    "QUEUES": [],
    "OPTIONS": {},
  },
}
```

The router is optional when using the default database, but harmless to include
and required for [multi-database setups](#multi-database-setup).

Run migrations:

```bash
python manage.py migrate
```

## Quick Start

Define a task with Django's `@task` decorator:

```python
# myapp/tasks.py
from django.tasks import task

@task
def add(a, b):
  return a + b
```

Start the `dj_queue` runtime in one terminal:

```bash
python manage.py dj_queue
```

Then enqueue work from another terminal or from your application code:

```python
from myapp.tasks import add

task_result = add.enqueue(3, 7)
print(task_result.id)
```

Read the result back through Django's task backend API:

```python
from myapp.tasks import add

fresh_result = add.get_backend().get_result(task_result.id)
print(fresh_result.status)
print(fresh_result.return_value)
```

When the worker has executed the job, `fresh_result.return_value` will be `10`.

## Common Patterns

### Scheduled jobs

Use `run_after` to keep work out of the ready queue until a future time:

```python
from datetime import timedelta
from django.utils import timezone
from myapp.tasks import send_digest

future = timezone.now() + timedelta(hours=1)
send_digest.using(run_after=future).enqueue("daily")
```

### Priorities and named queues

Use `priority` and `queue_name` on the task call itself:

```python
from myapp.tasks import deliver_email

deliver_email.using(queue_name="email", priority=10).enqueue("welcome")
deliver_email.using(queue_name="email", priority=-5).enqueue("digest")
```

### Bulk enqueue

Use `enqueue_all()` when you need one backend call to submit many jobs:

```python
from myapp.tasks import process_item

results = process_item.get_backend().enqueue_all(
  [(process_item, [item_id], {}) for item_id in range(5)]
)
```

### Enqueue after commit

`enqueue()` writes immediately. If a task depends on rows that are still inside
the current transaction, use `enqueue_on_commit()`:

```python
from django.db import transaction
from dj_queue.api import enqueue_on_commit
from myapp.tasks import send_receipt

with transaction.atomic():
  order = create_order()
  enqueue_on_commit(send_receipt, order.id)
```

### Examples

The repository ships real runnable examples in `examples/`.

Recommended entry points:

- [examples/ex01_basic_enqueue.py](examples/ex01_basic_enqueue.py)
- [examples/ex07_basic_enqueue_on_commit.py](examples/ex07_basic_enqueue_on_commit.py)
- [examples/ex08_basic_recurring.py](examples/ex08_basic_recurring.py)
- [examples/ex20_advanced_concurrency.py](examples/ex20_advanced_concurrency.py)
- [examples/ex21_advanced_queue_control.py](examples/ex21_advanced_queue_control.py)
- [examples/ex24_advanced_multi_db.py](examples/ex24_advanced_multi_db.py)
- [examples/ex25_advanced_asgi.py](examples/ex25_advanced_asgi.py)

The [examples index](examples/README.md) lists the full progression.

## How it Works

`python manage.py dj_queue` starts a supervisor for one backend alias.

Job lifecycle:

`enqueue -> ready | scheduled | blocked -> claimed -> successful | failed`

The runtime has four moving parts:

- `supervisor`: boots and stops the runtime
- `workers`: claim ready jobs and execute them
- `dispatchers`: promote due scheduled jobs and run concurrency maintenance
- `scheduler`: enqueue recurring tasks and finished-job cleanup when configured

Useful command variants:

```bash
python manage.py dj_queue
python manage.py dj_queue --mode async
python manage.py dj_queue --only-work
python manage.py dj_queue --only-dispatch
python manage.py dj_queue --skip-recurring
```

Mode and topology notes:

- `fork` is the default standalone mode
- `async` runs supervised actors in threads inside one process
- `--only-work` starts workers without dispatchers or scheduler
- `--only-dispatch` starts dispatchers without workers or scheduler
- `--skip-recurring` starts without the scheduler

`fork` runs each worker, dispatcher, and scheduler as a separate OS process.
`async` runs them as threads in one process, i.e., lower memory, less isolation.
Default is `fork`. Use `async` for embedded mode or memory-constrained environments.

### Claiming order

- within one selected queue, higher numeric `priority` is claimed first
- across multiple queue selectors, selector order wins
- `"*"` matches all queues
- selectors ending in `*` match queue prefixes such as `email*`

For example, a worker configured with `queues: ["email", "default"]` will
prefer ready work from `email` before `default`, even if `default` contains
higher-priority rows.

## Database Support

| Backend | Support level | Notes |
|---|---|---|
| PostgreSQL | first-class | polling, `SKIP LOCKED`, and optional `LISTEN/NOTIFY` |
| MySQL 8+ | supported | polling plus `SKIP LOCKED` |
| MariaDB 10.6+ | supported | polling plus `SKIP LOCKED` |
| SQLite | supported with limits | polling only, serialized writes, no `SKIP LOCKED`, no `LISTEN/NOTIFY`; practical for development, CI, and smaller deployments |

Polling is the portability path everywhere. Backend-specific features improve
latency and throughput but are not correctness requirements.

## Data Contract

Job payloads and persisted return values are stored in JSON columns, so they
must be JSON round-trippable.

- enqueueing args or kwargs that cannot round-trip through JSON fails immediately
- returning a non-JSON-serializable value marks the job failed instead of
  leaving it claimed forever

If you need to pass model instances, files, or custom objects, store them
elsewhere and pass identifiers or serialized data instead.

## Recurring Tasks

`dj_queue` supports both static recurring tasks from settings and dynamic
recurring tasks managed at runtime.

### Static recurring tasks

Define recurring tasks in `TASKS[...]["OPTIONS"]["recurring"]`:

```python
TASKS = {
  "default": {
    "BACKEND": "dj_queue.backend.DjQueueBackend",
    "QUEUES": [],
    "OPTIONS": {
      "recurring": {
        "nightly_cleanup": {
          "task_path": "myapp.tasks.cleanup",
          "schedule": "0 3 * * *",
          "queue_name": "maintenance",
          "priority": -5,
          "description": "nightly cleanup",
        },
      },
    },
  },
}
```

### Dynamic recurring tasks

Create, update, and remove recurring tasks at runtime:

```python
from dj_queue.api import schedule_recurring_task, unschedule_recurring_task

schedule_recurring_task(
  key="tenant_42_report",
  task_path="myapp.tasks.send_report",
  schedule="0 * * * *",
  queue_name="reports",
  priority=5,
)

unschedule_recurring_task("tenant_42_report")
```

Dynamic recurring tasks require
`TASKS[backend_alias]["OPTIONS"]["scheduler"]["dynamic_tasks_enabled"] = True`
or the equivalent `scheduler.dynamic_tasks_enabled: true` in the optional YAML
config.

The scheduler is part of the normal `dj_queue` runtime. You do not run a
separate recurring service.

## Concurrency Controls

Tasks can opt into database-backed concurrency limits.

`django.tasks` has no standard way to pass backend-specific options through the
`@task` decorator, so `dj_queue` reads them as attributes on the wrapped function:

```python
from django.tasks import task

@task
def sync_account(account_id, action):
  return f"{account_id}:{action}"

sync_account.func.concurrency_key = "account:{account_id}"
sync_account.func.concurrency_limit = 1
sync_account.func.concurrency_duration = 60
sync_account.func.on_conflict = "block"
```

With this configuration:

- the first matching job can run immediately
- later jobs for the same key can block until capacity is released
- `on_conflict = "discard"` turns the same pattern into singleton-style work

## Queue Operations

`QueueInfo` exposes operational queue controls without bypassing the queue
tables:

```python
from dj_queue.api import QueueInfo

orders = QueueInfo("orders")

print(orders.size)
print(orders.latency)
print(orders.paused)

orders.pause()
orders.resume()
orders.clear()
```

Operational commands:

```bash
python manage.py dj_queue_health
python manage.py dj_queue_health --max-age 120
python manage.py dj_queue_prune --older-than 86400
python manage.py dj_queue_prune --task-path myapp.tasks.cleanup
```

If Django admin is installed, `dj_queue` also registers the main operational
models there, including jobs, failed executions, processes, recurring tasks,
pauses, and semaphores.

## Failed Jobs

When a task raises, `dj_queue` keeps the job and its failed execution row in the
queue database, including the exception class, message, and traceback.

You can retry and discard failed jobs through Django admin, or call the same
operations directly through the operations layer:

```python
from dj_queue.operations.jobs import discard_failed_job, retry_failed_job

retry_failed_job(job_id)
discard_failed_job(job_id)
```

Failures stay inspectable until you act on them.

## Multi-Database Setup

`dj_queue` can keep queue tables on a dedicated database alias.

Example configuration:

```python
DATABASES = {
  "default": {
    "ENGINE": "django.db.backends.postgresql",
    "NAME": "app",
  },
  "queue": {
    "ENGINE": "django.db.backends.postgresql",
    "NAME": "queue",
  },
}

DATABASE_ROUTERS = ["dj_queue.routers.DjQueueRouter"]

TASKS = {
  "default": {
    "BACKEND": "dj_queue.backend.DjQueueBackend",
    "QUEUES": [],
    "OPTIONS": {
      "database_alias": "queue",
    },
  },
}
```

Run your normal application migrations on `default`, then migrate `dj_queue`
onto the queue database:

```bash
python manage.py migrate
python manage.py migrate dj_queue --database queue
```

With this setup, `dj_queue`'s ORM queries and raw SQL helpers stay on the queue
database.

## Embedded Server Mode

`dj_queue` can run inside an existing server process via embedded async
supervision.

### ASGI

Wrap your ASGI application with `DjQueueLifespan`:

```python
from django.core.asgi import get_asgi_application
from dj_queue.contrib.asgi import DjQueueLifespan

django_application = get_asgi_application()
application = DjQueueLifespan(django_application)
```

### Gunicorn

Import the provided hooks in your Gunicorn config:

```python
# gunicorn.conf.py
from dj_queue.contrib.gunicorn import post_fork, worker_exit
```

Both embedded integrations use `AsyncSupervisor(standalone=False)` and leave
signal handling to the host server.

## Configuration

### Deployment topology

Once migrations are in place, start processing jobs with `python manage.py dj_queue`
on the machine that should do the work. With the default configuration, this
starts the supervisor, workers, dispatcher, and scheduler for the default
backend alias and processes all queues.

For most deployments, start with a standalone `dj_queue` process. Reach for a
dedicated queue database before you reach for embedded mode.

- single database, standalone process: easiest way to start. Use the app
  database and run `python manage.py dj_queue`
- dedicated queue database: recommended production default. Keep queue tables
  and runtime traffic on `database_alias`. See [Multi-Database Setup](#multi-database-setup)
- embedded server mode: run `dj_queue` inside ASGI or Gunicorn when you want
  queue execution colocated with the server process. See [Embedded Server Mode](#embedded-server-mode)

For small deployments, running `dj_queue` on the same machine as the web server
is often enough. When you need more capacity, multiple machines can point at
the same queue database. Full `python manage.py dj_queue` instances coordinate
through database locking, so workers and dispatchers share load safely and
recurring firing stays deduplicated across schedulers.

In practice, keep recurring settings identical on every full node and prefer one
full instance plus additional `python manage.py dj_queue --only-work` nodes.
Add `--only-dispatch` nodes only when you need more scheduled-job promotion or
concurrency-maintenance throughput.

### Options

The main configuration lives in `TASKS[backend_alias]["OPTIONS"]`.

Start with these options:

- `mode`: `"fork"` or `"async"`
- `workers`: queue selectors, thread counts, and process counts
- `dispatchers`: scheduled promotion and concurrency maintenance settings
- `scheduler`: dynamic recurring polling settings
- `database_alias`: database alias for queue tables and runtime activity
- `preserve_finished_jobs` and `clear_finished_jobs_after`: result retention and cleanup

Additional operational tuning is available when needed, including
`use_skip_locked`, `listen_notify`, `silence_polling`,
`process_heartbeat_interval`, `process_alive_threshold`, `shutdown_timeout`, and
`on_thread_error`.

On PostgreSQL, `listen_notify` uses the same Django PostgreSQL driver
configuration as the main database connection. Install a compatible driver in
your project, or use `dj-queue[postgres]` to pull in `psycopg`.

### Precedence

Configuration precedence is explicit:

- CLI overrides
- environment variables
- YAML file pointed to by `DJ_QUEUE_CONFIG`
- Django `TASKS` settings

### YAML file config

```bash
# via cli
python manage.py dj_queue --config /etc/dj_queue.yml

# or via environment variable
DJ_QUEUE_CONFIG=/etc/dj_queue.yml python manage.py dj_queue
```

The YAML file should contain a single mapping of backend option values. It uses
the same shape as `TASKS[backend_alias]["OPTIONS"]`, not the full Django
`TASKS` structure:

```yaml
mode: async
database_alias: queue
preserve_finished_jobs: true
clear_finished_jobs_after: 86400
listen_notify: true
silence_polling: true

workers:
  - queues: ["default", "email*"]
    threads: 8
    processes: 1
    polling_interval: 0.1

dispatchers:
  - batch_size: 500
    polling_interval: 1
    concurrency_maintenance: true
    concurrency_maintenance_interval: 600

scheduler:
  dynamic_tasks_enabled: true
  polling_interval: 5

recurring:
  nightly_cleanup:
    task_path: myapp.tasks.cleanup
    schedule: "0 3 * * *"
    queue_name: maintenance
    priority: -5
    description: nightly cleanup
```

This file is merged on top of `TASKS[backend_alias]["OPTIONS"]`, then any
environment-variable and CLI overrides win after that.

Environment overrides currently supported by `dj_queue` itself:

- `DJ_QUEUE_CONFIG`
- `DJ_QUEUE_MODE`
- `DJ_QUEUE_SKIP_RECURRING`

## License

MIT
