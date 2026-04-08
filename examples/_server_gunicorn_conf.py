from examples import _server_app  # noqa: F401
from dj_queue.contrib import gunicorn

post_fork = gunicorn.post_fork
worker_exit = gunicorn.worker_exit

workers = 1
timeout = 30
graceful_timeout = 30
