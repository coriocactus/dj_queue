from django.core.management.base import BaseCommand
from django.core.management.base import CommandError
from django.db import connections

from dj_queue import observability
from dj_queue.config import load_backend_config
from dj_queue.db import database_capabilities


class Command(BaseCommand):
  help = "Print PostgreSQL autovacuum storage-parameter SQL for dj_queue tables"

  def add_arguments(self, parser):
    parser.add_argument("--backend", default="default")

  def handle(self, *args, **options):
    backend_alias = options["backend"]
    database_alias = load_backend_config(backend_alias).database_alias
    if database_capabilities(database_alias).backend_family != "postgresql":
      raise CommandError("dj_queue_postgres_autovacuum requires PostgreSQL")

    connection = connections[database_alias]
    self.stdout.write("-- dj_queue PostgreSQL autovacuum guidance")
    self.stdout.write("-- review before applying; dj_queue does not apply these in migrations")
    for statement in observability.postgres_autovacuum_sql(connection):
      self.stdout.write(statement)
