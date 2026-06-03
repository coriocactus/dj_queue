from django.db import connections

from dj_queue.sql import backend_sql


def create_ignore_conflicts(model, /, *, using, **fields):
  obj = model(**fields)
  connection = connections[using]
  quote = connection.ops.quote_name
  insert_fields = [
    field
    for field in model._meta.concrete_fields
    if not field.generated and not _is_auto_field(field)
  ]
  columns = ", ".join(quote(field.column) for field in insert_fields)
  placeholders = ", ".join(["%s"] * len(insert_fields))
  params = [
    field.get_db_prep_save(field.pre_save(obj, add=True), connection=connection)
    for field in insert_fields
  ]

  return backend_sql(using).create_ignore_conflicts(
    connection,
    model,
    columns=columns,
    placeholders=placeholders,
    params=params,
  )


def _is_auto_field(field):
  return field.get_internal_type() in {"AutoField", "BigAutoField", "SmallAutoField"}
