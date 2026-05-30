from django.db import connections

from dj_queue.db import database_capabilities


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

  table = quote(model._meta.db_table)
  backend_family = database_capabilities(using).backend_family
  if backend_family in {"mysql", "mariadb"}:
    pk_column = quote(model._meta.pk.column)
    sql = (
      f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) "
      f"ON DUPLICATE KEY UPDATE {pk_column} = {pk_column} + LAST_INSERT_ID(0)"
    )
  else:
    sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

  with connection.cursor() as cursor:
    cursor.execute(sql, params)
    if backend_family in {"mysql", "mariadb"}:
      return cursor.lastrowid != 0
    rowcount = cursor.rowcount

  return rowcount > 0


def _is_auto_field(field):
  return field.get_internal_type() in {"AutoField", "BigAutoField", "SmallAutoField"}
