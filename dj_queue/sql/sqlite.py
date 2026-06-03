def create_ignore_conflicts(connection, model, *, columns, placeholders, params):
  table = connection.ops.quote_name(model._meta.db_table)
  with connection.cursor() as cursor:
    cursor.execute(
      f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING",
      params,
    )
    return cursor.rowcount > 0
