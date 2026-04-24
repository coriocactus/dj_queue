from django.db import connections
from django.db.models import AutoField
from django.db.models.constants import OnConflict
from django.db.models.sql import InsertQuery


def create_ignore_conflicts(model, /, *, using, **fields):
  obj = model(**fields)
  queryset = model.objects.using(using).all()
  _objs_with_pk, objs_without_pk = queryset._prepare_for_bulk_create([obj])
  insert_fields = [field for field in model._meta.concrete_fields if not field.generated]
  if objs_without_pk:
    insert_fields = [field for field in insert_fields if not isinstance(field, AutoField)]
  query = InsertQuery(model, on_conflict=OnConflict.IGNORE)
  query.insert_values(insert_fields, [obj], raw=False)
  compiler = query.get_compiler(using=using)
  rowcount = 0

  with connections[using].cursor() as cursor:
    for sql, params in compiler.as_sql():
      cursor.execute(sql, params)
      rowcount += cursor.rowcount

  return rowcount > 0
