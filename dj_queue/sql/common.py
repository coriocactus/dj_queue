from django.db import connections

from dj_queue.models import (
  BlockedExecution,
  ClaimedExecution,
  Job,
  ReadyExecution,
)
from dj_queue.sql.state import (
  state_absence_checks_sql,
  state_models_except,
)


def create_ready_execution_after_blocked_consume(
  alias,
  *,
  job,
  backend_alias,
  queue_name,
  priority,
  ready_at,
):
  connection = connections[alias]
  quote = connection.ops.quote_name
  ready_table = quote(ReadyExecution._meta.db_table)
  job_id_column = quote(ReadyExecution._meta.get_field("job").column)
  backend_alias_column = quote(ReadyExecution._meta.get_field("backend_alias").column)
  queue_name_column = quote(ReadyExecution._meta.get_field("queue_name").column)
  priority_column = quote(ReadyExecution._meta.get_field("priority").column)
  created_at_column = quote(ReadyExecution._meta.get_field("created_at").column)
  latency_started_at_column = quote(ReadyExecution._meta.get_field("latency_started_at").column)
  job_id = Job._meta.get_field("id").get_db_prep_value(
    job.pk,
    connection=connection,
    prepared=False,
  )
  state_models = state_models_except(BlockedExecution)
  state_checks = state_absence_checks_sql(
    state_models,
    quote=quote,
    job_id_expression="%s",
  )

  with connection.cursor() as cursor:
    cursor.execute(
      f"""
      INSERT INTO {ready_table} (
        {job_id_column},
        {backend_alias_column},
        {queue_name_column},
        {priority_column},
        {created_at_column},
        {latency_started_at_column}
      )
      SELECT %s, %s, %s, %s, %s, %s
      WHERE {state_checks}
      """,
      [
        job_id,
        backend_alias,
        queue_name,
        priority,
        ready_at,
        ready_at,
        *([job_id] * len(state_models)),
      ],
    )
    return cursor.rowcount


def finish_job_if_no_execution_state(
  alias, job, return_value, *, finished_at, include_claimed=False
):
  connection = connections[alias]
  quote = connection.ops.quote_name
  jobs_table = quote(Job._meta.db_table)
  job_id_column = quote(Job._meta.get_field("id").column)
  backend_alias_column = quote(Job._meta.get_field("backend_alias").column)
  finished_at_column = quote(Job._meta.get_field("finished_at").column)
  return_value_column = quote(Job._meta.get_field("return_value").column)
  updated_at_column = quote(Job._meta.get_field("updated_at").column)
  ignored_models = () if include_claimed else (ClaimedExecution,)
  state_checks = state_absence_checks_sql(
    state_models_except(*ignored_models),
    quote=quote,
    job_id_expression=f"{jobs_table}.{job_id_column}",
  )
  job_id = Job._meta.get_field("id").get_db_prep_value(
    job.pk,
    connection=connection,
    prepared=False,
  )
  prepared_return_value = Job._meta.get_field("return_value").get_db_prep_save(
    return_value,
    connection=connection,
  )

  with connection.cursor() as cursor:
    cursor.execute(
      f"""
      UPDATE {jobs_table}
      SET
        {finished_at_column} = %s,
        {return_value_column} = %s,
        {updated_at_column} = %s
      WHERE
        {jobs_table}.{job_id_column} = %s
        AND {jobs_table}.{backend_alias_column} = %s
        AND {state_checks}
      """,
      [finished_at, prepared_return_value, finished_at, job_id, job.backend_alias],
    )
    return cursor.rowcount
