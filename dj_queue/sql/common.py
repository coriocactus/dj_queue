from django.db import connections

from dj_queue.models import (
  BlockedExecution,
  ClaimedExecution,
  Job,
  ReadyExecution,
)
from dj_queue.sql.state import (
  EXECUTION_STATE_MODELS,
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


def invalid_execution_state_exists(alias, backend_alias, *, queue_name=None):
  sql, params = invalid_execution_state_membership_sql(
    connections[alias],
    backend_alias=backend_alias,
    queue_name=queue_name,
  )
  with connections[alias].cursor() as cursor:
    cursor.execute(sql, params)
    return cursor.fetchone() is not None


def invalid_execution_state_membership_sql(connection, *, backend_alias, queue_name):
  quote = connection.ops.quote_name
  job_table = quote(Job._meta.db_table)
  job_id_column = quote(Job._meta.pk.column)
  job_backend_column = quote(Job._meta.get_field("backend_alias").column)
  job_queue_column = quote(Job._meta.get_field("queue_name").column)
  job_finished_column = quote(Job._meta.get_field("finished_at").column)
  selectors = []
  params = []
  for index, model in enumerate(EXECUTION_STATE_MODELS):
    state_alias = f"state_{index}"
    state_table = quote(model._meta.db_table)
    state_job_column = quote(model._meta.get_field("job").column)
    other_state_exists = " OR ".join(
      _other_state_exists_sql(
        other_model,
        quote=quote,
        state_job_expression=f"{state_alias}.{state_job_column}",
        alias=f"other_{other_index}",
      )
      for other_index, other_model in enumerate(EXECUTION_STATE_MODELS)
      if other_model is not model
    )
    invalid_conditions = [f"job.{job_finished_column} IS NOT NULL"]
    if other_state_exists:
      invalid_conditions.append(other_state_exists)
    state_mismatch = _denormalized_state_mismatch_sql(
      model,
      quote=quote,
      state_alias=state_alias,
      job_backend_expression=f"job.{job_backend_column}",
      job_queue_expression=f"job.{job_queue_column}",
    )
    if state_mismatch:
      invalid_conditions.append(state_mismatch)
    invalid_condition = " OR ".join(invalid_conditions)

    where_sql = f"job.{job_backend_column} = %s"
    state_params = [backend_alias]
    if queue_name is not None:
      where_sql = f"{where_sql} AND job.{job_queue_column} = %s"
      state_params.append(queue_name)

    selectors.append(
      f"""
      SELECT {state_alias}.{state_job_column} AS job_id
      FROM {state_table} {state_alias}
      INNER JOIN {job_table} job ON {state_alias}.{state_job_column} = job.{job_id_column}
      WHERE {where_sql}
        AND ({invalid_condition})
      """
    )
    params.extend(state_params)

  return (
    f"""
    SELECT 1
    FROM ({" UNION ".join(selectors)}) invalid_state_memberships
    LIMIT 1
    """,
    params,
  )


def _other_state_exists_sql(model, *, quote, state_job_expression, alias):
  state_table = quote(model._meta.db_table)
  state_job_column = quote(model._meta.get_field("job").column)
  return (
    f"EXISTS ("
    f"SELECT 1 FROM {state_table} {alias} "
    f"WHERE {alias}.{state_job_column} = {state_job_expression}"
    f")"
  )


def _denormalized_state_mismatch_sql(
  model,
  *,
  quote,
  state_alias,
  job_backend_expression,
  job_queue_expression,
):
  field_names = {field.name for field in model._meta.fields}
  conditions = []
  if "backend_alias" in field_names:
    backend_column = quote(model._meta.get_field("backend_alias").column)
    conditions.append(f"{state_alias}.{backend_column} <> {job_backend_expression}")
  if "queue_name" in field_names:
    queue_column = quote(model._meta.get_field("queue_name").column)
    conditions.append(f"{state_alias}.{queue_column} <> {job_queue_expression}")
  return " OR ".join(conditions)
