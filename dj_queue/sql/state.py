from dj_queue.models import (
  BlockedExecution,
  ClaimedExecution,
  FailedExecution,
  ReadyExecution,
  ScheduledExecution,
)

EXECUTION_STATE_MODELS = (
  ReadyExecution,
  ScheduledExecution,
  ClaimedExecution,
  BlockedExecution,
  FailedExecution,
)


def state_models_except(*ignored_models):
  ignored = set(ignored_models)
  return tuple(model for model in EXECUTION_STATE_MODELS if model not in ignored)


def state_absence_checks_sql(models, *, quote, job_id_expression):
  return " AND ".join(
    _state_absence_sql(model, quote=quote, job_id_expression=job_id_expression) for model in models
  )


def _state_absence_sql(model, *, quote, job_id_expression):
  state_table = quote(model._meta.db_table)
  state_job_id_column = quote(model._meta.get_field("job").column)
  return (
    f"NOT EXISTS ("
    f"SELECT 1 FROM {state_table} "
    f"WHERE {state_table}.{state_job_id_column} = {job_id_expression}"
    f")"
  )
