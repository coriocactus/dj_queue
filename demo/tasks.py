from django.tasks import task


def _result(name, account_id=None):
  return {"task": name, "account_id": account_id}


@task
def refresh_customer_cache(account_id):
  return _result("refresh_customer_cache", account_id)


@task
def generate_statement_pdf(account_id):
  return _result("generate_statement_pdf", account_id)


@task
def build_account_snapshot(account_id):
  return _result("build_account_snapshot", account_id)


@task
def send_digest(account_id):
  return _result("send_digest", account_id)


@task
def sync_account(account_id):
  return _result("sync_account", account_id)


sync_account.func.concurrency_key = "account:{account_id}"
sync_account.func.concurrency_limit = 2
sync_account.func.concurrency_duration = 360


@task
def push_billing_webhook(account_id):
  return _result("push_billing_webhook", account_id)


@task
def fetch_billing_events(account_id):
  return _result("fetch_billing_events", account_id)


@task
def trim_finished_exports(account_id):
  return _result("trim_finished_exports", account_id)
