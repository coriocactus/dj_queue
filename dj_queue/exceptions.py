class DjQueueError(Exception):
  pass


class EnqueueError(DjQueueError):
  pass


class DispatchPolicyError(EnqueueError):
  pass


class AlreadyRecorded(DjQueueError):
  pass


class ProcessExitError(DjQueueError):
  pass


class ProcessMissingError(DjQueueError):
  pass


class ProcessPrunedError(DjQueueError):
  pass


def exception_path(error):
  return f"{error.__class__.__module__}.{error.__class__.__qualname__}"
