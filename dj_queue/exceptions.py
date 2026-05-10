class DjQueueError(Exception):
  pass


class EnqueueError(DjQueueError):
  pass


class AlreadyRecorded(DjQueueError):
  pass


class ProcessExitError(DjQueueError):
  pass


class ProcessMissingError(DjQueueError):
  pass


class ProcessPrunedError(DjQueueError):
  pass
