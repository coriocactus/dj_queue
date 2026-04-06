CAPTURED_ERRORS = []


def reset():
  CAPTURED_ERRORS.clear()


def record_error(error):
  CAPTURED_ERRORS.append(error)


def raise_on_error(error):
  raise RuntimeError(f"callback failed: {error}")
