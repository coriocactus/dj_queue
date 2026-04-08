import importlib


def set_process_title(title):
  try:
    setproctitle = importlib.import_module("setproctitle")
  except ModuleNotFoundError:
    return False

  setproctitle.setproctitle(title)
  return True
