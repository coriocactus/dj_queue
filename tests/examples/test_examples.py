import os
import subprocess
import sys
from pathlib import Path

import pytest
from django.db import connections

EXAMPLES_DIR = Path(__file__).resolve().parents[2] / "examples"

EXAMPLES = sorted(
  p.stem
  for p in EXAMPLES_DIR.glob("*.py")
  if not p.name.startswith("_") and p.name != "__init__.py"
)


@pytest.mark.parametrize("name", EXAMPLES)
def test_example_runs_successfully(name):
  script = str(EXAMPLES_DIR / f"{name}.py")
  result = subprocess.run(
    [sys.executable, script],
    check=False,
    cwd=EXAMPLES_DIR.parent,
    env={**os.environ, "DB_BACKEND": os.environ.get("DB_BACKEND", "sqlite")},
    capture_output=True,
    text=True,
    timeout=120,
  )
  assert result.returncode == 0, (
    f"examples/{name}.py failed (exit {result.returncode}):\n{result.stdout}\n{result.stderr}"
  )
  connections.close_all()


def test_examples_use_active_python_and_close_connections(monkeypatch):
  commands = []
  seen = []

  def run(command, **_kwargs):
    commands.append(command)
    return type("Result", (), {"returncode": 0, "stdout": "", "stderr": ""})()

  monkeypatch.setattr(
    "tests.examples.test_examples.connections",
    type("DummyConnections", (), {"close_all": lambda self: seen.append("closed")})(),
  )
  monkeypatch.setattr("tests.examples.test_examples.subprocess.run", run)

  test_example_runs_successfully(EXAMPLES[0])

  assert commands == [[sys.executable, str(EXAMPLES_DIR / f"{EXAMPLES[0]}.py")]]
  assert seen == ["closed"]
