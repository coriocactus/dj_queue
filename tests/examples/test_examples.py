import os
import subprocess
from pathlib import Path

import pytest

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
    [script],
    cwd=EXAMPLES_DIR.parent,
    env={**os.environ, "DB_BACKEND": os.environ.get("DB_BACKEND", "sqlite")},
    capture_output=True,
    text=True,
    timeout=120,
  )
  assert result.returncode == 0, (
    f"examples/{name}.py failed (exit {result.returncode}):\n{result.stdout}\n{result.stderr}"
  )
