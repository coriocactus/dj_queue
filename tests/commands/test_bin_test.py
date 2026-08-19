import importlib.util
import sys
from pathlib import Path


def load_test_bin_module():
  module_path = Path(__file__).resolve().parents[2] / "bin" / "test.py"
  spec = importlib.util.spec_from_file_location("dj_queue_bin_test", module_path)
  module = importlib.util.module_from_spec(spec)
  assert spec is not None
  assert spec.loader is not None
  sys.modules[spec.name] = module
  spec.loader.exec_module(module)
  return module


def test_parse_collected_total_reads_pytest_summary():
  module = load_test_bin_module()

  assert module.parse_collected_total("172 tests collected in 1.23s\n") == 172
  assert module.parse_collected_total("1 test collected in 0.01s\n") == 1
  assert module.parse_collected_total("collecting ...\n") is None


def test_note_result_char_counts_ansi_wrapped_progress_only_up_to_total():
  module = load_test_bin_module()
  progress = module.BackendProgress(backend="sqlite", total=4)
  output = (
    "\033[32m.\033[0m\033[31mF\033[0m\n\033[33ms\033[0m\033[35mX\033[0m\n2 passed in 0.01s\n"
  )

  updates = 0
  for char in output:
    updates += int(module.note_result_char(progress, char))

  assert updates == 4
  assert progress.completed == 4


def test_format_progress_line_renders_single_line_bar(monkeypatch):
  module = load_test_bin_module()
  monkeypatch.setattr(module.time, "monotonic", lambda: 30.0)
  progress = module.BackendProgress(
    backend="postgres",
    total=10,
    completed=4,
    started_at=20.0,
  )

  line = module.format_progress_line(progress, columns=90)

  assert line.startswith("[postgres]  40%|")
  assert "4/10" in line
  assert "[00:10<00:15, 0.40 t/s]" in line
  assert "PASS" not in line
  assert "FAIL" not in line
  assert "\n" not in line


def test_trim_failure_output_drops_progress_before_colored_separator():
  module = load_test_bin_module()
  output = (
    "\033[32m.\033[0m\033[31mF\033[0m\033[32m [100%]\033[0m\n"
    "\033[31m============================= FAILURES =============================\033[0m\n"
    "failure body\n"
  )

  trimmed = module.trim_failure_output(output)

  assert trimmed.startswith("\033[31m============================= FAILURES")
  assert "[100%]" not in trimmed


def test_extract_warning_output_keeps_colored_warning_section():
  module = load_test_bin_module()
  output = (
    "\033[32m.\033[0m\033[32m [100%]\033[0m\n"
    "\033[33m=============================== warnings summary ===============================\033[0m\n"
    "warning body\n"
    "\033[32m1 passed, 1 warning in 0.01s\033[0m\n"
  )

  warning_output = module.extract_warning_output(output)

  assert warning_output.startswith("\033[33m=============================== warnings summary")
  assert "warning body" in warning_output
  assert "[100%]" not in warning_output


def test_format_progress_line_colors_completed_status_without_words(monkeypatch):
  module = load_test_bin_module()
  monkeypatch.setattr(module.time, "monotonic", lambda: 30.0)

  success_progress = module.BackendProgress(
    backend="sqlite",
    total=10,
    completed=10,
    exit_code=0,
    started_at=20.0,
  )
  warning_progress = module.BackendProgress(
    backend="postgres",
    total=10,
    completed=10,
    exit_code=0,
    has_warnings=True,
    started_at=20.0,
  )
  failure_progress = module.BackendProgress(
    backend="mysql",
    total=10,
    completed=7,
    exit_code=1,
    started_at=20.0,
  )

  success_line = module.format_progress_line(success_progress, columns=90)
  warning_line = module.format_progress_line(warning_progress, columns=90)
  failure_line = module.format_progress_line(failure_progress, columns=90)

  assert success_line.startswith("\033[32m[sqlite]")
  assert warning_line.startswith("\033[33m[postgres]")
  assert failure_line.startswith("\033[31m[mysql]")
  assert "PASS" not in success_line
  assert "PASS" not in warning_line
  assert "FAIL" not in warning_line
  assert "PASS" not in failure_line
  assert "FAIL" not in failure_line
