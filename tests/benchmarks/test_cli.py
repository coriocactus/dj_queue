import importlib.util
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
  "benchmark_cli", PROJECT_ROOT / "bin" / "benchmark.py"
)
benchmark_cli = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark_cli)


def test_all_backends_runs_benchmarks_and_reports_with_default_outputs(monkeypatch):
  commands = []
  reports = []
  outputs = {
    backend: Path(f"benchmark-results/{backend}-timestamp.jsonl")
    for backend in ("postgres", "mariadb", "mysql", "sqlite")
  }

  def fake_run(command, *, check):
    commands.append(command)
    assert check is True

  monkeypatch.setattr(benchmark_cli, "default_output_path", lambda backend: outputs[backend])
  monkeypatch.setattr(benchmark_cli.subprocess, "run", fake_run)
  monkeypatch.setattr(
    benchmark_cli,
    "render_markdown_report",
    lambda input_path, output_path: reports.append((input_path, output_path)),
  )

  args = benchmark_cli.parse_args(["all-backends"])

  assert benchmark_cli.run_all_backend_benchmarks(args) == 0
  assert [command[:2] for command in commands] == [
    [sys.executable, str(benchmark_cli.SCRIPT_PATH)],
    [sys.executable, str(benchmark_cli.SCRIPT_PATH)],
    [sys.executable, str(benchmark_cli.SCRIPT_PATH)],
    [sys.executable, str(benchmark_cli.SCRIPT_PATH)],
  ]
  assert [command[2:] for command in commands] == [
    [
      "all",
      "--backend",
      "postgres",
      "--sizes",
      "1000,10000",
      "--warmups",
      "1",
      "--runs",
      "3",
      "--output",
      "benchmark-results/postgres-timestamp.jsonl",
      "--conn-max-age",
      "60",
    ],
    [
      "all",
      "--backend",
      "mariadb",
      "--sizes",
      "1000,10000",
      "--warmups",
      "1",
      "--runs",
      "3",
      "--output",
      "benchmark-results/mariadb-timestamp.jsonl",
      "--conn-max-age",
      "60",
    ],
    [
      "all",
      "--backend",
      "mysql",
      "--sizes",
      "1000,10000",
      "--warmups",
      "1",
      "--runs",
      "3",
      "--output",
      "benchmark-results/mysql-timestamp.jsonl",
      "--conn-max-age",
      "60",
    ],
    [
      "all",
      "--backend",
      "sqlite",
      "--sizes",
      "1000,10000",
      "--warmups",
      "1",
      "--runs",
      "3",
      "--output",
      "benchmark-results/sqlite-timestamp.jsonl",
    ],
  ]
  assert reports == [
    (outputs["postgres"], Path("docs/benchmarks/postgres.md")),
    (outputs["mariadb"], Path("docs/benchmarks/mariadb.md")),
    (outputs["mysql"], Path("docs/benchmarks/mysql.md")),
    (outputs["sqlite"], Path("docs/benchmarks/sqlite.md")),
  ]
