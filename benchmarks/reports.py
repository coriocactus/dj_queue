import json
import os
import shlex
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path


SCENARIO_DESCRIPTIONS = {
  "single-enqueue": "one-by-one immediate enqueue latency and throughput",
  "bulk-enqueue": "bulk immediate enqueue throughput and SQL statement count",
  "scheduled-promotion": "due scheduled-row promotion from a mixed due/future backlog",
  "recurring-scale": "scheduler poll cost for persisted not-due recurring rows",
  "worker-drain": "async supervisor drain throughput for no-op ready jobs",
  "concurrency-contention": "one hot concurrency key through enqueue, block, release, and unblock",
}
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def render_markdown_report(input_path, output_path, *, run_command=None):
  rows = read_jsonl(input_path)
  if not rows:
    raise ValueError(f"no benchmark rows found in {input_path}")

  output = Path(output_path)
  output.parent.mkdir(parents=True, exist_ok=True)
  output.write_text(
    render_markdown(rows, input_path=input_path, output_path=output_path, run_command=run_command),
    encoding="utf-8",
  )
  return output


def read_jsonl(input_path):
  path = Path(input_path)
  with path.open(encoding="utf-8") as handle:
    return [json.loads(line) for line in handle if line.strip()]


def render_markdown(rows, *, input_path=None, output_path=None, run_command=None):
  metadata = rows[0]["metadata"]
  database = metadata["database"]
  benchmark = metadata.get("benchmark")
  backend_label = {"postgres": "PostgreSQL"}.get(metadata["backend"], metadata["backend"])
  lines = [
    f"# dj_queue {backend_label} Benchmark Report",
    "",
    "> Local development benchmark. Treat these numbers as reproducibility evidence, not a portable capacity guarantee.",
    "",
    f"Generated: {datetime.now(UTC).isoformat()}",
    "",
    "## Environment",
    "",
    f"- backend: `{metadata['backend']}`",
    f"- database: `{database['vendor']}` `{display_path(database['name'])}`",
    f"- database version: `{database['version']}`",
    f"- Python: `{metadata['python']}`",
    f"- Django: `{metadata['django']}`",
    f"- dj_queue: `{metadata['dj_queue']}`",
    f"- platform: `{metadata['platform']}`",
    f"- machine: `{metadata['machine']}`",
    f"- revision: `{metadata['git_revision']}`",
  ]
  if benchmark:
    lines.extend(
      [
        f"- benchmark worker count: `{benchmark['worker_count']}`",
        f"- benchmark worker threads: `{benchmark['worker_threads']}`",
        f"- preserve finished jobs: `{benchmark['preserve_finished_jobs']}`",
        f"- database CONN_MAX_AGE: `{benchmark.get('conn_max_age', 0)}`",
      ]
    )
  lines.extend(["", "## Results", ""])

  grouped = defaultdict(list)
  for row in rows:
    grouped[row["scenario"]].append(row)

  for scenario, scenario_rows in grouped.items():
    lines.extend(render_scenario_table(scenario, scenario_rows))

  lines.extend(
    render_reproduce_commands(
      rows, input_path=input_path, output_path=output_path, run_command=run_command
    )
  )
  return "\n".join(lines)


def render_reproduce_commands(rows, *, input_path=None, output_path=None, run_command=None):
  metadata = rows[0]["metadata"]
  backend = metadata["backend"]
  commands = []
  service_command = docker_compose_command(backend)
  if service_command:
    commands.append(service_command)

  command = run_command or command_from_metadata(metadata)
  if command:
    commands.append(command)
  else:
    commands.append("# rerun the benchmark command that produced this JSONL file")

  if input_path and output_path:
    commands.append(report_command(input_path, output_path, run_command=run_command))

  return ["## Reproduce", "", "```bash", *commands, "```", ""]


def docker_compose_command(backend):
  if backend in {"postgres", "mysql", "mariadb"}:
    return f"docker compose up {backend} -d"
  return None


def command_from_metadata(metadata):
  run = metadata.get("run")
  if not run:
    return None

  parts = ["bin/benchmark.py", run["command"]]
  if run["command"] == "scenario":
    parts.append(run["scenario"])
  parts.extend(
    [
      "--backend",
      metadata["backend"],
      "--sizes",
      ",".join(str(size) for size in run["sizes"]),
      "--warmups",
      str(run["warmups"]),
      "--runs",
      str(run["runs"]),
      "--output",
      display_path(run["output"]),
    ]
  )
  if run.get("worker_count") is not None:
    parts.extend(["--worker-count", run["worker_count"]])
  if run.get("worker_threads") is not None:
    parts.extend(["--worker-threads", run["worker_threads"]])
  if run.get("preserve_finished_jobs") is False:
    parts.append("--no-preserve-finished-jobs")
  elif run.get("preserve_finished_jobs") is True:
    parts.append("--preserve-finished-jobs")
  if run.get("conn_max_age") is not None:
    parts.extend(["--conn-max-age", run["conn_max_age"]])
  if not run.get("create_db", True):
    parts.append("--no-create-db")
  if not run.get("migrate", True):
    parts.append("--no-migrate")
  if not run.get("reset_db", True):
    parts.append("--no-reset-db")
  return shell_join(parts)


def report_command(input_path, output_path, *, run_command=None):
  parts = [
    "bin/benchmark.py",
    "report",
    display_path(input_path),
    "--output",
    display_path(output_path),
  ]
  if run_command:
    parts.extend(["--run-command", run_command])
  return shell_join(parts)


def display_path(path):
  path = Path(path)
  if not path.is_absolute():
    return path.as_posix()

  return os.path.relpath(path, PROJECT_ROOT)


def shell_join(parts):
  return " ".join(shlex.quote(str(part)) for part in parts)


def render_scenario_table(scenario, rows):
  metric_names = sorted({metric for row in rows for metric in row["metrics"]})
  preferred = [
    "duration_seconds",
    "jobs_per_second",
    "rows_per_second",
    "query_count",
    "latency_p95_ms",
    "enqueue_jobs_per_second",
    "drain_jobs_per_second",
    "drain_query_count",
    "claim_query_count",
    "execute_query_count",
    "finished_count",
    "ready_count",
    "promoted_count",
    "fired_count",
  ]
  columns = [metric for metric in preferred if metric in metric_names]
  columns.extend(metric for metric in metric_names if metric not in columns)

  description = SCENARIO_DESCRIPTIONS.get(scenario)
  heading = f"### `{scenario}`"
  if description:
    heading = f"{heading}: {description}"
  lines = [heading, ""]
  header = ["size", "run", *columns]
  lines.append("| " + " | ".join(header) + " |")
  lines.append("|" + "---|" * len(header))
  for row in rows:
    values = [row["size"], row["run_index"]]
    values.extend(format_value(row["metrics"].get(column)) for column in columns)
    lines.append("| " + " | ".join(str(value) for value in values) + " |")
  lines.append("")
  return lines


def format_value(value):
  if isinstance(value, float):
    return f"{value:.3f}"
  if value is None:
    return ""
  return value
