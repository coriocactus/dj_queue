#!/usr/bin/env -S uv run --script

import argparse
import os
import sys

import django

from benchmarks.harness import (
  benchmark_result,
  default_output_path,
  ensure_database_exists,
  environment_metadata,
  parse_sizes,
  prepare_database,
  reset_database,
)
from benchmarks.reports import render_markdown_report

DEFAULT_QUICK_SIZES = [100, 1000]
DEFAULT_ALL_SIZES = [1000, 10000]


def parse_args(argv):
  parser = argparse.ArgumentParser(description="Run dj_queue benchmark scenarios.")
  subparsers = parser.add_subparsers(dest="command", required=True)

  quick = subparsers.add_parser("quick", help="Run every scenario with small defaults.")
  add_run_options(quick, default_sizes=",".join(str(size) for size in DEFAULT_QUICK_SIZES))

  all_parser = subparsers.add_parser("all", help="Run every scenario with larger defaults.")
  add_run_options(all_parser, default_sizes=",".join(str(size) for size in DEFAULT_ALL_SIZES))

  scenario = subparsers.add_parser("scenario", help="Run one scenario.")
  scenario.add_argument("scenario")
  add_run_options(scenario, default_sizes=",".join(str(size) for size in DEFAULT_QUICK_SIZES))

  report = subparsers.add_parser("report", help="Render a Markdown report from JSONL output.")
  report.add_argument("input")
  report.add_argument("--output", required=True)
  report.add_argument("--run-command", help="Benchmark run command to show in the report.")

  return parser.parse_args(argv)


def add_run_options(parser, *, default_sizes):
  parser.add_argument(
    "--backend", choices=("sqlite", "postgres", "mysql", "mariadb"), default="postgres"
  )
  parser.add_argument("--database-name", help="Override BENCHMARK_DB_NAME.")
  parser.add_argument("--sizes", default=default_sizes, help="Comma-separated workload sizes.")
  parser.add_argument("--runs", type=int, default=1, help="Measured repetitions per size.")
  parser.add_argument("--warmups", type=int, default=0, help="Unrecorded warmups per size.")
  parser.add_argument("--output", help="JSONL output path.")
  parser.add_argument(
    "--create-db",
    action=argparse.BooleanOptionalAction,
    default=True,
    help="Create the PostgreSQL benchmark database if it is missing.",
  )
  parser.add_argument(
    "--migrate",
    action=argparse.BooleanOptionalAction,
    default=True,
    help="Run Django migrations before benchmarks.",
  )
  parser.add_argument(
    "--reset-db",
    action=argparse.BooleanOptionalAction,
    default=True,
    help="Flush tables before each measured run and warmup.",
  )


def main(argv):
  args = parse_args(argv)
  if args.command == "report":
    output = render_markdown_report(args.input, args.output, run_command=args.run_command)
    print(output)
    return 0

  try:
    return run_benchmarks(args)
  except ValueError as exc:
    print(exc, file=sys.stderr)
    return 2


def run_benchmarks(args):
  configure_environment(args)
  if args.create_db:
    ensure_database_exists(args.backend)
  django.setup()
  prepare_database(migrate=args.migrate)

  from benchmarks.scenarios import QUICK_SCENARIOS, SCENARIOS

  scenario_names = selected_scenarios(
    args, all_names=tuple(SCENARIOS), quick_names=QUICK_SCENARIOS
  )
  sizes = parse_sizes(args.sizes, default=DEFAULT_QUICK_SIZES)
  if args.runs <= 0 or args.warmups < 0:
    raise ValueError("runs must be positive and warmups cannot be negative")

  output_path = args.output or default_output_path(args.backend)
  from benchmarks.harness import ResultWriter

  writer = ResultWriter(output_path)
  metadata = environment_metadata(backend=args.backend)
  metadata["run"] = run_metadata(args, sizes=sizes, output_path=output_path)

  for scenario_name in scenario_names:
    scenario = SCENARIOS[scenario_name]
    for size in sizes:
      for _warmup_index in range(args.warmups):
        run_once(scenario, size=size, reset=args.reset_db)
      for run_index in range(args.runs):
        metrics = run_once(scenario, size=size, reset=args.reset_db)
        writer.write(
          benchmark_result(
            scenario=scenario_name,
            size=size,
            run_index=run_index,
            metrics=metrics,
            metadata=metadata,
          )
        )
  return 0


def configure_environment(args):
  os.environ.setdefault("DJANGO_SETTINGS_MODULE", "benchmarks.settings")
  os.environ["BENCHMARK_BACKEND"] = args.backend
  if args.database_name:
    os.environ["BENCHMARK_DB_NAME"] = args.database_name


def run_metadata(args, *, sizes, output_path):
  return {
    "command": args.command,
    "scenario": args.scenario if args.command == "scenario" else None,
    "sizes": sizes,
    "runs": args.runs,
    "warmups": args.warmups,
    "output": str(output_path),
    "create_db": args.create_db,
    "migrate": args.migrate,
    "reset_db": args.reset_db,
  }


def selected_scenarios(args, *, all_names, quick_names):
  if args.command == "scenario":
    if args.scenario not in all_names:
      raise ValueError(
        f"unknown scenario {args.scenario!r}; expected one of {', '.join(all_names)}"
      )
    return (args.scenario,)
  if args.command == "quick":
    return quick_names
  return all_names


def run_once(scenario, *, size, reset):
  if reset:
    reset_database()
  return scenario(size)


if __name__ == "__main__":
  raise SystemExit(main(sys.argv[1:]))
