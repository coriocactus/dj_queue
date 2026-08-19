#!/usr/bin/env -S uv run --script

import argparse
import os
import random
import sys
from pathlib import Path

import pytest

from tests.sim.config import DEFAULT_SEEDS, DEFAULT_STEPS

PROJECT_ROOT = Path(__file__).resolve().parent.parent
WORKLOAD_FILES = {
  "core": "tests/integration/test_seeded_simulation.py",
  "queue": "tests/integration/test_seeded_queue_control_simulation.py",
  "recurring": "tests/integration/test_seeded_recurring_retry_simulation.py",
}

# --no-header drops the platform/version block
# -rN suppresses the summary section
# -v gives one line per parametrized case
DEFAULT_PYTEST_ARGS = ["--no-header", "-rN", "-v"]


def parse_args(argv):
  parser = argparse.ArgumentParser(description="Run seeded simulation workloads.")
  parser.add_argument(
    "workload",
    nargs="?",
    choices=["all", *WORKLOAD_FILES],
    default="all",
  )
  parser.add_argument("--steps", type=int, help="Simulation steps per seed.")
  parser.add_argument(
    "--seed",
    dest="seed_values",
    action="append",
    type=int,
    default=[],
    metavar="SEED",
    help="A specific seed.",
  )
  parser.add_argument("--seeds", help="A count of random seeds to generate.")
  parser.add_argument(
    "--seed-range",
    action="append",
    default=[],
    metavar="FROM-TO",
    help="Inclusive seed range such as 10-20.",
  )
  args, pytest_args = parser.parse_known_args(argv)
  if pytest_args[:1] == ["--"]:
    pytest_args = pytest_args[1:]
  return args, pytest_args


def parse_seed_range(value):
  start_text, separator, end_text = value.partition("-")
  if not separator or not start_text or not end_text:
    raise ValueError(f"invalid seed range: {value}")

  start = int(start_text)
  end = int(end_text)
  if end < start:
    raise ValueError(f"seed range must increase: {value}")
  return list(range(start, end + 1))


def random_seeds(count):
  if count <= 0:
    raise ValueError("random seed count must be positive")

  rng = random.SystemRandom()
  seeds = []
  seen = set()
  while len(seeds) < count:
    seed = rng.randrange(0, 2**31)
    if seed in seen:
      continue
    seeds.append(seed)
    seen.add(seed)
  return seeds


def unique(values):
  return list(dict.fromkeys(values))


def resolve_seeds(args):
  explicit_seeds = []

  if args.seeds:
    explicit_seeds.extend(random_seeds(int(args.seeds)))

  explicit_seeds.extend(args.seed_values)

  for seed_range in args.seed_range:
    explicit_seeds.extend(parse_seed_range(seed_range))

  if explicit_seeds:
    return unique(explicit_seeds)
  return DEFAULT_SEEDS


def resolve_steps(args):
  if args.steps is not None:
    return args.steps
  return DEFAULT_STEPS


def selected_workloads(workload):
  if workload == "all":
    return [(name, WORKLOAD_FILES[name]) for name in ("core", "queue", "recurring")]
  return [(workload, WORKLOAD_FILES[workload])]


def run_simulation(*, test_files, seeds, steps, pytest_args):
  os.environ["SIM_SEEDS"] = ",".join(str(seed) for seed in seeds)
  os.environ["SIM_STEPS"] = str(steps)
  abs_files = [str(PROJECT_ROOT / f) for f in test_files]
  return int(pytest.main([*abs_files, *DEFAULT_PYTEST_ARGS, *pytest_args]))


def main(argv):
  try:
    args, pytest_args = parse_args(argv)
    seeds = resolve_seeds(args)
    steps = resolve_steps(args)
  except ValueError as exc:
    print(exc, file=sys.stderr)
    return 2

  selected = selected_workloads(args.workload)
  return run_simulation(
    test_files=[test_file for _, test_file in selected],
    seeds=seeds,
    steps=steps,
    pytest_args=pytest_args,
  )


if __name__ == "__main__":
  raise SystemExit(main(sys.argv[1:]))
