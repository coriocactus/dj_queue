from pathlib import Path

from benchmarks.catalog import SCENARIO_CONTEXT, SCENARIO_DESCRIPTIONS
from benchmarks.reports import render_four_horsemen_markdown, render_markdown


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_render_markdown_report_includes_environment_and_metrics():
  markdown = render_markdown(
    [
      {
        "scenario": "bulk-enqueue",
        "size": 100,
        "run_index": 0,
        "metrics": {
          "duration_seconds": 0.1,
          "jobs_per_second": 1000.0,
          "query_count": 5,
        },
        "metadata": {
          "backend": "postgres",
          "database": {
            "vendor": "postgresql",
            "name": "dj_queue_benchmark",
            "version": "PostgreSQL 17",
          },
          "python": "3.14",
          "django": "6.0",
          "dj_queue": "0.6.4",
          "platform": "test-platform",
          "machine": "test-machine",
          "git_revision": "abc123",
        },
      }
    ]
  )

  assert "# dj_queue PostgreSQL Benchmark Report" in markdown
  assert "`dj_queue_benchmark`" in markdown
  assert "bulk-enqueue" in markdown
  assert (
    "### `bulk-enqueue`: bulk immediate enqueue throughput and SQL statement count" in markdown
  )
  assert "- key metric: **`jobs_per_second`** - bulk enqueue throughput" in markdown
  assert (
    "- healthy local baseline: `>= 6,000 jobs/sec` for 10k independent "
    "immediate jobs in under 2 seconds" in markdown
  )
  assert "- use case: imports, backfills, and fan-out jobs" in markdown
  assert "- mechanics: calls `DjQueueBackend.enqueue_all()`" in markdown
  assert "1000.000" in markdown
  assert "| size | run | duration_seconds | **jobs_per_second** | query_count |" in markdown
  assert "| 100 | 0 | 0.100 | **1000.000** | 5 |" in markdown


def test_render_markdown_report_includes_all_scenario_descriptions():
  markdown = render_markdown(
    [
      benchmark_row(scenario=scenario, metrics={context["key_metric"]: 1.0})
      for scenario, context in SCENARIO_CONTEXT.items()
    ]
  )

  for scenario, description in SCENARIO_DESCRIPTIONS.items():
    assert f"### `{scenario}`: {description}" in markdown

  for context in SCENARIO_CONTEXT.values():
    assert (
      f"- key metric: **`{context['key_metric']}`** - {context['key_metric_note']}" in markdown
    )
    assert f"- healthy local baseline: {context['healthy_local_baseline']}" in markdown
    assert f"- use case: {context['use_case']}" in markdown
    assert f"- mechanics: {context['mechanics']}" in markdown


def test_render_markdown_report_uses_recorded_run_metadata():
  markdown = render_markdown(
    [
      benchmark_row(
        metadata={
          "benchmark": {
            "worker_count": 2,
            "worker_threads": 4,
            "preserve_finished_jobs": False,
            "conn_max_age": 60,
          },
          "run": benchmark_run_metadata(
            worker_count=2,
            worker_threads=4,
            preserve_finished_jobs=False,
            conn_max_age=60,
          ),
        }
      )
    ],
    input_path="benchmark-results/postgres-all.jsonl",
    output_path="docs/benchmarks/postgres-all.md",
  )

  assert "- benchmark worker count: `2`" in markdown
  assert "- benchmark worker threads: `4`" in markdown
  assert "- preserve finished jobs: `False`" in markdown
  assert "- database CONN_MAX_AGE: `60`" in markdown
  assert "docker compose up postgres -d" in markdown
  assert (
    "bin/benchmark.py all --backend postgres --sizes 1000,10000 --warmups 1 "
    "--runs 3 --output benchmark-results/postgres-all.jsonl --worker-count 2 "
    "--worker-threads 4 --no-preserve-finished-jobs --conn-max-age 60"
  ) in markdown
  assert (
    "bin/benchmark.py report benchmark-results/postgres-all.jsonl "
    "--output docs/benchmarks/postgres-all.md"
  ) in markdown


def test_render_markdown_report_uses_relative_reproduce_paths():
  input_path = PROJECT_ROOT / "benchmark-results" / "postgres-20260509T164423Z.jsonl"
  output_path = PROJECT_ROOT / "docs" / "benchmarks" / "postgres.md"

  markdown = render_markdown(
    [
      benchmark_row(
        metadata={
          "run": benchmark_run_metadata(output=str(input_path), conn_max_age=60),
        }
      )
    ],
    input_path=input_path,
    output_path=output_path,
  )

  assert str(PROJECT_ROOT) not in markdown
  assert (
    "bin/benchmark.py all --backend postgres --sizes 1000,10000 --warmups 1 "
    "--runs 3 --output benchmark-results/postgres-20260509T164423Z.jsonl "
    "--conn-max-age 60"
  ) in markdown
  assert (
    "bin/benchmark.py report benchmark-results/postgres-20260509T164423Z.jsonl "
    "--output docs/benchmarks/postgres.md"
  ) in markdown


def test_render_markdown_report_uses_relative_database_path():
  db_path = PROJECT_ROOT / "benchmark-results" / "dj_queue_benchmark.sqlite3"

  markdown = render_markdown(
    [
      benchmark_row(
        metadata={
          "backend": "sqlite",
          "database": {
            "vendor": "sqlite",
            "name": str(db_path),
            "version": "3.50.4",
          },
        }
      )
    ]
  )

  assert str(PROJECT_ROOT) not in markdown
  assert "- database: `sqlite` `benchmark-results/dj_queue_benchmark.sqlite3`" in markdown


def test_render_markdown_report_skips_sqlite_locking_scenarios():
  sqlite_metadata = {
    "backend": "sqlite",
    "database": {
      "vendor": "sqlite",
      "name": "benchmark-results/dj_queue_benchmark.sqlite3",
      "version": "3.50.4",
    },
  }

  markdown = render_markdown(
    [
      benchmark_row(metadata=sqlite_metadata),
      benchmark_row(scenario="concurrency-contention", metadata=sqlite_metadata),
      benchmark_row(scenario="ordered-selector-claim", metadata=sqlite_metadata),
    ]
  )

  assert "bulk-enqueue" in markdown
  assert "concurrency-contention" not in markdown
  assert "ordered-selector-claim" not in markdown


def test_render_markdown_report_accepts_run_command_override():
  run_command = (
    "bin/benchmark.py all --backend postgres --sizes 1000,10000 "
    "--warmups 1 --runs 3 --output benchmark-results/postgres-all.jsonl"
  )
  markdown = render_markdown(
    [benchmark_row()],
    input_path="benchmark-results/postgres-all.jsonl",
    output_path="docs/benchmarks/postgres-all.md",
    run_command=run_command,
  )

  assert run_command in markdown
  assert "--run-command 'bin/benchmark.py all --backend postgres" in markdown


def test_render_four_horsemen_report_compares_10k_key_metric_medians():
  markdown = render_four_horsemen_markdown(
    {
      "postgres": [
        benchmark_row(size=1000, metrics={"jobs_per_second": 1.0}),
        benchmark_row(size=10000, metrics={"jobs_per_second": 100.0}),
        benchmark_row(size=10000, metrics={"jobs_per_second": 300.0}),
        benchmark_row(size=10000, metrics={"jobs_per_second": 200.0}),
        benchmark_row(
          scenario="concurrency-contention",
          size=10000,
          metrics={"drain_jobs_per_second": 40.0},
        ),
      ],
      "mariadb": [
        benchmark_row(
          size=10000,
          metadata={"backend": "mariadb", "database": {"vendor": "mysql"}},
          metrics={"jobs_per_second": 20.0},
        ),
        benchmark_row(
          size=10000,
          metadata={"backend": "mariadb", "database": {"vendor": "mysql"}},
          metrics={"jobs_per_second": 10.0},
        ),
        benchmark_row(
          size=10000,
          metadata={"backend": "mariadb", "database": {"vendor": "mysql"}},
          metrics={"jobs_per_second": 30.0},
        ),
      ],
      "mysql": [
        benchmark_row(
          size=10000,
          metadata={"backend": "mysql", "database": {"vendor": "mysql"}},
          metrics={"jobs_per_second": 50.0},
        ),
      ],
      "sqlite": [
        benchmark_row(
          size=10000,
          metadata={
            "backend": "sqlite",
            "database": {
              "vendor": "sqlite",
              "name": str(PROJECT_ROOT / "benchmark-results" / "dj_queue_benchmark.sqlite3"),
            },
            "benchmark": {"worker_count": 1, "worker_threads": 1},
          },
          metrics={"jobs_per_second": 400.0},
        ),
        benchmark_row(
          scenario="concurrency-contention",
          size=10000,
          metadata={"backend": "sqlite", "database": {"vendor": "sqlite"}},
          metrics={"drain_jobs_per_second": 999.0},
        ),
      ],
    }
  )

  assert "# dj_queue Four Horsemen Benchmark Report" in markdown
  assert "## 10k median key metric comparison" in markdown
  assert "| scenario | key metric | postgres | mariadb | mysql | sqlite |" in markdown
  assert "| `bulk-enqueue` | `jobs_per_second` | 200.000 | 20.000 | 50.000 | 400.000 |" in markdown
  assert "| `concurrency-contention` | `drain_jobs_per_second` |" in markdown
  assert "not supported" in markdown
  assert "## Metadata" in markdown
  assert "| workers | `` | `` | `` | `1` |" in markdown
  assert "benchmark-results/dj_queue_benchmark.sqlite3" in markdown
  assert "## Scenario keys" in markdown
  assert "| `bulk-enqueue` | `jobs_per_second` | bulk enqueue throughput" in markdown
  assert markdown.index("## Metadata") < markdown.index("## Scenario keys")
  assert markdown.index("## Scenario keys") < markdown.index("## 10k median key metric comparison")


def benchmark_row(*, scenario="bulk-enqueue", size=100, metadata=None, metrics=None):
  base_metadata = {
    "backend": "postgres",
    "database": {
      "vendor": "postgresql",
      "name": "dj_queue_benchmark",
      "version": "PostgreSQL 17",
    },
    "python": "3.14",
    "django": "6.0",
    "dj_queue": "0.6.4",
    "platform": "test-platform",
    "machine": "test-machine",
    "git_revision": "abc123",
  }
  if metadata:
    if "database" in metadata:
      base_metadata["database"].update(metadata["database"])
      metadata = {key: value for key, value in metadata.items() if key != "database"}
    base_metadata.update(metadata)
  row_metrics = {
    "duration_seconds": 0.1,
    "jobs_per_second": 1000.0,
    "query_count": 5,
  }
  if metrics:
    row_metrics.update(metrics)
  return {
    "scenario": scenario,
    "size": size,
    "run_index": 0,
    "metrics": row_metrics,
    "metadata": base_metadata,
  }


def benchmark_run_metadata(**overrides):
  metadata = {
    "command": "all",
    "scenario": None,
    "sizes": [1000, 10000],
    "runs": 3,
    "warmups": 1,
    "output": "benchmark-results/postgres-all.jsonl",
    "create_db": True,
    "migrate": True,
    "reset_db": True,
  }
  metadata.update(overrides)
  return metadata
