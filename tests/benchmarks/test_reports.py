from benchmarks.reports import render_markdown


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
  assert "1000.000" in markdown
  assert "| 100 | 0 | 0.100 | 1000.000 | 5 |" in markdown


def test_render_markdown_report_uses_recorded_run_metadata():
  markdown = render_markdown(
    [
      benchmark_row(
        metadata={
          "benchmark": {
            "worker_count": 2,
            "worker_threads": 4,
            "preserve_finished_jobs": False,
          },
          "run": benchmark_run_metadata(
            worker_count=2,
            worker_threads=4,
            preserve_finished_jobs=False,
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
  assert "docker compose up postgres -d" in markdown
  assert (
    "bin/benchmark.py all --backend postgres --sizes 1000,10000 --warmups 1 "
    "--runs 3 --output benchmark-results/postgres-all.jsonl --worker-count 2 "
    "--worker-threads 4 --no-preserve-finished-jobs"
  ) in markdown
  assert (
    "bin/benchmark.py report benchmark-results/postgres-all.jsonl "
    "--output docs/benchmarks/postgres-all.md"
  ) in markdown


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


def benchmark_row(*, metadata=None):
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
    base_metadata.update(metadata)
  return {
    "scenario": "bulk-enqueue",
    "size": 100,
    "run_index": 0,
    "metrics": {
      "duration_seconds": 0.1,
      "jobs_per_second": 1000.0,
      "query_count": 5,
    },
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
