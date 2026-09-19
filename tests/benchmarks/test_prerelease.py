import json
from unittest.mock import Mock

import pytest

from bin import prerelease


def test_paths_are_absolute_and_database_is_generated(monkeypatch, tmp_path):
  monkeypatch.chdir(tmp_path)
  args = prerelease.parse_args(
    ["--from-ref", "old", "--to-ref", "new", "--backend", "sqlite", "--result-dir", "results"]
  )

  assert args.result_dir == tmp_path / "results"
  assert args.database_name == str(tmp_path / "results" / "prerelease.sqlite3")


@pytest.mark.parametrize("protocol", [None, True, "1", 2])
def test_rollout_compatibility_rejects_missing_or_different_protocol(protocol):
  with pytest.raises(ValueError, match="protocol"):
    prerelease.validate_rollout_compatibility(
      {"rollout_protocol": protocol, "django_version": "6.0.1"},
      {"rollout_protocol": 1, "django_version": "6.0.1"},
    )


def test_rollout_compatibility_requires_same_django():
  old = {"rollout_protocol": 1, "django_version": "6.0.1"}
  assert prerelease.validate_rollout_compatibility(old, old) == {"x": old, "y": old}
  with pytest.raises(ValueError, match="same Django"):
    prerelease.validate_rollout_compatibility(old, {**old, "django_version": "6.1.0"})


def test_resolve_revisions_rejects_unrelated_revisions(monkeypatch):
  monkeypatch.setattr(prerelease, "git_output", Mock(side_effect=["a" * 40, "b" * 40]))
  monkeypatch.setattr(prerelease.subprocess, "run", Mock(return_value=Mock(returncode=1)))

  with pytest.raises(ValueError, match="not an ancestor"):
    prerelease.resolve_revisions("old", "new")


def test_wait_for_requires_progress_and_checks_processes(monkeypatch):
  run = Mock(side_effect=[{"depth": 1}, {"depth": 0}])
  process = Mock(spec=prerelease.ManagedProcess)
  monkeypatch.setattr(prerelease, "run_runtime", run)
  monkeypatch.setattr(prerelease.time, "sleep", lambda _seconds: None)

  result = prerelease.wait_for(None, None, [process], "drain", lambda value: value["depth"] == 0)

  assert result == {"depth": 0}
  assert run.call_count == 2
  assert process.assert_running.call_count == 4


def test_wait_for_rejects_missing_progress(monkeypatch):
  monkeypatch.setattr(prerelease, "run_runtime", Mock(return_value={"depth": 1}))
  monkeypatch.setattr(prerelease.time, "monotonic", Mock(side_effect=[0, 0, 61]))
  monkeypatch.setattr(prerelease.time, "sleep", lambda _seconds: None)

  with pytest.raises(RuntimeError, match="insufficient progress"):
    prerelease.wait_for(None, None, [], "drain", lambda value: value["depth"] == 0)


def test_runtime_environment_excludes_checkout_import_paths(monkeypatch, tmp_path):
  monkeypatch.setenv("PYTHONPATH", "/unrelated/checkout")
  monkeypatch.setenv("PYTHONHOME", "/unrelated/python")
  args = prerelease.parse_args(
    ["--from-ref", "old", "--to-ref", "new", "--result-dir", str(tmp_path)]
  )

  env = prerelease.runtime_env(args, "Y")

  assert "PYTHONPATH" not in env
  assert "PYTHONHOME" not in env
  assert env["PRERELEASE_RUNTIME_LABEL"] == "Y"


@pytest.mark.parametrize("failure", [None, "create-database", "check", "drop-database", "stop"])
def test_run_records_failure_and_only_drops_owned_database(failure, monkeypatch, tmp_path):
  args = prerelease.parse_args(
    ["--from-ref", "old", "--to-ref", "new", "--result-dir", str(tmp_path / "results")]
  )
  monkeypatch.setattr(prerelease, "resolve_revisions", lambda *_args: ("old", "new"))
  monkeypatch.setattr(
    prerelease,
    "build_runtime",
    lambda label, revision, **_kwargs: prerelease.RevisionRuntime(
      label, revision, args.result_dir / f"{label}.whl", "hash", tmp_path / label
    ),
  )
  commands = []

  def run_runtime(_runtime, _args, command, **_kwargs):
    commands.append(command)
    if command == failure:
      raise RuntimeError(f"failed {command}")
    if command == "compatibility":
      return {"rollout_protocol": 1, "django_version": "6.0.1"}
    return None

  process = Mock()
  if failure == "stop":
    process.stop.side_effect = RuntimeError("failed stop")

  def check(_x, _y, _args, processes, _outcome):
    processes.append(process)
    if failure == "check":
      raise RuntimeError("failed check")

  monkeypatch.setattr(prerelease, "run_runtime", run_runtime)
  monkeypatch.setattr(prerelease, "check_upgrade", check)

  code = prerelease.run(args)

  manifest = json.loads((args.result_dir / "manifest.json").read_text())
  assert code == (0 if failure is None else 1)
  assert manifest["status"] == ("passed" if failure is None else "failed")
  assert ("drop-database" in commands) is (failure != "create-database")
  if failure != "create-database":
    process.stop.assert_called_once()
  if failure == "drop-database":
    assert manifest["database_cleanup"] == "failed"
