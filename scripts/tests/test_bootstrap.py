from __future__ import annotations

import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path

import pytest

from scripts.bootstrap import (
    BootstrapContext,
    BootstrapError,
    environment_python,
    parse_args,
    prepare_submodules,
    resolve_repository_root,
    verify_submodule_commit,
    verify_windows_symlinks,
)


class RecordingRunner:
    def __init__(self, results: list[subprocess.CompletedProcess[str]] | None = None) -> None:
        self.calls: list[tuple[tuple[str, ...], Path, dict[str, str]]] = []
        self._results = list(results or [])

    def __call__(
        self,
        args: Sequence[str],
        *,
        cwd: Path,
        env: Mapping[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append((tuple(args), cwd, dict(env or {})))
        if self._results:
            return self._results.pop(0)
        return subprocess.CompletedProcess(args, 0, "", "")


def test_parse_args_defaults_to_mutating_mode() -> None:
    assert parse_args([]).check is False


def test_parse_args_accepts_check_mode() -> None:
    assert parse_args(["--check"]).check is True


def test_environment_python_is_platform_specific(tmp_path: Path) -> None:
    assert environment_python(tmp_path, "windows") == tmp_path / "Scripts" / "python.exe"
    assert environment_python(tmp_path, "macos") == tmp_path / "bin" / "python"


def test_repository_root_requires_expected_configuration(tmp_path: Path) -> None:
    script = tmp_path / "scripts" / "bootstrap.py"
    script.parent.mkdir()
    script.touch()

    with pytest.raises(BootstrapError, match="pyproject.toml"):
        resolve_repository_root(script)


def test_context_uses_only_fixed_repository_paths(tmp_path: Path) -> None:
    context = BootstrapContext(root=tmp_path, platform="macos", check_only=True)

    assert context.root_environment == tmp_path / ".venv"
    assert context.metricflow_environment == tmp_path / "vendor" / "metricflow" / ".venv"
    assert context.dbt_metricflow_environment == tmp_path / "vendor" / "dbt-metricflow" / ".venv"


def completed(
    stdout: str = "",
    returncode: int = 0,
    stderr: str = "",
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(("git",), returncode, stdout, stderr)


def test_dirty_submodule_stops_before_update(tmp_path: Path) -> None:
    context = BootstrapContext(tmp_path, "windows", False)
    (tmp_path / "vendor" / "dbt" / ".git").mkdir(parents=True)
    runner = RecordingRunner([completed(" M metricflow/file.py\n")])

    with pytest.raises(BootstrapError, match="vendor/dbt"):
        prepare_submodules(context, runner)

    assert all("update" not in args for args, _, _ in runner.calls)


def test_check_mode_never_configures_or_updates_git(tmp_path: Path) -> None:
    context = BootstrapContext(tmp_path, "windows", True)
    runner = RecordingRunner()

    with pytest.raises(BootstrapError, match="not initialized"):
        prepare_submodules(context, runner)

    forbidden = {"config", "update", "restore", "deinit", "reset", "checkout"}
    assert not any(forbidden.intersection(args) for args, _, _ in runner.calls)


def test_unexpected_submodule_commit_fails(tmp_path: Path) -> None:
    context = BootstrapContext(tmp_path, "macos", True)
    runner = RecordingRunner(
        [
            completed("160000 commit expected\tvendor/metricflow\n"),
            completed("actual\n"),
        ]
    )

    with pytest.raises(BootstrapError, match="gitlink"):
        verify_submodule_commit(context, runner, Path("vendor/metricflow"))


def test_windows_symlink_check_rejects_regular_file(tmp_path: Path) -> None:
    context = BootstrapContext(tmp_path, "windows", True)
    link_path = Path("fixtures/project_configuration.yaml")
    absolute_link = tmp_path / "vendor" / "metricflow" / link_path
    absolute_link.parent.mkdir(parents=True)
    absolute_link.write_text("../shared/project_configuration.yaml", encoding="utf-8")
    runner = RecordingRunner(
        [
            completed(f"120000 hash 0\t{link_path.as_posix()}\0"),
            completed(),
        ]
    )

    with pytest.raises(BootstrapError, match="regular file"):
        verify_windows_symlinks(context, runner)


def test_windows_symlink_check_accepts_real_link(tmp_path: Path) -> None:
    context = BootstrapContext(tmp_path, "windows", True)
    link_path = Path("fixtures/project_configuration.yaml")
    link_parent = tmp_path / "vendor" / "metricflow" / link_path.parent
    link_parent.mkdir(parents=True)
    target = link_parent / "shared.yaml"
    target.touch()
    try:
        (tmp_path / "vendor" / "metricflow" / link_path).symlink_to(target)
    except OSError as error:
        pytest.skip(f"Current process cannot create Windows symlinks: {error}")
    runner = RecordingRunner(
        [
            completed(f"120000 hash 0\t{link_path.as_posix()}\0"),
            completed(),
        ]
    )

    verify_windows_symlinks(context, runner)
