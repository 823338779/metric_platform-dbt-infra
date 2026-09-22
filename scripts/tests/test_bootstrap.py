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
    resolve_repository_root,
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
