# Portable Development Bootstrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add idempotent Windows and macOS bootstrap commands that install prerequisites, create three Python 3.12 environments at stable repository-relative paths, initialize pinned submodules, and verify the project is ready for PyCharm.

**Architecture:** Thin PowerShell and Bash launchers install platform prerequisites and invoke one standard-library Python orchestrator. The orchestrator owns allow-listed Git, uv, and Hatch operations, stores all environments inside the checkout, and exposes a read-only `--check` mode. Command execution and filesystem/platform probes are injected so unit tests never install software, elevate privileges, or mutate real submodules.

**Tech Stack:** PowerShell 7/Windows PowerShell 5.1, Bash 3.2+, Python 3.12 standard library, uv 0.12+, Hatch 1.18.1, pytest, Ruff, Git submodules

**Spec:** `docs/superpowers/specs/2026-09-22-portable-development-bootstrap-design.md`

## Global Constraints

- Support Windows with `winget` and macOS with Homebrew; Linux is outside the initial scope.
- Use the latest uv-managed CPython `3.12` patch release.
- Do not install, launch, or modify PyCharm.
- Do not modify source files inside `vendor/dbt`, `vendor/metricflow`, or `vendor/dbt-metricflow`.
- Do not commit user names, drive letters, checkout paths, or Hatch cache hashes.
- Keep the root uv environment isolated from the two upstream Hatch environments.
- Never reset, deinitialize, restore, or overwrite a dirty submodule.
- Accept no arbitrary command strings or arbitrary filesystem targets.
- Use `from __future__ import annotations`, a module logger, and complete type annotations in every new Python file.
- Run affected tests, complete `pytest`, and `ruff check` before completion.

## Review Focus

- A checkout path containing spaces must be passed as one subprocess argument and must not be interpolated into a shell string; test every generated command through the injected runner.
- An existing `.venv` created by Python 3.11, 3.13, or 3.14 must stop with a scoped recovery command and must not be deleted; test version inspection before every mutating sync.
- A submodule with tracked or untracked changes must stop before symlink repair; test dirty output for each vendored path.
- `--check` must not execute package installation, Git configuration, submodule update, restore, or dependency synchronization; assert the full captured command list.
- The known dbt-metricflow editable-version warning must be narrowly allow-listed while any other `pip check` output fails; test the exact accepted line and a second unexpected line.

---

### Task 1: Bootstrap Core and Read-Only Project Model

**Files:**
- Create: `.python-version`
- Create: `scripts/bootstrap.py`
- Create: `scripts/tests/test_bootstrap.py`
- Modify: `pyproject.toml`

**Interfaces:**
- Consumes: repository root, `--check`, `sys.platform`, and injected command runner
- Produces: `BootstrapContext`, `BootstrapError`, `parse_args()`, `resolve_repository_root()`, `environment_python()`, `run_checked()`, and `main()`

- [ ] **Step 1: Write failing tests for the project model and CLI**

Create `scripts/tests/test_bootstrap.py` with the initial contracts:

```python
from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Mapping, Sequence

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
```

- [ ] **Step 2: Add bootstrap tests to the root pytest suite and verify failure**

Change `pyproject.toml` to:

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests", "scripts/tests"]
```

Create `.python-version` with exactly:

```text
3.12
```

Run:

```powershell
uv run pytest scripts/tests/test_bootstrap.py -v
```

Expected: collection fails because `scripts.bootstrap` does not exist.

- [ ] **Step 3: Implement the typed project model and argument parser**

Create `scripts/bootstrap.py` with these public definitions:

```python
from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

logger = logging.getLogger(__name__)

PYTHON_VERSION = "3.12"
HATCH_VERSION = "1.18.1"
PlatformName = Literal["windows", "macos"]


class BootstrapError(RuntimeError):
    """Report an actionable bootstrap failure without a traceback."""


class CommandRunner(Protocol):
    def __call__(
        self,
        args: Sequence[str],
        *,
        cwd: Path,
        env: Mapping[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        """Run one allow-listed command without invoking a shell."""
        raise NotImplementedError


@dataclass(frozen=True)
class BootstrapContext:
    root: Path
    platform: PlatformName
    check_only: bool

    @property
    def root_environment(self) -> Path:
        return self.root / ".venv"

    @property
    def metricflow_environment(self) -> Path:
        return self.root / "vendor" / "metricflow" / ".venv"

    @property
    def dbt_metricflow_environment(self) -> Path:
        return self.root / "vendor" / "dbt-metricflow" / ".venv"


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare local development environments.")
    parser.add_argument("--check", action="store_true", help="Verify without changing the computer or checkout.")
    return parser.parse_args(argv)


def resolve_repository_root(script_path: Path) -> Path:
    root = script_path.resolve().parents[1]
    required = ("pyproject.toml", "uv.lock", ".gitmodules")
    missing = [name for name in required if not (root / name).is_file()]
    if missing:
        raise BootstrapError(f"Repository root is missing required file: {missing[0]}")
    return root


def environment_python(environment: Path, platform: PlatformName) -> Path:
    return environment / ("Scripts/python.exe" if platform == "windows" else "bin/python")
```

Implement a real runner with `shell=False` and captured UTF-8 output. It returns `CompletedProcess` for every exit code so `pip check` can inspect its expected nonzero result. Add `run_checked()` as the single wrapper that raises `BootstrapError` for nonzero results; its error contains the executable, arguments, exit code, stdout, and stderr but no environment dump. `main()` must map `win32` to `windows`, `darwin` to `macos`, reject other platforms, configure logging, and return `2` for `BootstrapError`.

```python
def run_checked(
    runner: CommandRunner,
    args: Sequence[str],
    *,
    cwd: Path,
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    result = runner(args, cwd=cwd, env=env)
    if result.returncode != 0:
        command = repr(tuple(args))
        raise BootstrapError(
            f"Command failed with exit code {result.returncode}: {command}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result
```

- [ ] **Step 4: Run focused tests and Ruff**

Run:

```powershell
uv run pytest scripts/tests/test_bootstrap.py -v
uv run ruff check scripts/bootstrap.py scripts/tests/test_bootstrap.py
```

Expected: all Task 1 tests pass and Ruff reports no errors.

- [ ] **Step 5: Commit Task 1**

```powershell
git add .python-version pyproject.toml scripts/bootstrap.py scripts/tests/test_bootstrap.py
git commit -m "build: add portable bootstrap core"
```

---

### Task 2: Safe Submodule and Windows Symlink Preparation

**Files:**
- Modify: `scripts/bootstrap.py`
- Modify: `scripts/tests/test_bootstrap.py`

**Interfaces:**
- Consumes: `BootstrapContext`, `CommandRunner`, fixed `SUBMODULE_PATHS`
- Produces: `verify_submodule_commit()`, `prepare_submodules()`, `tracked_symlinks()`, and `verify_windows_symlinks()`

- [ ] **Step 1: Write failing tests for clean/dirty submodules and check mode**

Add tests that use `RecordingRunner` to establish these exact behaviors:

```python
def completed(stdout: str = "", returncode: int = 0, stderr: str = "") -> subprocess.CompletedProcess[str]:
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
    runner = RecordingRunner([completed("expected\n"), completed("actual\n")])

    with pytest.raises(BootstrapError, match="gitlink"):
        verify_submodule_commit(context, runner, Path("vendor/metricflow"))
```

Add a filesystem test where a known fixture path is a regular file containing `../shared/project_configuration.yaml`; `verify_windows_symlinks()` must reject it. Add the corresponding passing test using `os.symlink()` and skip only when the test process cannot create symlinks.

- [ ] **Step 2: Run the new tests to verify failure**

Run:

```powershell
uv run pytest scripts/tests/test_bootstrap.py -k "submodule or symlink or gitlink" -v
```

Expected: failures report the missing submodule functions.

- [ ] **Step 3: Implement allow-listed submodule inspection**

Add constants and functions with these exact signatures:

```python
SUBMODULE_PATHS = (
    Path("vendor/dbt"),
    Path("vendor/metricflow"),
    Path("vendor/dbt-metricflow"),
)


def submodule_is_clean(context: BootstrapContext, runner: CommandRunner, relative_path: Path) -> bool:
    """Return whether an initialized submodule has no tracked or untracked changes."""
    result = run_checked(
        runner,
        ("git", "-C", str(context.root / relative_path), "status", "--porcelain=v1", "--untracked-files=all"),
        cwd=context.root,
    )
    return not result.stdout.strip()


def verify_submodule_commit(
    context: BootstrapContext,
    runner: CommandRunner,
    relative_path: Path,
) -> None:
    """Require the submodule HEAD to equal the gitlink recorded by the parent."""
    tree = run_checked(runner, ("git", "ls-tree", "HEAD", relative_path.as_posix()), cwd=context.root)
    fields = tree.stdout.split()
    if len(fields) < 3 or fields[0] != "160000":
        raise BootstrapError(f"Missing gitlink for {relative_path.as_posix()}")
    expected = fields[2]
    actual = run_checked(
        runner,
        ("git", "-C", str(context.root / relative_path), "rev-parse", "HEAD"),
        cwd=context.root,
    ).stdout.strip()
    if actual != expected:
        raise BootstrapError(f"Submodule does not match parent gitlink: {relative_path.as_posix()}")


def tracked_symlinks(
    context: BootstrapContext,
    runner: CommandRunner,
    relative_path: Path,
) -> tuple[Path, ...]:
    """Return tracked mode-120000 paths from one initialized submodule."""
    result = run_checked(
        runner,
        ("git", "-C", str(context.root / relative_path), "ls-files", "-s", "-z"),
        cwd=context.root,
    )
    links: list[Path] = []
    for record in result.stdout.split("\0"):
        if not record:
            continue
        metadata, filename = record.split("\t", maxsplit=1)
        if metadata.split(maxsplit=1)[0] == "120000":
            links.append(Path(filename))
    return tuple(links)


def verify_windows_symlinks(context: BootstrapContext, runner: CommandRunner) -> None:
    """Require every tracked Git symlink in both MetricFlow checkouts to be a real link."""
    for relative_path in (Path("vendor/metricflow"), Path("vendor/dbt-metricflow")):
        for link_path in tracked_symlinks(context, runner, relative_path):
            if not (context.root / relative_path / link_path).is_symlink():
                raise BootstrapError(f"Git symlink was checked out as a regular file: {relative_path / link_path}")


def prepare_submodules(context: BootstrapContext, runner: CommandRunner) -> None:
    """Initialize or inspect pinned submodules without overwriting user work."""
    initialized = tuple(path for path in SUBMODULE_PATHS if (context.root / path / ".git").exists())
    for relative_path in initialized:
        if not submodule_is_clean(context, runner, relative_path):
            raise BootstrapError(f"Submodule contains local changes: {relative_path.as_posix()}")
    if context.check_only:
        if initialized != SUBMODULE_PATHS:
            raise BootstrapError("Submodules are not initialized; run bootstrap without --check")
    else:
        if context.platform == "windows":
            run_checked(runner, ("git", "config", "core.symlinks", "true"), cwd=context.root)
        run_checked(runner, ("git", "submodule", "update", "--init", "--recursive"), cwd=context.root)
        if context.platform == "windows":
            for relative_path in (Path("vendor/metricflow"), Path("vendor/dbt-metricflow")):
                run_checked(
                    runner,
                    ("git", "-C", str(context.root / relative_path), "config", "core.symlinks", "true"),
                    cwd=context.root,
                )
                for link_path in tracked_symlinks(context, runner, relative_path):
                    if not (context.root / relative_path / link_path).is_symlink():
                        run_checked(
                            runner,
                            ("git", "-C", str(context.root / relative_path), "restore", "--worktree", "--", link_path.as_posix()),
                            cwd=context.root,
                        )
    for relative_path in SUBMODULE_PATHS:
        verify_submodule_commit(context, runner, relative_path)
    if context.platform == "windows":
        verify_windows_symlinks(context, runner)
```

Use only these Git forms:

```text
git -C <submodule> status --porcelain=v1 --untracked-files=all
git ls-tree HEAD <submodule>
git -C <submodule> rev-parse HEAD
git -C <repo> config core.symlinks true
git submodule update --init --recursive
git -C <submodule> ls-files -s -z
git -C <submodule> restore --worktree -- <tracked-symlink-path>
```

Keep the implementation above split into focused helpers if Ruff line-length or complexity checks require it; preserve the exact command allow-list and ordering. Before any Windows repair, require all initialized submodules to be clean. Set `core.symlinks=true` in the parent and initialized MetricFlow submodules, update recursively, restore only tracked symlink paths that are not actual links, then verify them. Never call `reset`, `deinit`, or unrestricted `restore`.

In `--check`, only inspect initialization, gitlink commits, dirty state, and actual link types.

- [ ] **Step 4: Run submodule tests and the current dependency commit test**

Run:

```powershell
uv run pytest scripts/tests/test_bootstrap.py -k "submodule or symlink or gitlink" -v
uv run pytest tests/test_dependencies.py::test_vendored_sources_are_at_audited_commits -v
uv run ruff check scripts/bootstrap.py scripts/tests/test_bootstrap.py
```

Expected: all commands pass.

- [ ] **Step 5: Commit Task 2**

```powershell
git add scripts/bootstrap.py scripts/tests/test_bootstrap.py
git commit -m "build: prepare submodules safely"
```

---

### Task 3: Environment Creation and Verification

**Files:**
- Modify: `scripts/bootstrap.py`
- Modify: `scripts/tests/test_bootstrap.py`

**Interfaces:**
- Consumes: uv executable on `PATH`, `BootstrapContext`, `CommandRunner`, Python 3.12
- Produces: `find_uv_python()`, `sync_environments()`, `verify_environments()`, and a complete orchestrator flow

- [ ] **Step 1: Write failing tests for environment validation and command generation**

Add tests for these behaviors:

```python
def test_wrong_existing_python_is_not_replaced(tmp_path: Path) -> None:
    context = BootstrapContext(tmp_path, "macos", False)
    python = environment_python(context.root_environment, context.platform)
    python.parent.mkdir(parents=True)
    python.touch()
    runner = RecordingRunner([completed("3.11\n")])

    with pytest.raises(BootstrapError, match="Python 3.12"):
        require_environment_version(context, runner, context.root_environment)

    assert not any("sync" in args or "env" in args for args, _, _ in runner.calls)


def test_metricflow_hatch_environment_uses_local_path(tmp_path: Path) -> None:
    root = tmp_path / "checkout with spaces"
    context = BootstrapContext(root, "windows", False)
    runner = RecordingRunner()

    create_hatch_environment(
        context,
        runner,
        project=root / "vendor" / "metricflow",
        environment=root / "vendor" / "metricflow" / ".venv",
        python=Path("C:/uv/python.exe"),
    )

    args, cwd, env = runner.calls[0]
    assert args == ("uv", "tool", "run", "--from", "hatch==1.18.1", "hatch", "env", "create", "dev-env")
    assert cwd == root / "vendor" / "metricflow"
    assert env["HATCH_ENV_TYPE_VIRTUAL_PATH"] == str(root / "vendor" / "metricflow" / ".venv")
    assert env["HATCH_PYTHON"] == str(Path("C:/uv/python.exe"))


def test_check_mode_does_not_install_or_sync(tmp_path: Path) -> None:
    context = BootstrapContext(tmp_path, "macos", True)
    runner = RecordingRunner()

    sync_environments(context, runner)

    forbidden = {"install", "sync", "create"}
    assert not any(forbidden.intersection(args) for args, _, _ in runner.calls)


def test_pip_check_accepts_only_the_known_editable_mismatch(tmp_path: Path) -> None:
    context = BootstrapContext(tmp_path, "windows", True)
    runner = RecordingRunner([completed(DBT_METRICFLOW_EDITABLE_MISMATCH + "\n", returncode=1)])

    verify_pip_check(
        context,
        runner,
        Path("python.exe"),
        allowed_lines=frozenset({DBT_METRICFLOW_EDITABLE_MISMATCH}),
    )


def test_pip_check_rejects_an_additional_problem(tmp_path: Path) -> None:
    context = BootstrapContext(tmp_path, "windows", True)
    output = DBT_METRICFLOW_EDITABLE_MISMATCH + "\nmissing-package 1.0 requires absent-package\n"
    runner = RecordingRunner([completed(output, returncode=1)])

    with pytest.raises(BootstrapError, match="missing-package"):
        verify_pip_check(
            context,
            runner,
            Path("python.exe"),
            allowed_lines=frozenset({DBT_METRICFLOW_EDITABLE_MISMATCH}),
        )
```

Add tests for missing interpreter files, root import-source verification, and clean `pip check`; the code above already pins paths containing spaces and both dbt-metricflow mismatch branches.

- [ ] **Step 2: Run environment tests to verify failure**

Run:

```powershell
uv run pytest scripts/tests/test_bootstrap.py -k "environment or hatch or python or pip_check or import_source" -v
```

Expected: failures identify the missing environment functions.

- [ ] **Step 3: Implement Python and environment synchronization**

Add these functions:

```python
def find_uv_python(context: BootstrapContext, runner: CommandRunner) -> Path:
    """Return the uv-managed CPython 3.12 executable."""
    result = run_checked(runner, ("uv", "python", "find", PYTHON_VERSION), cwd=context.root)
    python = Path(result.stdout.strip())
    if not python.is_file():
        raise BootstrapError(f"uv returned a missing Python interpreter: {python}")
    return python


def require_environment_version(
    context: BootstrapContext,
    runner: CommandRunner,
    environment: Path,
) -> None:
    """Reject an existing environment that is missing or not based on Python 3.12."""
    if not environment.exists():
        return
    python = environment_python(environment, context.platform)
    if not python.is_file():
        raise BootstrapError(f"Existing environment has no Python interpreter: {environment}")
    result = run_checked(
        runner,
        (str(python), "-c", "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"),
        cwd=context.root,
    )
    if result.stdout.strip() != PYTHON_VERSION:
        raise BootstrapError(f"Existing environment must use Python 3.12: {environment}")


def create_hatch_environment(
    context: BootstrapContext,
    runner: CommandRunner,
    *,
    project: Path,
    environment: Path,
    python: Path,
) -> None:
    """Create one upstream dev-env at a stable project-local path."""
    hatch_env = dict(os.environ)
    hatch_env["HATCH_PYTHON"] = str(python)
    hatch_env["HATCH_ENV_TYPE_VIRTUAL_PATH"] = str(environment)
    run_checked(
        runner,
        ("uv", "tool", "run", "--from", f"hatch=={HATCH_VERSION}", "hatch", "env", "create", "dev-env"),
        cwd=project,
        env=hatch_env,
    )


def sync_environments(context: BootstrapContext, runner: CommandRunner) -> None:
    """Install Python and synchronize all three isolated environments."""
    if context.check_only:
        return
    for environment in (
        context.root_environment,
        context.metricflow_environment,
        context.dbt_metricflow_environment,
    ):
        require_environment_version(context, runner, environment)
    run_checked(runner, ("uv", "python", "install", PYTHON_VERSION), cwd=context.root)
    python = find_uv_python(context, runner)
    run_checked(
        runner,
        ("uv", "sync", "--frozen", "--all-groups", "--python", PYTHON_VERSION),
        cwd=context.root,
    )
    create_hatch_environment(
        context,
        runner,
        project=context.root / "vendor" / "metricflow",
        environment=context.metricflow_environment,
        python=python,
    )
    create_hatch_environment(
        context,
        runner,
        project=context.root / "vendor" / "dbt-metricflow" / "dbt-metricflow",
        environment=context.dbt_metricflow_environment,
        python=python,
    )
```

The mutating sequence must be:

```text
uv python install 3.12
uv python find 3.12
uv sync --frozen --all-groups --python 3.12
uv tool run --from hatch==1.18.1 hatch env create dev-env
uv tool run --from hatch==1.18.1 hatch env create dev-env
```

Run the first Hatch command in `vendor/metricflow` with its environment path. Run the second in `vendor/dbt-metricflow/dbt-metricflow`, but set its environment path to `vendor/dbt-metricflow/.venv`. Pass a merged copy of `os.environ` containing `HATCH_PYTHON` and `HATCH_ENV_TYPE_VIRTUAL_PATH`; never log the merged environment.

Before mutating an existing environment, execute its Python with `-c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"` and reject anything other than `3.12`.

- [ ] **Step 4: Implement verification commands and the known mismatch rule**

Add:

```python
DBT_METRICFLOW_EDITABLE_MISMATCH = (
    "dbt-metricflow 0.15.0 has requirement metricflow==0.213.0, "
    "but you have metricflow 0.214.0.dev0."
)


def verify_pip_check(
    context: BootstrapContext,
    runner: CommandRunner,
    python: Path,
    *,
    allowed_lines: frozenset[str] = frozenset(),
) -> None:
    """Accept a clean dependency graph or only explicitly allow-listed diagnostics."""
    result = runner((str(python), "-m", "pip", "check"), cwd=context.root)
    lines = frozenset(line.strip() for line in (result.stdout + result.stderr).splitlines() if line.strip())
    if result.returncode == 0:
        return
    unexpected = lines - allowed_lines
    if unexpected or not lines:
        raise BootstrapError("Dependency integrity check failed: " + "; ".join(sorted(lines)))


def verify_environments(context: BootstrapContext, runner: CommandRunner) -> None:
    """Validate versions, imports, local sources, lint, and representative tests."""
    root_python = environment_python(context.root_environment, context.platform)
    metricflow_python = environment_python(context.metricflow_environment, context.platform)
    dbt_metricflow_python = environment_python(context.dbt_metricflow_environment, context.platform)
    for environment in (
        context.root_environment,
        context.metricflow_environment,
        context.dbt_metricflow_environment,
    ):
        require_environment_version(context, runner, environment)
    run_checked(runner, (str(root_python), "-c", ROOT_IMPORT_AND_SOURCE_CHECK), cwd=context.root)
    run_checked(runner, (str(metricflow_python), "-c", METRICFLOW_IMPORT_CHECK), cwd=context.root)
    run_checked(runner, (str(dbt_metricflow_python), "-c", DBT_METRICFLOW_IMPORT_CHECK), cwd=context.root)
    verify_pip_check(context, runner, root_python)
    verify_pip_check(context, runner, metricflow_python)
    verify_pip_check(
        context,
        runner,
        dbt_metricflow_python,
        allowed_lines=frozenset({DBT_METRICFLOW_EDITABLE_MISMATCH}),
    )
    run_checked(
        runner,
        (str(root_python), "-m", "pytest", "tests/test_dependencies.py", "-v"),
        cwd=context.root,
    )
    run_checked(
        runner,
        (str(root_python), "-m", "ruff", "check", "src", "tests", "scripts"),
        cwd=context.root,
    )
    rendered_query = "tests_metricflow/integration/test_rendered_query.py::test_render_query"
    run_checked(
        runner,
        (str(metricflow_python), "-m", "pytest", rendered_query, "-v"),
        cwd=context.root / "vendor" / "metricflow",
    )
    run_checked(
        runner,
        (str(dbt_metricflow_python), "-m", "pytest", rendered_query, "-v"),
        cwd=context.root / "vendor" / "dbt-metricflow",
    )
```

Define `ROOT_IMPORT_AND_SOURCE_CHECK` as a multiline `-c` program that imports `dbt`, `dbt_metricflow`, `fastapi`, `metricflow`, and `pytest`; reads each distribution's `direct_url.json`; converts its file URL with `urlparse()` and `url2pathname()`; and compares the resolved paths to `vendor/dbt/core`, `vendor/dbt-metricflow/dbt-metricflow`, and `vendor/metricflow`. Define `METRICFLOW_IMPORT_CHECK` to import `duckdb`, `graphviz`, `metricflow`, `metricflow_semantic_interfaces`, `metricflow_semantics`, `pytest`, and `sqlalchemy`. Define `DBT_METRICFLOW_IMPORT_CHECK` to import `dbt`, `dbt_metricflow`, `metricflow`, and `pytest`.

Use these exact programs:

```python
ROOT_IMPORT_AND_SOURCE_CHECK = """
import importlib.metadata as metadata
import json
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname
import dbt, dbt_metricflow, fastapi, metricflow, pytest
expected = {
    "dbt-core": "vendor/dbt/core",
    "dbt-metricflow": "vendor/dbt-metricflow/dbt-metricflow",
    "metricflow": "vendor/metricflow",
}
for name, relative in expected.items():
    direct_url = json.loads(metadata.distribution(name).read_text("direct_url.json"))
    actual = Path(url2pathname(urlparse(direct_url["url"]).path)).resolve()
    required = (Path.cwd() / relative).resolve()
    if actual != required:
        raise SystemExit(f"{name} source mismatch: {actual} != {required}")
"""

METRICFLOW_IMPORT_CHECK = (
    "import duckdb, graphviz, metricflow, metricflow_semantic_interfaces, "
    "metricflow_semantics, pytest, sqlalchemy"
)
DBT_METRICFLOW_IMPORT_CHECK = "import dbt, dbt_metricflow, metricflow, pytest"
```

Verification must use each environment's Python executable directly and run:

```text
<root-python> -c <version/import/source-metadata check>
<root-python> -m pip check
<metricflow-python> -c <MetricFlow/SQLAlchemy/DuckDB/pytest imports>
<metricflow-python> -m pip check
<dbt-metricflow-python> -c <dbt/dbt_metricflow/metricflow/pytest imports>
<dbt-metricflow-python> -m pip check
<root-python> -m pytest tests/test_dependencies.py -v
<root-python> -m ruff check src tests scripts
<metricflow-python> -m pytest tests_metricflow/integration/test_rendered_query.py::test_render_query -v
<dbt-metricflow-python> -m pytest tests_metricflow/integration/test_rendered_query.py::test_render_query -v
```

The root metadata check must resolve `direct_url.json` paths and assert they equal the three allow-listed `vendor` locations. The dbt-metricflow `pip check` may ignore only the exact pinned editable mismatch constant; a successful empty or informational output also passes so an upstream correction does not break bootstrap.

- [ ] **Step 5: Wire the orchestrator flow and summary**

Make `main()` call, in order:

```python
prepare_submodules(context, subprocess_runner)
if not context.check_only:
    sync_environments(context, subprocess_runner)
verify_environments(context, subprocess_runner)
print_interpreter_summary(context)
```

The summary prints only paths relative to `context.root` and labels the Windows/macOS executable suffix.

- [ ] **Step 6: Run focused tests and Ruff**

Run:

```powershell
uv run pytest scripts/tests/test_bootstrap.py -v
uv run ruff check scripts/bootstrap.py scripts/tests/test_bootstrap.py
```

Expected: all bootstrap tests pass and Ruff reports no errors.

- [ ] **Step 7: Commit Task 3**

```powershell
git add scripts/bootstrap.py scripts/tests/test_bootstrap.py
git commit -m "build: create and verify local environments"
```

---

### Task 4: Windows and macOS Launchers

**Files:**
- Create: `bootstrap.ps1`
- Create: `bootstrap.sh`
- Modify: `scripts/tests/test_bootstrap.py`

**Interfaces:**
- Consumes: optional `--check`, Windows `winget`, macOS Homebrew, repository-local `scripts/bootstrap.py`
- Produces: one-command prerequisite installation and invocation of the shared orchestrator

- [ ] **Step 1: Write failing launcher contract tests**

Add tests that read both launchers and pin the safety-critical contract:

```python
def test_windows_launcher_uses_allow_listed_packages(project_root: Path) -> None:
    script = (project_root / "bootstrap.ps1").read_text(encoding="utf-8")

    assert "Git.Git" in script
    assert "astral-sh.uv" in script
    assert "AllowDevelopmentWithoutDevLicense" in script
    assert "Start-Process" in script and "-Verb RunAs" in script
    assert '"3.12"' in script
    assert '"hatch==1.18.1"' in script
    assert script.index("if ($Check) {") < script.index("winget install")


def test_macos_launcher_uses_homebrew_and_forwards_arguments(project_root: Path) -> None:
    script = (project_root / "bootstrap.sh").read_text(encoding="utf-8")

    assert script.startswith("#!/usr/bin/env bash\nset -euo pipefail\n")
    assert "brew install git uv" in script
    assert "uv python install 3.12" in script
    assert 'uv tool install "hatch==1.18.1"' in script
    assert '"$@"' in script
    assert script.index('if [[ "${1:-}" == "--check" ]]') < script.index("brew install git uv")
```

Define a local `project_root` fixture in the same file using `Path(__file__).parents[2]`.

- [ ] **Step 2: Run launcher tests to verify failure**

Run:

```powershell
uv run pytest scripts/tests/test_bootstrap.py -k launcher -v
```

Expected: tests fail because both launcher files are absent.

- [ ] **Step 3: Implement the Windows launcher**

Create `bootstrap.ps1` with:

```powershell
[CmdletBinding()]
param(
    [switch]$Check,
    [switch]$EnableDeveloperModeOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$repositoryRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$developerModeKey = "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\AppModelUnlock"

function Test-Administrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}
```

Implement `Enable-DeveloperMode` so the elevated-only branch creates the registry key when missing and sets `AllowDevelopmentWithoutDevLicense` to DWORD `1`. The normal branch uses `Start-Process` with `-Verb RunAs`, `-Wait`, and a quoted `-File` argument pointing only to the current launcher.

Branch on `$Check` before every mutation. In check mode, require `git` and `uv` to already resolve, read the Developer Mode registry value without elevation, and fail with an actionable message if any prerequisite is absent. Do not call `winget`, `Start-Process`, `uv python install`, or `uv tool install` in check mode. Locate an existing managed Python without downloads using `uv --no-python-downloads python find 3.12`, then invoke that executable directly with `scripts/bootstrap.py --check`.

Require `winget`; if unavailable, fail with the official App Installer requirement. Install exact package IDs with noninteractive agreement flags:

```text
winget install --id Git.Git --exact --source winget --accept-package-agreements --accept-source-agreements
winget install --id astral-sh.uv --exact --source winget --accept-package-agreements --accept-source-agreements
```

Resolve uv from `Get-Command uv` or `%LOCALAPPDATA%\Microsoft\WinGet\Links\uv.exe`. Then run `uv python install 3.12`, `uv tool install hatch==1.18.1`, and:

```powershell
$arguments = @("run", "--no-project", "--python", "3.12", "$repositoryRoot\scripts\bootstrap.py")
& $uvPath @arguments
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
```

- [ ] **Step 4: Implement the macOS launcher**

Create executable `bootstrap.sh` using only Bash 3.2-compatible syntax. When `brew` is absent, run the official Homebrew installer URL, then evaluate `brew shellenv` from `/opt/homebrew/bin/brew` or `/usr/local/bin/brew`. Run:

```bash
brew install git uv
uv python install 3.12
uv tool install "hatch==1.18.1"
uv run --no-project --python 3.12 "$repository_root/scripts/bootstrap.py" "$@"
```

Reject arguments other than no arguments or `--check` before installing anything. For `--check`, require existing `git` and `uv`, skip Homebrew and all install commands, obtain the interpreter with `uv --no-python-downloads python find 3.12`, and execute `"$python" "$repository_root/scripts/bootstrap.py" --check`. Mark the file executable with:

```bash
git update-index --chmod=+x bootstrap.sh
```

- [ ] **Step 5: Validate launcher syntax and contract tests**

On Windows run:

```powershell
$tokens = $null
$errors = $null
[System.Management.Automation.Language.Parser]::ParseFile(
    (Resolve-Path .\bootstrap.ps1),
    [ref]$tokens,
    [ref]$errors
) | Out-Null
if ($errors.Count -ne 0) { $errors; exit 1 }
uv run pytest scripts/tests/test_bootstrap.py -k launcher -v
```

On macOS CI or a macOS workstation run:

```bash
bash -n bootstrap.sh
uv run pytest scripts/tests/test_bootstrap.py -k launcher -v
```

Expected: syntax validation and launcher tests pass on their respective platforms.

- [ ] **Step 6: Commit Task 4**

```powershell
git add bootstrap.ps1 bootstrap.sh scripts/tests/test_bootstrap.py
git commit -m "build: add Windows and macOS bootstrap launchers"
```

---

### Task 5: Documentation and End-to-End Verification

**Files:**
- Modify: `README.md`
- Modify: `scripts/tests/test_bootstrap.py` only if an end-to-end defect requires a regression test

**Interfaces:**
- Consumes: completed launchers and orchestrator
- Produces: onboarding instructions, diagnostics, recovery instructions, and verified final behavior

- [ ] **Step 1: Replace the manual-first environment instructions with one-command setup**

Add a README section near “获取完整源码” with:

````markdown
## 一键配置开发环境

Windows：

```powershell
.\bootstrap.ps1
```

macOS：

```bash
./bootstrap.sh
```

只检查、不修改：

```powershell
.\bootstrap.ps1 --check
```

```bash
./bootstrap.sh --check
```
````

Explain that Windows may request administrator approval for Developer Mode and that macOS may request normal Homebrew/Xcode Command Line Tools setup interaction.

- [ ] **Step 2: Document stable PyCharm interpreter locations**

Add a table with exactly these locations:

| Module | Windows | macOS |
|---|---|---|
| Service | `.venv\Scripts\python.exe` | `.venv/bin/python` |
| MetricFlow | `vendor\metricflow\.venv\Scripts\python.exe` | `vendor/metricflow/.venv/bin/python` |
| dbt-metricflow | `vendor\dbt-metricflow\.venv\Scripts\python.exe` | `vendor/dbt-metricflow/.venv/bin/python` |

State that PyCharm itself is not installed or modified and that no `.idea` path is required. Keep the existing manual Hatch commands under a “手动诊断与恢复” subheading, updated to use the project-local environment variables rather than user-cache paths.

- [ ] **Step 3: Document safe recovery for incompatible environments and dirty submodules**

Include the exact recovery sequence for a user who explicitly chooses to rebuild an incompatible environment:

```powershell
Rename-Item .venv .venv.backup
.\bootstrap.ps1
```

```bash
mv .venv .venv.backup
./bootstrap.sh
```

Explain that the same pattern applies inside either vendored directory, and that dirty submodule output must be reviewed or committed by the user before rerunning. Do not recommend `git reset --hard` or recursive deletion.

- [ ] **Step 4: Run the non-mutating bootstrap check**

After a successful default bootstrap on the current platform, run:

```powershell
.\bootstrap.ps1 --check
```

Expected: exit code `0`, three Python 3.12 interpreters reported with repository-relative paths, correct gitlinks, real Windows symlinks, clean dependency checks, Ruff pass, and both rendered-query tests pass.

- [ ] **Step 5: Run the complete repository verification**

Run:

```powershell
uv run pytest -v
uv run ruff check src tests scripts
git submodule status
git diff --check
```

Expected: all root and bootstrap tests pass; Ruff and whitespace checks pass; every submodule status line begins with a space.

- [ ] **Step 6: Confirm only intended files changed**

Run:

```powershell
git status --short
git diff --stat HEAD
```

Expected files for this implementation are `.python-version`, `bootstrap.ps1`, `bootstrap.sh`, `scripts/bootstrap.py`, `scripts/tests/test_bootstrap.py`, `pyproject.toml`, and `README.md`. Preserve the pre-existing unrelated working-tree changes and do not stage `vendor/` contents or local `.idea` files.

- [ ] **Step 7: Commit Task 5**

```powershell
git add README.md
git commit -m "docs: explain one-command environment setup"
```
