from __future__ import annotations

import argparse
import logging
import os
import re
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

logger = logging.getLogger(__name__)

PYTHON_VERSION = "3.12"
HATCH_VERSION = "1.18.1"
MINIMUM_UV_VERSION = (0, 12, 0)
PlatformName = Literal["windows", "macos"]
SUBMODULE_PATHS = (
    Path("vendor/dbt"),
    Path("vendor/metricflow"),
    Path("vendor/dbt-metricflow"),
)
METRICFLOW_SUBMODULE_PATHS = (
    Path("vendor/metricflow"),
    Path("vendor/dbt-metricflow"),
)
DBT_METRICFLOW_EDITABLE_MISMATCH = (
    "The package `dbt-metricflow` requires `metricflow==0.213.0`, "
    "but `0.214.0.dev0` is installed"
)
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
    direct_url_text = metadata.distribution(name).read_text("direct_url.json")
    if direct_url_text is None:
        raise SystemExit(f"{name} has no direct_url.json")
    direct_url = json.loads(direct_url_text)
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
ENVIRONMENT_IDENTITY_CHECK = (
    "import sys; from pathlib import Path; "
    "print(sys.implementation.name); "
    "print(f'{sys.version_info.major}.{sys.version_info.minor}'); "
    "print(Path(sys.base_prefix).resolve())"
)


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
        """Run one command and return its captured result."""
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
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify without changing the computer or checkout.",
    )
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


def subprocess_runner(
    args: Sequence[str],
    *,
    cwd: Path,
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            list(args),
            cwd=cwd,
            env=dict(env) if env is not None else None,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            shell=False,
            check=False,
        )
    except OSError as error:
        raise BootstrapError(f"Unable to start command {args[0]}: {error}") from None


def verification_environment(context: BootstrapContext) -> dict[str, str]:
    """Return an inherited environment with read-only check safeguards."""
    environment = dict(os.environ)
    if context.check_only:
        environment.update(
            {
                "GIT_OPTIONAL_LOCKS": "0",
                "PYTHONDONTWRITEBYTECODE": "1",
                "UV_NO_CACHE": "1",
            }
        )
    return environment


def run_checked(
    runner: CommandRunner,
    args: Sequence[str],
    *,
    cwd: Path,
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    try:
        result = runner(args, cwd=cwd, env=env)
    except OSError as error:
        raise BootstrapError(f"Unable to start command {args[0]}: {error}") from None
    if result.returncode != 0:
        command = repr(tuple(args))
        raise BootstrapError(
            f"Command failed with exit code {result.returncode}: {command}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def submodule_is_clean(
    context: BootstrapContext,
    runner: CommandRunner,
    relative_path: Path,
) -> bool:
    """Return whether an initialized submodule has no local changes."""
    result = run_checked(
        runner,
        (
            "git",
            "-C",
            str(context.root / relative_path),
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
        ),
        cwd=context.root,
        env=verification_environment(context),
    )
    return not result.stdout.strip()


def verify_submodule_commit(
    context: BootstrapContext,
    runner: CommandRunner,
    relative_path: Path,
) -> None:
    """Require a submodule HEAD to equal its parent gitlink."""
    tree = run_checked(
        runner,
        ("git", "ls-tree", "HEAD", relative_path.as_posix()),
        cwd=context.root,
        env=verification_environment(context),
    )
    fields = tree.stdout.split()
    if len(fields) < 3 or fields[0] != "160000":
        raise BootstrapError(f"Missing gitlink for {relative_path.as_posix()}")
    actual = run_checked(
        runner,
        ("git", "-C", str(context.root / relative_path), "rev-parse", "HEAD"),
        cwd=context.root,
        env=verification_environment(context),
    ).stdout.strip()
    if actual != fields[2]:
        raise BootstrapError(
            f"Submodule does not match parent gitlink: {relative_path.as_posix()}"
        )


def tracked_symlinks(
    context: BootstrapContext,
    runner: CommandRunner,
    relative_path: Path,
) -> tuple[Path, ...]:
    """Return safe tracked mode-120000 paths from an initialized submodule."""
    result = run_checked(
        runner,
        (
            "git",
            "-C",
            str(context.root / relative_path),
            "ls-files",
            "-s",
            "-z",
        ),
        cwd=context.root,
        env=verification_environment(context),
    )
    links: list[Path] = []
    for record in result.stdout.split("\0"):
        if not record:
            continue
        metadata, filename = record.split("\t", maxsplit=1)
        if metadata.split(maxsplit=1)[0] != "120000":
            continue
        link_path = Path(filename)
        if link_path.is_absolute() or ".." in link_path.parts:
            raise BootstrapError(f"Unsafe tracked symlink path: {filename}")
        links.append(link_path)
    return tuple(links)


def verify_windows_symlinks(
    context: BootstrapContext,
    runner: CommandRunner,
) -> None:
    """Require tracked MetricFlow Git symlinks to be real filesystem links."""
    for relative_path in METRICFLOW_SUBMODULE_PATHS:
        for link_path in tracked_symlinks(context, runner, relative_path):
            if not (context.root / relative_path / link_path).is_symlink():
                raise BootstrapError(
                    "Git symlink was checked out as a regular file: "
                    f"{relative_path / link_path}"
                )


def _require_clean_initialized_submodules(
    context: BootstrapContext,
    runner: CommandRunner,
) -> tuple[Path, ...]:
    initialized = tuple(
        path for path in SUBMODULE_PATHS if (context.root / path / ".git").exists()
    )
    for relative_path in initialized:
        if not submodule_is_clean(context, runner, relative_path):
            raise BootstrapError(
                f"Submodule contains local changes: {relative_path.as_posix()}"
            )
    return initialized


def _repair_windows_symlinks(
    context: BootstrapContext,
    runner: CommandRunner,
) -> None:
    for relative_path in METRICFLOW_SUBMODULE_PATHS:
        run_checked(
            runner,
            (
                "git",
                "-C",
                str(context.root / relative_path),
                "config",
                "core.symlinks",
                "true",
            ),
            cwd=context.root,
        )
        for link_path in tracked_symlinks(context, runner, relative_path):
            if (context.root / relative_path / link_path).is_symlink():
                continue
            run_checked(
                runner,
                (
                    "git",
                    "-C",
                    str(context.root / relative_path),
                    "restore",
                    "--worktree",
                    "--",
                    link_path.as_posix(),
                ),
                cwd=context.root,
            )


def prepare_submodules(context: BootstrapContext, runner: CommandRunner) -> None:
    """Initialize or inspect pinned submodules without overwriting user work."""
    initialized = _require_clean_initialized_submodules(context, runner)
    if context.check_only:
        if initialized != SUBMODULE_PATHS:
            raise BootstrapError(
                "Submodules are not initialized; run bootstrap without --check"
            )
    else:
        if context.platform == "windows":
            run_checked(
                runner,
                ("git", "config", "core.symlinks", "true"),
                cwd=context.root,
            )
        run_checked(
            runner,
            ("git", "submodule", "update", "--init", "--recursive"),
            cwd=context.root,
        )
        if context.platform == "windows":
            _repair_windows_symlinks(context, runner)
    for relative_path in SUBMODULE_PATHS:
        verify_submodule_commit(context, runner, relative_path)
    if context.platform == "windows":
        verify_windows_symlinks(context, runner)


def find_uv_python(context: BootstrapContext, runner: CommandRunner) -> Path:
    """Return the uv-managed CPython 3.12 executable."""
    result = run_checked(
        runner,
        (
            "uv",
            "--no-python-downloads",
            "python",
            "find",
            "--system",
            "--managed-python",
            PYTHON_VERSION,
        ),
        cwd=context.root,
        env=verification_environment(context),
    )
    python = Path(result.stdout.strip()).resolve()
    if not python.is_file():
        raise BootstrapError(f"uv returned a missing Python interpreter: {python}")
    return python


def validate_uv_version(context: BootstrapContext, runner: CommandRunner) -> None:
    """Require the supported uv CLI before running shared orchestration."""
    result = run_checked(
        runner,
        ("uv", "--version"),
        cwd=context.root,
        env=verification_environment(context),
    )
    match = re.match(r"^uv (\d+)\.(\d+)\.(\d+)", result.stdout.strip())
    if match is None or tuple(map(int, match.groups())) < MINIMUM_UV_VERSION:
        raise BootstrapError("uv 0.12 or newer is required; rerun the platform bootstrap")


def _environment_recovery(context: BootstrapContext, environment: Path) -> str:
    relative = environment.relative_to(context.root)
    if context.platform == "windows":
        path = str(relative)
        return f"Rename-Item '{path}' '{path}.backup'; .\\bootstrap.ps1"
    path = relative.as_posix()
    return f"mv '{path}' '{path}.backup' && ./bootstrap.sh"


def require_environment_version(
    context: BootstrapContext,
    runner: CommandRunner,
    environment: Path,
    managed_python: Path | None = None,
) -> None:
    """Require an existing environment to use uv-managed CPython 3.12."""
    if not environment.exists():
        return
    python = environment_python(environment, context.platform)
    if not python.is_file():
        raise BootstrapError(
            f"Existing environment has no Python interpreter: {environment}"
        )
    result = run_checked(
        runner,
        (
            str(python),
            "-c",
            ENVIRONMENT_IDENTITY_CHECK,
        ),
        cwd=context.root,
        env=verification_environment(context),
    )
    identity = result.stdout.splitlines()
    if len(identity) != 3 or identity[0] != "cpython" or identity[1] != PYTHON_VERSION:
        raise BootstrapError(
            f"Existing environment must use uv-managed CPython 3.12: {environment}. "
            f"Recover with: {_environment_recovery(context, environment)}"
        )
    if (
        managed_python is not None
        and Path(identity[2]).resolve() != managed_python.resolve().parent
    ):
        raise BootstrapError(
            f"Existing environment does not use the selected uv-managed Python: "
            f"{environment}. Recover with: {_environment_recovery(context, environment)}"
        )


def create_upstream_environment(
    context: BootstrapContext,
    runner: CommandRunner,
    *,
    project: Path,
    environment: Path,
    python: Path,
    editable_overrides: tuple[Path, ...] = (),
) -> None:
    """Synchronize one upstream Hatch dev feature into a local environment."""
    if not environment.exists():
        run_checked(
            runner,
            ("uv", "venv", "--python", str(python), str(environment)),
            cwd=project,
        )
    environment_interpreter = environment_python(environment, context.platform)
    run_checked(
        runner,
        (
            "uv",
            "pip",
            "install",
            "--exact",
            "--python",
            str(environment_interpreter),
            "--editable",
            f"{project}[dev-env-requirements]",
        ),
        cwd=project,
    )
    for editable in editable_overrides:
        run_checked(
            runner,
            (
                "uv",
                "pip",
                "install",
                "--no-deps",
                "--python",
                str(environment_interpreter),
                "--editable",
                str(editable),
            ),
            cwd=project,
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
    run_checked(
        runner,
        ("uv", "python", "install", "--upgrade", PYTHON_VERSION),
        cwd=context.root,
    )
    python = find_uv_python(context, runner)
    for environment in (
        context.root_environment,
        context.metricflow_environment,
        context.dbt_metricflow_environment,
    ):
        require_environment_version(context, runner, environment, python)
    run_checked(
        runner,
        ("uv", "sync", "--frozen", "--all-groups", "--python", str(python)),
        cwd=context.root,
    )
    create_upstream_environment(
        context,
        runner,
        project=context.root / "vendor" / "metricflow",
        environment=context.metricflow_environment,
        python=python,
    )
    dbt_metricflow_project = (
        context.root / "vendor" / "dbt-metricflow" / "dbt-metricflow"
    )
    create_upstream_environment(
        context,
        runner,
        project=dbt_metricflow_project,
        environment=context.dbt_metricflow_environment,
        python=python,
        editable_overrides=(dbt_metricflow_project.parent,),
    )


def verify_pip_check(
    context: BootstrapContext,
    runner: CommandRunner,
    python: Path,
    *,
    allowed_lines: frozenset[str] = frozenset(),
) -> None:
    """Accept a clean dependency graph or explicitly allow-listed diagnostics."""
    result = runner(
        ("uv", "pip", "check", "--python", str(python)),
        cwd=context.root,
        env=verification_environment(context),
    )
    lines = frozenset(
        line.strip()
        for line in (result.stdout + result.stderr).splitlines()
        if line.strip()
    )
    if result.returncode == 0:
        return
    diagnostics = frozenset(
        line
        for line in lines
        if not line.startswith(("Using Python ", "Checked "))
    )
    expected_summary = (
        f"Found {len(allowed_lines)} incompatibility"
        if len(allowed_lines) == 1
        else f"Found {len(allowed_lines)} incompatibilities"
    )
    if allowed_lines and diagnostics == allowed_lines | {expected_summary}:
        return
    raise BootstrapError(
        "Dependency integrity check failed: " + "; ".join(sorted(lines))
    )


def _required_environment_pythons(
    context: BootstrapContext,
    runner: CommandRunner,
    managed_python: Path,
) -> tuple[Path, Path, Path]:
    environments = (
        context.root_environment,
        context.metricflow_environment,
        context.dbt_metricflow_environment,
    )
    for environment in environments:
        require_environment_version(context, runner, environment, managed_python)
    pythons = tuple(
        environment_python(environment, context.platform) for environment in environments
    )
    missing = [python for python in pythons if not python.is_file()]
    if missing:
        raise BootstrapError(f"Required environment is missing: {missing[0]}")
    return pythons


def verify_environments(context: BootstrapContext, runner: CommandRunner) -> None:
    """Validate versions, imports, local sources, lint, and representative tests."""
    managed_python = find_uv_python(context, runner)
    root_python, metricflow_python, dbt_metricflow_python = (
        _required_environment_pythons(context, runner, managed_python)
    )
    environment = verification_environment(context)
    run_checked(
        runner,
        (str(root_python), "-c", ROOT_IMPORT_AND_SOURCE_CHECK),
        cwd=context.root,
        env=environment,
    )
    run_checked(
        runner,
        (str(metricflow_python), "-c", METRICFLOW_IMPORT_CHECK),
        cwd=context.root,
        env=environment,
    )
    run_checked(
        runner,
        (str(dbt_metricflow_python), "-c", DBT_METRICFLOW_IMPORT_CHECK),
        cwd=context.root,
        env=environment,
    )
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
        (
            str(root_python),
            "-m",
            "pytest",
            "tests/test_dependencies.py",
            "-v",
            "-p",
            "no:cacheprovider",
        ),
        cwd=context.root,
        env=environment,
    )
    run_checked(
        runner,
        (
            str(root_python),
            "-m",
            "ruff",
            "check",
            "--no-cache",
            "src",
            "tests",
            "scripts",
        ),
        cwd=context.root,
        env=environment,
    )
    rendered_query = (
        "tests_metricflow/integration/test_rendered_query.py::test_render_query"
    )
    run_checked(
        runner,
        (
            str(metricflow_python),
            "-m",
            "pytest",
            rendered_query,
            "-v",
            "-p",
            "no:cacheprovider",
        ),
        cwd=context.root / "vendor" / "metricflow",
        env=environment,
    )
    run_checked(
        runner,
        (
            str(dbt_metricflow_python),
            "-m",
            "pytest",
            rendered_query,
            "-v",
            "-p",
            "no:cacheprovider",
        ),
        cwd=context.root / "vendor" / "dbt-metricflow",
        env=environment,
    )


def print_interpreter_summary(context: BootstrapContext) -> None:
    """Print stable interpreter paths relative to the checkout."""
    print("Development environments are ready:")
    for label, environment in (
        ("service", context.root_environment),
        ("metricflow", context.metricflow_environment),
        ("dbt-metricflow", context.dbt_metricflow_environment),
    ):
        python = environment_python(environment, context.platform)
        print(f"  {label}: {python.relative_to(context.root)}")


def current_platform(platform: str) -> PlatformName:
    if platform == "win32":
        return "windows"
    if platform == "darwin":
        return "macos"
    raise BootstrapError(f"Unsupported platform: {platform}")


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    try:
        options = parse_args(sys.argv[1:] if argv is None else argv)
        context = BootstrapContext(
            root=resolve_repository_root(Path(__file__)),
            platform=current_platform(sys.platform),
            check_only=options.check,
        )
        validate_uv_version(context, subprocess_runner)
        prepare_submodules(context, subprocess_runner)
        sync_environments(context, subprocess_runner)
        verify_environments(context, subprocess_runner)
        print_interpreter_summary(context)
        return 0
    except BootstrapError as error:
        logger.error("%s", error)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
