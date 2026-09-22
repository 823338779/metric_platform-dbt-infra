from __future__ import annotations

import argparse
import logging
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
SUBMODULE_PATHS = (
    Path("vendor/dbt"),
    Path("vendor/metricflow"),
    Path("vendor/dbt-metricflow"),
)
METRICFLOW_SUBMODULE_PATHS = (
    Path("vendor/metricflow"),
    Path("vendor/dbt-metricflow"),
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
    )
    fields = tree.stdout.split()
    if len(fields) < 3 or fields[0] != "160000":
        raise BootstrapError(f"Missing gitlink for {relative_path.as_posix()}")
    actual = run_checked(
        runner,
        ("git", "-C", str(context.root / relative_path), "rev-parse", "HEAD"),
        cwd=context.root,
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
        logger.info("Repository: %s", context.root)
        return 0
    except BootstrapError as error:
        logger.error("%s", error)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
