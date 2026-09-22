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
