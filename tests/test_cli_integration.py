from __future__ import annotations

import logging
import os
import shutil
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)

FIXTURES_DIR = Path(__file__).parent / "fixtures"
DBT_PROJECT_FIXTURE = FIXTURES_DIR / "dbt_project"
PROFILES_FIXTURE = FIXTURES_DIR / "profiles"
DBT_TIMEOUT_SECONDS = 60


def dbt_environment() -> dict[str, str]:
    """Disable dbt telemetry while retaining the uv-managed executable path."""
    environment = dict(os.environ)
    environment["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
    environment["PYTHONUTF8"] = "1"
    return environment


def test_dbt_reports_pinned_starrocks_plugin() -> None:
    """The installed dbt command must discover the exact StarRocks adapter pin."""
    result = subprocess.run(
        ["dbt", "--version"],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=dbt_environment(),
        timeout=DBT_TIMEOUT_SECONDS,
    )

    assert "1.12.5" in result.stdout
    assert "starrocks" in result.stdout.lower()
    assert "1.12.2" in result.stdout


def test_dbt_parse_generates_semantic_artifacts_without_database(
    tmp_path: Path,
) -> None:
    """A minimal StarRocks project must parse without opening a database connection."""
    project_dir = tmp_path / "project"
    profiles_dir = tmp_path / "profiles"
    shutil.copytree(DBT_PROJECT_FIXTURE, project_dir)
    shutil.copytree(PROFILES_FIXTURE, profiles_dir)

    subprocess.run(
        [
            "dbt",
            "parse",
            "--project-dir",
            str(project_dir),
            "--profiles-dir",
            str(profiles_dir),
            "--no-partial-parse",
        ],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=dbt_environment(),
        timeout=DBT_TIMEOUT_SECONDS,
    )

    assert (project_dir / "target" / "manifest.json").is_file()
    assert (project_dir / "target" / "semantic_manifest.json").is_file()
