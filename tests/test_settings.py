from __future__ import annotations

import logging
from pathlib import Path

from dbt_metricflow_service.settings import Settings

logger = logging.getLogger(__name__)


def test_settings_reads_paths_and_limits(monkeypatch, tmp_path: Path) -> None:
    """Environment overrides must define resolved paths and numeric limits."""
    projects = tmp_path / "projects"
    profiles = tmp_path / "profiles"
    projects.mkdir()
    profiles.mkdir()
    artifacts = tmp_path / "artifacts"
    monkeypatch.setenv("PROJECTS_ROOT", str(projects))
    monkeypatch.setenv("DBT_PROFILES_DIR", str(profiles))
    monkeypatch.setenv("COMMAND_TIMEOUT_SECONDS", "45")
    monkeypatch.setenv("MAX_OUTPUT_BYTES", "4096")
    monkeypatch.setenv("JOB_ARTIFACTS_ROOT", str(artifacts))

    settings = Settings.from_environment()

    assert settings.projects_root == projects.resolve()
    assert settings.profiles_dir == profiles.resolve()
    assert settings.command_timeout_seconds == 45
    assert settings.max_output_bytes == 4096
    assert settings.job_artifacts_root == artifacts.resolve()


def test_settings_defaults_to_local_directories(monkeypatch, tmp_path: Path) -> None:
    """A local checkout should use directories under its working directory by default."""
    monkeypatch.chdir(tmp_path)
    for name in ("PROJECTS_ROOT", "DBT_PROFILES_DIR", "JOB_ARTIFACTS_ROOT"):
        monkeypatch.delenv(name, raising=False)

    settings = Settings.from_environment()

    assert settings.projects_root == tmp_path / "projects"
    assert settings.profiles_dir == tmp_path / "profiles"
    assert settings.job_artifacts_root == tmp_path / "job-artifacts"
