from __future__ import annotations

import logging
from importlib.metadata import version
from pathlib import Path

from fastapi.testclient import TestClient

from dbt_metricflow_service import __version__
from dbt_metricflow_service.api import create_app
from dbt_metricflow_service.projects import ProjectRegistry
from dbt_metricflow_service.settings import Settings

logger = logging.getLogger(__name__)

VERSION_DISTRIBUTIONS = (
    "dbt-core",
    "dbt-starrocks",
    "dbt-metricflow",
    "metricflow",
)


class EmptyJobRunner:
    """Supply the smallest runner surface needed by health routes."""

    async def get(self, job_id: object) -> None:
        return None


def build_client(tmp_path: Path) -> TestClient:
    """Create an application with concrete readable mount directories."""
    projects_root = tmp_path / "projects"
    profiles_dir = tmp_path / "profiles"
    projects_root.mkdir()
    profiles_dir.mkdir()
    settings = Settings(
        projects_root=projects_root,
        profiles_dir=profiles_dir,
        command_timeout_seconds=30,
        max_output_bytes=1024,
    )
    return TestClient(create_app(settings, ProjectRegistry(projects_root), EmptyJobRunner()))


def test_live_does_not_depend_on_cli(tmp_path: Path, monkeypatch: object) -> None:
    """Liveness must only prove that the HTTP process can answer."""
    client = build_client(tmp_path)
    monkeypatch.setattr("dbt_metricflow_service.api.shutil.which", lambda _: None)  # type: ignore[attr-defined]

    response = client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ready_returns_503_when_cli_is_missing(tmp_path: Path, monkeypatch: object) -> None:
    """Readiness must reject traffic when either packaged CLI is unavailable."""
    client = build_client(tmp_path)
    monkeypatch.setattr("dbt_metricflow_service.api.shutil.which", lambda _: None)  # type: ignore[attr-defined]

    response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "not_ready"


def test_ready_accepts_installed_clis_and_readable_mounts(
    tmp_path: Path, monkeypatch: object
) -> None:
    """Readiness must pass when runtime commands and mounted directories exist."""
    client = build_client(tmp_path)
    monkeypatch.setattr("dbt_metricflow_service.api.shutil.which", lambda name: f"/bin/{name}")  # type: ignore[attr-defined]

    response = client.get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ready"}


def test_versions_returns_service_and_pinned_package_versions(tmp_path: Path) -> None:
    """Version inspection must report the running distributions, not constants."""
    response = build_client(tmp_path).get("/v1/versions")

    assert response.status_code == 200
    assert response.json() == {
        "service": __version__,
        **{distribution: version(distribution) for distribution in VERSION_DISTRIBUTIONS},
    }
