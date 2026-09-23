from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from dbt_metricflow_service.api import create_app
from dbt_metricflow_service.models import CommandSpec, JobRecord, JobStatus
from dbt_metricflow_service.projects import ProjectRegistry
from dbt_metricflow_service.settings import Settings

logger = logging.getLogger(__name__)

JOB_ID = UUID("00000000-0000-0000-0000-000000000003")


class StubJobRunner:
    """Record MetricFlow submissions without executing the installed CLI."""

    def __init__(self) -> None:
        self.submissions: list[tuple[str, CommandSpec]] = []

    async def submit(self, project: str, command: CommandSpec) -> JobRecord:
        self.submissions.append((project, command))
        return JobRecord(
            id=JOB_ID,
            project=project,
            status=JobStatus.QUEUED,
            submitted_at=datetime.now(UTC),
        )

    async def get(self, job_id: UUID) -> None:
        return None


def write_manifest(project_dir: Path, adapter_type: str) -> None:
    """Write only the dbt manifest metadata consumed by the API boundary."""
    target_dir = project_dir / "target"
    target_dir.mkdir(exist_ok=True)
    (target_dir / "manifest.json").write_text(
        json.dumps({"metadata": {"adapter_type": adapter_type}}),
        encoding="utf-8",
    )


@pytest.fixture
def metricflow_api(
    tmp_path: Path,
) -> tuple[TestClient, Path, StubJobRunner]:
    """Create a real project registry and recording runner for adapter tests."""
    projects_root = tmp_path / "projects"
    project_dir = projects_root / "sales"
    profiles_dir = tmp_path / "profiles"
    project_dir.mkdir(parents=True)
    profiles_dir.mkdir()
    (project_dir / "dbt_project.yml").write_text("name: sales\n", encoding="utf-8")
    settings = Settings(
        projects_root=projects_root,
        profiles_dir=profiles_dir,
        command_timeout_seconds=30,
        max_output_bytes=1024,
    )
    runner = StubJobRunner()
    app = create_app(settings, ProjectRegistry(projects_root), runner)
    return TestClient(app), project_dir, runner


def test_starrocks_metricflow_job_is_rejected(
    metricflow_api: tuple[TestClient, Path, StubJobRunner],
) -> None:
    """MetricFlow 0.213.0 must not attempt SQL generation for StarRocks."""
    client, project_dir, runner = metricflow_api
    write_manifest(project_dir, adapter_type="starrocks")

    response = client.post(
        "/v1/metricflow/jobs",
        json={"project": "sales", "command": "query", "metrics": ["revenue"]},
    )

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "metricflow_adapter_not_supported"
    assert "starrocks" in response.json()["detail"]["message"]
    assert runner.submissions == []


def test_postgres_metricflow_explain_is_submitted(
    metricflow_api: tuple[TestClient, Path, StubJobRunner],
) -> None:
    """A supported adapter must enqueue a literal mf explain command."""
    client, project_dir, runner = metricflow_api
    write_manifest(project_dir, adapter_type="postgres")

    response = client.post(
        "/v1/metricflow/jobs",
        json={"project": "sales", "command": "explain", "metrics": ["revenue"]},
    )

    assert response.status_code == 202
    assert response.json()["status"] == "queued"
    project, command = runner.submissions[0]
    assert project == "sales"
    assert command.argv[:4] == ("mf", "query", "--explain", "--quiet")
    assert command.write_operation is False


def test_missing_manifest_returns_semantic_manifest_error(
    metricflow_api: tuple[TestClient, Path, StubJobRunner],
) -> None:
    """MetricFlow requests require dbt to generate target/manifest.json first."""
    client, _, runner = metricflow_api

    response = client.post(
        "/v1/metricflow/jobs",
        json={"project": "sales", "command": "list_metrics"},
    )

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "semantic_manifest_not_generated"
    assert runner.submissions == []


def test_invalid_manifest_returns_stable_error(
    metricflow_api: tuple[TestClient, Path, StubJobRunner],
) -> None:
    """Unreadable manifest metadata must not reach MetricFlow command creation."""
    client, project_dir, runner = metricflow_api
    target_dir = project_dir / "target"
    target_dir.mkdir()
    (target_dir / "manifest.json").write_text("{not-json", encoding="utf-8")

    response = client.post(
        "/v1/metricflow/jobs",
        json={"project": "sales", "command": "list_metrics"},
    )

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "invalid_dbt_manifest"
    assert runner.submissions == []


def test_unknown_adapter_is_rejected(
    metricflow_api: tuple[TestClient, Path, StubJobRunner],
) -> None:
    """Adapters outside the upstream allowlist must fail with their actual name."""
    client, project_dir, runner = metricflow_api
    write_manifest(project_dir, adapter_type="custom_adapter")

    response = client.post(
        "/v1/metricflow/jobs",
        json={"project": "sales", "command": "list_metrics"},
    )

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "metricflow_adapter_not_supported"
    assert "custom_adapter" in response.json()["detail"]["message"]
    assert runner.submissions == []


@pytest.mark.parametrize("command", ["list_dimensions", "explain", "query"])
def test_metricflow_command_without_metrics_returns_stable_422(
    metricflow_api: tuple[TestClient, Path, StubJobRunner], command: str
) -> None:
    """Command-specific input errors must remain structured client failures."""
    client, project_dir, runner = metricflow_api
    write_manifest(project_dir, adapter_type="postgres")

    response = client.post(
        "/v1/metricflow/jobs",
        json={"project": "sales", "command": command},
    )

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "validation_error"
    assert runner.submissions == []


def test_resources_skip_stale_manifest_precheck(
    metricflow_api: tuple[TestClient, Path, StubJobRunner],
) -> None:
    client, _, runner = metricflow_api
    response = client.post(
        "/v1/metricflow/jobs",
        json={
            "project": "sales",
            "command": "list_metrics",
            "resources": {"metrics.yml": "version: 2\n"},
        },
    )
    assert response.status_code == 202
    assert runner.submissions[0][1].use_job_artifacts is True


def test_blank_resources_keep_manifest_precheck(
    metricflow_api: tuple[TestClient, Path, StubJobRunner],
) -> None:
    client, _, runner = metricflow_api
    response = client.post(
        "/v1/metricflow/jobs",
        json={
            "project": "sales",
            "command": "list_metrics",
            "resources": {"metrics.yml": " \n"},
        },
    )
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "semantic_manifest_not_generated"
    assert runner.submissions == []
