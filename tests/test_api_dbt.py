from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from dbt_metricflow_service.api import create_app
from dbt_metricflow_service.jobs import ProjectBusyError
from dbt_metricflow_service.models import CommandSpec, JobRecord, JobStatus
from dbt_metricflow_service.projects import ProjectRegistry
from dbt_metricflow_service.settings import Settings

logger = logging.getLogger(__name__)

JOB_ID = UUID("00000000-0000-0000-0000-000000000002")


class StubJobRunner:
    """Record command submissions without starting dbt in HTTP boundary tests."""

    def __init__(self, *, busy: bool = False) -> None:
        self.busy = busy
        self.submissions: list[tuple[str, CommandSpec]] = []
        self.records: dict[UUID, JobRecord] = {}

    async def submit(self, project: str, command: CommandSpec) -> JobRecord:
        if self.busy:
            raise ProjectBusyError(project)
        self.submissions.append((project, command))
        record = JobRecord(
            id=JOB_ID,
            project=project,
            status=JobStatus.QUEUED,
            submitted_at=datetime.now(UTC),
        )
        self.records[record.id] = record
        return record

    async def get(self, job_id: UUID) -> JobRecord | None:
        return self.records.get(job_id)


@pytest.fixture
def api_dependencies(tmp_path: Path) -> tuple[Settings, ProjectRegistry, StubJobRunner]:
    """Create one real dbt project and an isolated profiles directory."""
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
    return settings, ProjectRegistry(projects_root), runner


@pytest.fixture
def client(
    api_dependencies: tuple[Settings, ProjectRegistry, StubJobRunner],
) -> TestClient:
    """Expose the dbt API with a recording runner."""
    settings, registry, runner = api_dependencies
    return TestClient(create_app(settings, registry, runner))


def test_submit_dbt_job_returns_202(
    client: TestClient,
    api_dependencies: tuple[Settings, ProjectRegistry, StubJobRunner],
) -> None:
    """A valid request must enqueue literal dbt argv without waiting."""
    response = client.post(
        "/v1/dbt/jobs",
        json={"project": "sales", "command": "build", "target": "prod"},
    )

    assert response.status_code == 202
    assert response.json()["status"] == "queued"
    _, _, runner = api_dependencies
    project, command = runner.submissions[0]
    assert project == "sales"
    assert command.argv[:2] == ("dbt", "build")
    assert command.argv[-2:] == ("--target", "prod")


def test_job_status_returns_record(
    client: TestClient,
) -> None:
    """A retained job must be available from the shared status route."""
    submitted = client.post("/v1/dbt/jobs", json={"project": "sales", "command": "parse"})

    response = client.get(f"/v1/jobs/{submitted.json()['id']}")

    assert response.status_code == 200
    assert response.json()["status"] == "queued"


def test_unknown_job_after_restart_returns_404(client: TestClient) -> None:
    """An in-memory identifier absent from this process must return a stable error."""
    response = client.get("/v1/jobs/00000000-0000-0000-0000-000000000001")

    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "job_not_found"


def test_invalid_project_returns_stable_422(client: TestClient) -> None:
    """Unsafe project identifiers must fail before command construction."""
    response = client.post(
        "/v1/dbt/jobs",
        json={"project": "../outside", "command": "build"},
    )

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "invalid_project"


def test_project_busy_returns_409(
    api_dependencies: tuple[Settings, ProjectRegistry, StubJobRunner],
) -> None:
    """A second project mutation must map to a retryable conflict response."""
    settings, registry, runner = api_dependencies
    runner.busy = True
    client = TestClient(create_app(settings, registry, runner))

    response = client.post("/v1/dbt/jobs", json={"project": "sales", "command": "run"})

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "project_busy"


def test_arbitrary_dbt_command_returns_stable_422(client: TestClient) -> None:
    """Pydantic validation must keep raw CLI commands outside the API allowlist."""
    response = client.post(
        "/v1/dbt/jobs",
        json={"project": "sales", "command": "run-operation"},
    )

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "validation_error"


def test_nonblank_resources_submit_fixed_worker_without_echoing_yaml(
    client: TestClient,
    api_dependencies: tuple[Settings, ProjectRegistry, StubJobRunner],
) -> None:
    marker = "API_RESOURCE_MARKER_91df"
    response = client.post(
        "/v1/dbt/jobs",
        json={
            "project": "sales",
            "command": "parse",
            "resources": {"orders.yml": f"version: 2\n# {marker}\n"},
        },
    )
    assert response.status_code == 202
    assert marker not in response.text
    command = api_dependencies[2].submissions[0][1]
    assert command.argv[1:] == ("-m", "dbt_metricflow_service.resource_worker")
    assert command.stdin_data is not None


def test_debug_rejects_nonblank_resources(client: TestClient) -> None:
    response = client.post(
        "/v1/dbt/jobs",
        json={"project": "sales", "command": "debug", "resources": {"a.yml": "{}"}},
    )
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "validation_error"
