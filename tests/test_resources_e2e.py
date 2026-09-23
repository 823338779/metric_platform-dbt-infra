from __future__ import annotations

import json
import logging
import shutil
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from dbt_metricflow_service.api import create_app
from dbt_metricflow_service.jobs import JobRunner
from dbt_metricflow_service.projects import ProjectRegistry
from dbt_metricflow_service.settings import Settings

logger = logging.getLogger(__name__)
PROJECT_FIXTURE = Path(__file__).parent / "fixtures" / "dbt_project"
FINAL_STATUSES = frozenset({"succeeded", "failed", "timed_out"})


def _build_client(tmp_path: Path) -> tuple[TestClient, Path, Path]:
    projects_root = tmp_path / "projects"
    project_dir = projects_root / "sales"
    profiles_dir = tmp_path / "profiles"
    artifacts_root = tmp_path / "artifacts"
    shutil.copytree(PROJECT_FIXTURE, project_dir)
    profiles_dir.mkdir()
    database_path = json.dumps(str(tmp_path / "warehouse.duckdb"))
    (profiles_dir / "profiles.yml").write_text(
        "wrapper_fixture:\n  target: test\n  outputs:\n    test:\n"
        f"      type: duckdb\n      path: {database_path}\n      schema: main\n",
        encoding="utf-8",
    )
    settings = Settings(
        projects_root=projects_root,
        profiles_dir=profiles_dir,
        command_timeout_seconds=60,
        max_output_bytes=128 * 1024,
        job_artifacts_root=artifacts_root,
    )
    runner = JobRunner(
        settings.command_timeout_seconds,
        settings.max_output_bytes,
        job_artifacts_root=artifacts_root,
    )
    return TestClient(create_app(settings, ProjectRegistry(projects_root), runner)), project_dir, artifacts_root


def _wait(client: TestClient, job_id: str) -> dict[str, object]:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        record = client.get(f"/v1/jobs/{job_id}").json()
        if record["status"] in FINAL_STATUSES:
            return record
        time.sleep(0.05)
    pytest.fail("resource job did not finish")


def _submit(client: TestClient, path: str, payload: dict[str, object]) -> dict[str, object]:
    response = client.post(path, json=payload)
    assert response.status_code == 202, response.text
    return _wait(client, response.json()["id"])


@pytest.mark.parametrize("command", ["parse", "compile", "seed", "run", "test", "build"])
def test_dbt_commands_consume_request_yaml_without_changing_disk(
    tmp_path: Path,
    command: str,
) -> None:
    client, project_dir, artifacts_root = _build_client(tmp_path)
    source = project_dir / "models" / "orders.yml"
    original = source.read_bytes()
    raw = original.decode("utf-8") + "\n# E2E_MEMORY_ONLY_MARKER_a1c3\n"
    with client:
        record = _submit(
            client,
            "/v1/dbt/jobs",
            {"project": "sales", "command": command, "resources": {"orders.yml": raw}},
        )
    assert record["status"] == "succeeded", record
    assert source.read_bytes() == original
    assert not list(artifacts_root.iterdir())


def test_metricflow_uses_request_definition_and_concurrent_jobs_are_isolated(tmp_path: Path) -> None:
    client, project_dir, artifacts_root = _build_client(tmp_path)
    raw = (project_dir / "models" / "orders.yml").read_text(encoding="utf-8")
    first = raw.replace(
        "    metrics:\n      - name: revenue",
        "    metrics:\n      - name: first_revenue",
    )
    second = raw.replace(
        "    metrics:\n      - name: revenue",
        "    metrics:\n      - name: second_revenue",
    )
    assert first != raw and second != raw
    with client:
        first_response = client.post(
            "/v1/metricflow/jobs",
            json={"project": "sales", "command": "list_metrics", "resources": {"orders.yml": first}},
        )
        second_response = client.post(
            "/v1/metricflow/jobs",
            json={"project": "sales", "command": "list_metrics", "resources": {"orders.yml": second}},
        )
        assert first_response.status_code == second_response.status_code == 202
        first_record = _wait(client, first_response.json()["id"])
        second_record = _wait(client, second_response.json()["id"])
    assert first_record["status"] == second_record["status"] == "succeeded"
    assert "first_revenue" in str(first_record["stdout"])
    assert "second_revenue" not in str(first_record["stdout"])
    assert "second_revenue" in str(second_record["stdout"])
    assert "first_revenue" not in str(second_record["stdout"])
    assert not list(artifacts_root.iterdir())


def test_invalid_request_yaml_fails_without_falling_back(tmp_path: Path) -> None:
    client, _, artifacts_root = _build_client(tmp_path)
    marker = "INVALID_MEMORY_MARKER_82cc"
    with client:
        record = _submit(
            client,
            "/v1/dbt/jobs",
            {
                "project": "sales",
                "command": "parse",
                "resources": {"orders.yml": f"version: [{marker}\n"},
            },
        )
    assert record["status"] == "failed"
    assert "resource_parse_error" in str(record["stderr"])
    assert marker not in str(record["stdout"])
    assert marker not in str(record["stderr"])
    assert not list(artifacts_root.iterdir())


def test_blank_resources_use_existing_cli_path(tmp_path: Path) -> None:
    client, project_dir, artifacts_root = _build_client(tmp_path)
    with client:
        record = _submit(
            client,
            "/v1/dbt/jobs",
            {"project": "sales", "command": "parse", "resources": {"orders.yml": " \n"}},
        )
    assert record["status"] == "succeeded", record
    assert (project_dir / "target" / "manifest.json").is_file()
    assert not list(artifacts_root.iterdir())
