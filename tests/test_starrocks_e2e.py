from __future__ import annotations

import logging
import os
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from dbt_metricflow_service.api import create_app
from dbt_metricflow_service.jobs import JobRunner
from dbt_metricflow_service.projects import ProjectRegistry
from dbt_metricflow_service.settings import Settings

logger = logging.getLogger(__name__)

E2E_ENABLED = os.getenv("RUN_STARROCKS_E2E") == "1"
E2E_ENVIRONMENT_KEYS = (
    "DBT_STARROCKS_HOST",
    "DBT_STARROCKS_PORT",
    "DBT_STARROCKS_USER",
    "DBT_ENV_SECRET_STARROCKS_PASSWORD",
    "DBT_STARROCKS_SCHEMA",
)
E2E_PROJECT = "starrocks_e2e"
FINAL_JOB_STATUSES = frozenset({"succeeded", "failed", "timed_out"})
POLL_TIMEOUT_SECONDS = 120.0
POLL_INTERVAL_SECONDS = 0.2


def _required_environment() -> dict[str, str]:
    """Read real StarRocks settings only when the caller explicitly enables E2E."""
    missing = [key for key in E2E_ENVIRONMENT_KEYS if not os.getenv(key)]
    if missing:
        pytest.skip(f"missing StarRocks E2E environment: {', '.join(missing)}")
    return {key: os.environ[key] for key in E2E_ENVIRONMENT_KEYS}


def _write_e2e_project(projects_root: Path, profiles_dir: Path, values: dict[str, str]) -> None:
    """Create a disposable dbt project whose only credentials came from environment."""
    project_dir = projects_root / E2E_PROJECT
    models_dir = project_dir / "models"
    seeds_dir = project_dir / "seeds"
    models_dir.mkdir(parents=True)
    seeds_dir.mkdir()
    profiles_dir.mkdir()

    (project_dir / "dbt_project.yml").write_text(
        "\n".join(
            (
                f"name: {E2E_PROJECT}",
                "version: '1.0.0'",
                "config-version: 2",
                f"profile: {E2E_PROJECT}",
                "model-paths: [models]",
                "seed-paths: [seeds]",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    (seeds_dir / "orders_seed.csv").write_text(
        "order_id,revenue\n1,10\n",
        encoding="utf-8",
    )
    (models_dir / "orders.sql").write_text(
        "select order_id, revenue from {{ ref('orders_seed') }}\n",
        encoding="utf-8",
    )
    (models_dir / "orders.yml").write_text(
        "version: 2\nmodels:\n  - name: orders\n    columns:\n"
        "      - name: order_id\n        data_tests: [not_null]\n",
        encoding="utf-8",
    )
    (profiles_dir / "profiles.yml").write_text(
        "\n".join(
            (
                f"{E2E_PROJECT}:",
                "  target: test",
                "  outputs:",
                "    test:",
                "      type: starrocks",
                f"      host: {values['DBT_STARROCKS_HOST']}",
                f"      port: {int(values['DBT_STARROCKS_PORT'])}",
                f"      schema: {values['DBT_STARROCKS_SCHEMA']}",
                f"      username: {values['DBT_STARROCKS_USER']}",
                "      password: \"{{ env_var('DBT_ENV_SECRET_STARROCKS_PASSWORD') }}\"",
            )
        )
        + "\n",
        encoding="utf-8",
    )


def _submit_and_wait(client: TestClient, command: str) -> dict[str, object]:
    """Submit one HTTP job and poll the shared task resource to completion."""
    response = client.post(
        "/v1/dbt/jobs",
        json={"project": E2E_PROJECT, "command": command},
    )
    assert response.status_code == 202, response.text
    job_id = response.json()["id"]
    deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        status_response = client.get(f"/v1/jobs/{job_id}")
        assert status_response.status_code == 200, status_response.text
        record = status_response.json()
        if record["status"] in FINAL_JOB_STATUSES:
            return record
        time.sleep(POLL_INTERVAL_SECONDS)
    pytest.fail(f"dbt {command} job did not finish within {POLL_TIMEOUT_SECONDS} seconds")


@pytest.mark.skipif(not E2E_ENABLED, reason="set RUN_STARROCKS_E2E=1 to run")
def test_real_starrocks_debug_seed_build_and_test(tmp_path: Path) -> None:
    """Run the supported dbt modeling lifecycle through the HTTP service."""
    values = _required_environment()
    projects_root = tmp_path / "projects"
    profiles_dir = tmp_path / "profiles"
    _write_e2e_project(projects_root, profiles_dir, values)
    settings = Settings(
        projects_root=projects_root,
        profiles_dir=profiles_dir,
        command_timeout_seconds=int(POLL_TIMEOUT_SECONDS),
        max_output_bytes=65_536,
    )
    app = create_app(
        settings,
        ProjectRegistry(projects_root),
        JobRunner(settings.command_timeout_seconds, settings.max_output_bytes),
    )

    with TestClient(app) as client:
        for command in ("debug", "seed", "build", "test"):
            record = _submit_and_wait(client, command)
            assert record["status"] == "succeeded", record


def test_e2e_profile_references_secret_environment_without_copying_value(
    tmp_path: Path,
) -> None:
    """The disposable profile must not persist a real password in pytest temp data."""
    values = {
        "DBT_STARROCKS_HOST": "127.0.0.1",
        "DBT_STARROCKS_PORT": "9030",
        "DBT_STARROCKS_USER": "fixture",
        "DBT_ENV_SECRET_STARROCKS_PASSWORD": "complex: password # value",
        "DBT_STARROCKS_SCHEMA": "fixture",
    }
    projects_root = tmp_path / "projects"
    profiles_dir = tmp_path / "profiles"

    _write_e2e_project(projects_root, profiles_dir, values)

    profile = (profiles_dir / "profiles.yml").read_text(encoding="utf-8")
    assert values["DBT_ENV_SECRET_STARROCKS_PASSWORD"] not in profile
    assert "env_var('DBT_ENV_SECRET_STARROCKS_PASSWORD')" in profile
