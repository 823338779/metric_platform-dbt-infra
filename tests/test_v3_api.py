"""实际装配只注册引擎 v3 业务路由。"""

import os
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from dbt_metricflow_service.api.app import create_app
from dbt_metricflow_service.settings import Settings
from tests.test_publication_storage import store as store
from tests.test_v3_build_acceptance import service
from tests.test_v3_contract import build_body

TOKEN = "fixture-token"
AUTH = {"Authorization": "Bearer " + TOKEN}


@pytest.fixture
def client(store, tmp_path):
    service(store)
    app = create_app(
        Settings(
            profiles_dir=tmp_path,
            command_timeout_seconds=30,
            max_output_bytes=1024,
            database_url=os.environ["SERVICE_TEST_DATABASE_URL"],
            service_token=TOKEN,
            temp_root=tmp_path,
            toolchain_version="api-fixture",
        )
    )
    yield TestClient(app)
    app.state.runtime.db.close()


def test_v3_build_api_and_error_contract(client):
    response = client.post("/v3/builds", json=build_body(idempotencyKey=uuid4().hex), headers=AUTH)
    assert response.status_code == 202
    body = response.json()
    assert body["buildStatus"] == "QUEUED"
    assert not {"projectId", "releaseId", "runId", "jobId"}.intersection(body)
    assert client.get("/v3/builds/" + body["buildId"]).status_code == 200
    invalid = client.post("/v3/builds", json=build_body(projectId="legacy"), headers=AUTH)
    assert invalid.status_code == 422
    assert set(invalid.json()["error"]) == {"code", "message", "retryable", "phase", "buildId"}
    assert client.post("/v3/builds", json=build_body()).status_code == 401


@pytest.mark.parametrize(
    "path",
    [
        "/v2/projects",
        "/v2/projects/x/releases",
        "/v2/projects/x/validations",
        "/v1/project-runs",
        "/v1/query-jobs",
        "/internal/git-branch-events",
    ],
)
def test_removed_business_routes_are_404(client, path):
    assert client.get(path).status_code == 404
    assert client.post(path, json={}, headers=AUTH).status_code == 404


def test_openapi_declares_only_v3_business_contract(client):
    paths = client.get("/openapi.json").json()["paths"]
    assert all(path.startswith("/v3/") or path in {"/health/live", "/health/ready", "/v1/versions"} for path in paths)
    assert "/v3/builds/{build_id}/queries" in paths


def test_exchange_fixture_matches_actual_openapi(client):
    import json
    from pathlib import Path

    from jsonschema import validate

    document = client.get("/openapi.json").json()
    fixture = json.loads((Path(__file__).parent / "fixtures/v3/contract.json").read_text(encoding="utf-8"))
    for item in fixture["examples"]:
        validate(item["value"], {"$ref": "#/components/schemas/" + item["model"], "components": document["components"]})
