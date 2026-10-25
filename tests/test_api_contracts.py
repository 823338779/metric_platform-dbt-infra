from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

from dbt_metricflow_service.api.app import create_app
from dbt_metricflow_service.engines.models import DataQueryResult, DimensionQueryResult
from dbt_metricflow_service.storage.records import StoredBuild

BUILD_ID = "00000000-0000-0000-0000-000000000002"
ENVIRONMENT = "00000000-0000-0000-0000-000000000001"


@pytest.fixture
def api(monkeypatch, settings, build_request):
    now = datetime.now(UTC)
    build = StoredBuild(
        build_id=BUILD_ID, run_id=str(build_request.execution.environment_id), repository=build_request.repository,
        branch_name="main", environment=ENVIRONMENT, environment_id=ENVIRONMENT, physical_target_key="target",
        config_version="1", toolchain_version="test", caller="service", idempotency_key="key", request_digest="digest",
        request_json=build_request, config_snapshot=build_request.execution, requested_commit_sha="b" * 40,
        commit_sha="b" * 40, build_status="SUCCEEDED", phase="COMPLETE", output_set_id="output", error_code=None,
        version=1, created_at=now, updated_at=now, finished_at=now,
    )
    queries = SimpleNamespace(execute_query=Mock(return_value=DataQueryResult(rows=[["中文"]], sql="select 1")),
                              available_dimensions=lambda *args: DimensionQueryResult(),
                              lineage=lambda *args: [{"unique_id": "model.orders"}])
    artifacts = SimpleNamespace(read_file=lambda *args: b"source")
    runtime = SimpleNamespace(db=SimpleNamespace(check=lambda: None), artifacts=artifacts,
                              toolchain="test", queries=queries)
    monkeypatch.setattr("dbt_metricflow_service.api.app.Runtime", lambda _: runtime)
    monkeypatch.setattr("dbt_metricflow_service.storage.builds.BuildStore.get",
                        lambda self, identity: build if str(identity) == BUILD_ID else None)
    monkeypatch.setattr("dbt_metricflow_service.storage.builds.BuildStore.accept", lambda *args, **kwargs: build)
    monkeypatch.setattr("dbt_metricflow_service.storage.builds.BuildStore.list",
                        lambda *args, **kwargs: ([build], None))
    monkeypatch.setattr("dbt_metricflow_service.storage.jobs.JobStore.get",
                        lambda *args: SimpleNamespace(input_set_id="input"))
    monkeypatch.setattr("dbt_metricflow_service.storage.deployments.DeploymentStore.initial", lambda *args: None)
    monkeypatch.setattr("dbt_metricflow_service.storage.deployments.DeploymentStore.initials", lambda *args: {})
    monkeypatch.setattr("dbt_metricflow_service.storage.deployments.DeploymentStore.current", lambda *args: None)
    monkeypatch.setattr("dbt_metricflow_service.storage.deployments.DeploymentStore.list",
                        lambda *args: ([], None))
    monkeypatch.setattr("dbt_metricflow_service.storage.execution_logs.ExecutionLogStore.list",
                        lambda *args: ([], "poll-cursor", False))
    monkeypatch.setattr("dbt_metricflow_service.engines.dbt.DbtClient.catalog", lambda _: {"nodes": {}})
    monkeypatch.setattr("dbt_metricflow_service.engines.dbt.DbtClient.resource",
                        lambda *args: {"unique_id": "model.orders"})
    monkeypatch.setattr("dbt_metricflow_service.engines.dbt.DbtClient.source_path", lambda *args: "models/orders.sql")
    app = create_app(settings)
    return TestClient(app), app, queries


@pytest.mark.parametrize("method,path,body,status,key", [
    ("POST", "/builds", "build", 202, "execution"),
    ("GET", f"/builds?environmentId={ENVIRONMENT}", None, 200, "items"),
    ("GET", f"/builds/{BUILD_ID}", None, 200, "execution"),
    ("GET", f"/builds/{BUILD_ID}/execution-logs", None, 200, "nextCursor"),
    ("GET", f"/builds/{BUILD_ID}/catalog", None, 200, "nodes"),
    ("GET", f"/builds/{BUILD_ID}/resources/model.orders", None, 200, "source"),
    ("GET", f"/builds/{BUILD_ID}/lineage?selector=orders", None, 200, None),
    ("GET", f"/deployments?repository=repo&environmentId={ENVIRONMENT}", None, 200, "items"),
    ("GET", f"/deployments/current?repository=repo&environmentId={ENVIRONMENT}&branchName=main", None, 200, "version"),
    ("POST", f"/query/available-dimensions?build_id={BUILD_ID}", {"metrics": ["revenue"]}, 200, "dimensions"),
    ("POST", f"/query?build_id={BUILD_ID}", {"mode": "QUERY", "metrics": ["revenue"]}, 200, "rows"),
    ("GET", "/health/live", None, 200, "status"),
    ("GET", "/health/ready", None, 200, "status"),
    ("GET", "/versions", None, 200, "dbt-core"),
])
def test_all_http_operations(api, build_request, monkeypatch, method, path, body, status, key):
    client, _, _ = api
    monkeypatch.setattr("shutil.which", lambda *args: None)
    payload = build_request.model_dump(mode="json", by_alias=True) if body == "build" else body
    response = client.request(method, path, json=payload)
    assert response.status_code == status, response.text
    if key:
        assert key in response.json()
    else:
        assert isinstance(response.json(), list)


def test_openapi_describes_modes_and_system_responses(api):
    _, app, _ = api
    paths = app.openapi()["paths"]
    assert sum(method in {"get", "post"} for item in paths.values() for method in item) == 14
    schema = paths["/query"]["post"]["requestBody"]["content"]["application/json"]["schema"]
    assert schema["discriminator"]["propertyName"] == "mode"
    assert set(schema["discriminator"]["mapping"]) == {"QUERY", "EXPLAIN", "PREVIEW", "DIMENSION_VALUES"}
    for path in ["/health/live", "/health/ready", "/versions"]:
        assert paths[path]["get"]["responses"]["200"]["content"]["application/json"]["schema"]


@pytest.mark.parametrize("path", ["", "/catalog", "/resources/model.orders", "/lineage?selector=orders",
                                  "/execution-logs"])
def test_missing_build_errors(api, path):
    client, _, _ = api
    response = client.get(f"/builds/00000000-0000-0000-0000-000000000003{path}")
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "BUILD_NOT_FOUND"


@pytest.mark.parametrize("body", [
    {"mode": "PREVIEW", "selector": "orders", "metrics": []},
    {"mode": "QUERY", "metrics": ["revenue"], "filters": [{"optionId": "d", "operator": "IN", "value": 3}]},
])
def test_invalid_queries_do_not_start_engine(api, body):
    client, _, queries = api
    response = client.post(f"/query?build_id={BUILD_ID}", json=body)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "INVALID_REQUEST"
    queries.execute_query.assert_not_called()


@pytest.mark.parametrize("threshold", [1e-5, 1e-7])
def test_lineage_limit_matches_http_json_bytes(api, settings, threshold):
    _, _, queries = api
    queries.lineage = lambda *args: [{"config": {"meta": {"threshold": threshold, "label": "中文"}}}]
    path = f"/builds/{BUILD_ID}/lineage?selector=orders"
    response = TestClient(create_app(settings)).get(path)
    assert response.status_code == 200
    actual_size = len(response.content)
    for limit, expected_status in [(actual_size, 200), (actual_size - 1, 422)]:
        client = TestClient(create_app(replace(settings, max_result_bytes=limit)))
        limited = client.get(path)
        assert limited.status_code == expected_status, limited.text
        if expected_status == 422:
            assert limited.json()["error"]["code"] == "RESULT_TOO_LARGE"
