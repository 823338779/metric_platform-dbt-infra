"""新环境构建接口与旧调用方并存，环境筛选覆盖全部构建状态。"""

import os
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from dbt_metricflow_service.api.app import create_app
from dbt_metricflow_service.settings import Settings
from tests.test_api import AUTH, TOKEN
from tests.test_execution_profiles import settings as infrastructure
from tests.test_execution_snapshot_storage import request, snapshot
from tests.test_publication_storage import store as store


@pytest.fixture
def client(store, tmp_path):
    config = infrastructure()
    app = create_app(Settings(profiles_dir=tmp_path, max_output_bytes=1024,
        database_url=os.environ["SERVICE_TEST_DATABASE_URL"], service_token=TOKEN, temp_root=tmp_path,
        toolchain_version="environment-api", execution_instances=config.execution_instances,
        execution_credentials=config.execution_credentials))
    yield TestClient(app)
    app.state.runtime.db.close()


def submit(client, **changes):
    response = client.post("/builds", json=request(**changes).model_dump(mode="json", by_alias=True), headers=AUTH)
    assert response.status_code == 202, response.text
    return response.json()


def test_environment_history_includes_failed_unpublished_builds(client):
    environment = snapshot()
    first = submit(client, execution=environment)
    second = submit(client, execution=environment, branchName="feature/test")
    # 直接模拟失败任务事实，环境历史仍包含没有部署的失败构建。
    with client.app.state.runtime.db.transaction() as connection:
        connection.exec_driver_sql(
            "UPDATE runtime_job SET status='FAILED',error_code='TEST_FAILURE' "
            "WHERE job_id=(SELECT run_id FROM engine_build WHERE build_id=%s)", (second["buildId"],)
        )
    response = client.get("/builds", params={"environmentId": str(environment.environment_id)})
    assert response.status_code == 200
    rows = response.json()["items"]
    assert {row["buildId"] for row in rows} == {first["buildId"], second["buildId"]}
    assert all(row["initialDeployment"] is None for row in rows)
    assert next(row for row in rows if row["buildId"] == second["buildId"])["buildStatus"] == "FAILED"


def test_cursor_is_bound_to_environment_and_filters(client):
    environment = snapshot()
    submit(client, execution=environment)
    submit(client, execution=environment)
    first = client.get("/builds", params={"environmentId": str(environment.environment_id), "limit": 1}).json()
    assert first["nextCursor"]
    response = client.get("/builds", params={"environmentId": str(uuid4()), "cursor": first["nextCursor"]})
    assert response.status_code == 422


def test_new_environment_can_build_main_or_feature_branch(client):
    environment = snapshot()
    for branch in ("main", "feature/test"):
        result = submit(client, execution=environment, branchName=branch, deploymentPolicy="ON_SUCCESS")
        assert result["execution"]["environmentId"] == str(environment.environment_id)
        assert result["initialDeployment"]["environmentId"] == str(environment.environment_id)
        assert result["initialDeployment"]["deploymentStatus"] == "WAITING_FOR_BUILD"


def test_v3_response_never_serializes_uuid_as_environment_enum(client):
    result = submit(client)
    assert client.get("/v3/builds/" + result["buildId"]).status_code == 404
    assert client.get("/builds/" + result["buildId"]).status_code == 200
    assert client.get("/v3/builds", params={"repository": result["repository"]}).status_code == 404


def test_snapshot_build_requires_service_authentication(client):
    assert client.post("/builds", json=request().model_dump(mode="json", by_alias=True)).status_code == 401


def test_unknown_timezone_is_input_error(client):
    body = request().model_dump(mode="json", by_alias=True)
    body["execution"]["businessTimezone"] = "Invalid/Unknown"
    assert client.post("/builds", json=body, headers=AUTH).status_code == 422


def test_v3_cancel_rejects_snapshot_before_mutation(client):
    result = submit(client)
    response = client.post(f"/v3/builds/{result['buildId']}/cancel", headers=AUTH)
    assert response.status_code == 404
    after = client.get(f"/builds/{result['buildId']}").json()
    assert "cancelRequested" not in after
    assert after["buildStatus"] == result["buildStatus"]


def test_v3_deployment_rejects_snapshot_before_mutation(client):
    from sqlalchemy import text

    result = submit(client)
    database = client.app.state.runtime.db
    with database.transaction() as connection:
        connection.execute(text("UPDATE engine_build SET build_status='SUCCEEDED', commit_sha=:sha WHERE build_id=:id"),
                           {"id": result["buildId"], "sha": "a" * 40})
        before = connection.execute(text("SELECT count(*) FROM engine_deployment_attempt")).scalar()
    response = client.post("/v3/deployments", headers=AUTH, json={
        "buildId": result["buildId"], "branchName": "main", "expectedTargetVersion": 0,
        "idempotencyKey": uuid4().hex,
    })
    assert response.status_code == 404
    with database.transaction() as connection:
        assert connection.execute(text("SELECT count(*) FROM engine_deployment_attempt")).scalar() == before
