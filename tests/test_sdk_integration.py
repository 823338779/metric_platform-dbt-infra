import asyncio
from types import SimpleNamespace
from uuid import uuid4

from fastapi.testclient import TestClient

from dbt_metricflow_service.api.app import create_app
from dbt_metricflow_service.application.builds import BuildService
from dbt_metricflow_service.application.catalog import CatalogService
from dbt_metricflow_service.application.deployments import DeploymentService
from dbt_metricflow_service.execution.build import BuildExecutor
from dbt_metricflow_service.execution.query import QueryExecutor
from dbt_metricflow_service.models.deployments import DeploymentKey
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.builds import BuildStore
from dbt_metricflow_service.storage.deployments import DeploymentStore
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.records import ArtifactMetadata, LeasedJob


def write_project(directory):
    (directory / "models").mkdir(parents=True)
    (directory / "dbt_project.yml").write_text(
        "name: api_review\nversion: '1.0'\nconfig-version: 2\nprofile: execution\n"
        "model-paths: [models]\nmodels:\n  api_review:\n    +materialized: table\n", encoding="utf-8")
    (directory / "models/orders.sql").write_text(
        "select 1 as id, 'paid' as status, timestamp '2024-01-01' as ds, 12.34::decimal(10,2) as amount "
        "union all select 2, 'new', timestamp '2024-01-02', 2.00::decimal(10,2)", encoding="utf-8")
    (directory / "models/time_spine.sql").write_text(
        "select cast(day as date) as date_day from generate_series("
        "date '2023-12-01', date '2024-02-01', interval 1 day) as days(day)", encoding="utf-8")
    (directory / "models/semantic.yml").write_text("""version: 2
models:
  - name: time_spine
    time_spine:
      standard_granularity_column: date_day
    columns:
      - name: date_day
        granularity: day
semantic_models:
  - name: orders
    model: ref('orders')
    defaults:
      agg_time_dimension: ds
    entities:
      - name: order
        type: primary
        expr: id
    dimensions:
      - name: ds
        type: time
        type_params:
          time_granularity: day
      - name: status
        type: categorical
    measures:
      - name: amount
        agg: sum
        expr: amount
metrics:
  - name: revenue
    label: Revenue
    type: simple
    type_params:
      measure: amount
""", encoding="utf-8")


def test_real_build_and_all_http_operations(database, settings, build_request, tmp_path, monkeypatch):
    artifacts = ArtifactStore(database)
    jobs = JobStore(database)
    executor = QueryExecutor(settings, artifacts)
    runtime = SimpleNamespace(db=database, artifacts=artifacts, queries=executor, toolchain="test-toolchain")
    monkeypatch.setattr("dbt_metricflow_service.api.app.Runtime", lambda _: runtime)
    monkeypatch.setattr("dbt_metricflow_service.api.app.remote_head", lambda *args, **kwargs: "b" * 40)
    client = TestClient(create_app(settings))
    headers = {"Authorization": "Bearer test-token"}
    body = build_request.model_dump(mode="json", by_alias=True)
    body["deploymentPolicy"] = "ON_SUCCESS"
    accepted = client.post("/builds", json=body, headers=headers)
    assert accepted.status_code == 202, accepted.text
    build_id = accepted.json()["buildId"]
    assert "configVersion" not in accepted.json()
    builds = BuildStore(database)
    build = builds.get(build_id)
    leased = jobs.claim(uuid4(), toolchain_version="test-toolchain", job_id=build.run_id)
    assert leased is not None
    project = tmp_path / "input"
    write_project(project)
    source_id = artifacts.capture(leased.project_id, project, metadata=ArtifactMetadata(
        project_digest="test-project-digest", source_commit_sha=build.commit_sha,
    ))
    assert jobs.attach_input(leased.job_id, leased.lease_token, source_id, project_digest="test-project-digest")
    leased = LeasedJob(**jobs.get(leased.job_id).model_dump(),
                       attempt_id=leased.attempt_id, lease_token=leased.lease_token)
    result = asyncio.run(BuildExecutor(settings, jobs, artifacts).execute(leased))
    # A stale lease cannot publish; the proper lease seals and publishes the same output.
    assert not jobs.finish(leased.job_id, uuid4(), output_set_id=result.output_set_id, seal=artifacts.seal)
    assert jobs.finish(leased.job_id, leased.lease_token, output_set_id=result.output_set_id, seal=artifacts.seal)
    view = client.get(f"/builds/{build_id}").json()
    assert view["buildStatus"] == "SUCCEEDED"
    assert view["catalogAvailable"] is True
    environment = str(build_request.execution.environment_id)
    listing = client.get("/builds", params={"environmentId": environment})
    assert listing.status_code == 200
    assert [row["buildId"] for row in listing.json()["items"]] == [build_id]
    logs = client.get(f"/builds/{build_id}/execution-logs", params={"limit": 200})
    assert logs.status_code == 200 and logs.json()["items"]
    assert logs.json()["nextCursor"]
    catalog = client.get(f"/builds/{build_id}/catalog")
    assert catalog.status_code == 200, catalog.text
    assert "model.api_review.orders" in catalog.json()["nodes"]
    resource = client.get(f"/builds/{build_id}/resources/model.api_review.orders")
    assert resource.status_code == 200, resource.text
    assert "12.34" in resource.json()["source"]["content"]
    lineage = client.get(f"/builds/{build_id}/lineage", params={"selector": "orders"})
    assert lineage.status_code == 200, lineage.text
    assert any(row["unique_id"] == "model.api_review.orders" for row in lineage.json())
    dimensions = client.post("/query/available-dimensions", params={"build_id": build_id},
                             json={"metrics": ["revenue"]}, headers=headers)
    assert dimensions.status_code == 200, dimensions.text
    status = next(row for row in dimensions.json()["dimensions"] if row["name"] == "status")
    assert "valueType" not in status
    for mode, fields in [
        ("QUERY", {"metrics": ["revenue"], "groupBy": [{"optionId": status["optionId"]}]}),
        ("EXPLAIN", {"metrics": ["revenue"]}),
        ("PREVIEW", {"selector": "orders"}),
        ("DIMENSION_VALUES", {"metrics": ["revenue"], "dimensionOptionId": status["optionId"]}),
    ]:
        response = client.post("/query", params={"build_id": build_id},
                               json={"mode": mode, "limit": 1, **fields}, headers=headers)
        assert response.status_code == 200, (mode, response.text)
        payload = response.json()
        assert payload["mode"] == mode and "boundaryPolicy" not in payload
        if mode == "EXPLAIN":
            assert "sql" in payload and "rows" not in payload
            assert "LIMIT 2" in payload["sql"].upper()
        else:
            assert len(payload["rows"]) == 1 and payload["resultTruncated"] is True
            assert "sql" not in payload
        if mode == "PREVIEW":
            assert "normalizedTimeRange" not in payload
    filtered = client.post("/query", params={"build_id": build_id}, headers=headers, json={
        "mode": "QUERY", "metrics": ["revenue"],
        "filters": [{"optionId": status["optionId"], "operator": "IN", "value": ["paid"]}],
    })
    assert filtered.status_code == 200, filtered.text
    assert float(filtered.json()["rows"][0][0]) == 12.34
    service = BuildService(builds, "test-toolchain", settings)
    catalogs = CatalogService(builds, artifacts, executor.lineage, settings.max_result_bytes)
    deployments = DeploymentService(DeploymentStore(database), lambda *args: "b" * 40,
                                    builds=builds, catalogs=catalogs)
    key = DeploymentKey(repository=build_request.repository, environment=environment, branch_name="main")
    assert deployments.reconcile(key, 1).deployment_status == "DEPLOYED"
    params = {"repository": build_request.repository, "environmentId": environment, "branchName": "main"}
    current = client.get("/deployments/current", params=params)
    assert current.status_code == 200 and current.json()["activeBuildId"] == build_id
    history = client.get("/deployments", params=params)
    assert history.status_code == 200 and history.json()["items"][0]["deploymentStatus"] == "DEPLOYED"
    assert client.get("/health/live").json() == {"status": "ok"}
    assert client.get("/health/ready").json() == {"status": "ready"}
    assert client.get("/versions").status_code == 200
    assert service.get(build_id).initial_deployment.deployment_status == "DEPLOYED"
