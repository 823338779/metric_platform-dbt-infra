from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
import yaml
from test_sdk_integration import write_project

from dbt_metricflow_service.api.app import create_app
from dbt_metricflow_service.application.catalog import CatalogService
from dbt_metricflow_service.application.deployments import DeploymentService
from dbt_metricflow_service.execution.build import BuildExecutor
from dbt_metricflow_service.execution.errors import ExecutionError
from dbt_metricflow_service.execution.query import QueryExecutor
from dbt_metricflow_service.models.deployments import DeploymentKey
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.builds import BuildStore
from dbt_metricflow_service.storage.deployments import DeploymentStore
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.records import ArtifactMetadata


@pytest.mark.asyncio
async def test_sales_on_oversized_catalog(database, settings, build_request, tmp_path, monkeypatch):
    settings = replace(settings, max_result_bytes=4096, max_output_bytes=16000)
    artifacts, jobs, builds = ArtifactStore(database), JobStore(database), BuildStore(database)
    executor = QueryExecutor(settings, artifacts)
    runtime = SimpleNamespace(db=database, artifacts=artifacts, queries=executor, toolchain="test")
    monkeypatch.setattr("dbt_metricflow_service.api.app.Runtime", lambda _: runtime)
    monkeypatch.setattr("dbt_metricflow_service.api.app.remote_head", lambda *_, **kw: "b" * 40)

    project = tmp_path / "project"
    write_project(project)
    sql = project / "models/orders.sql"
    sql.write_text(sql.read_text().replace("as status", "as channel"), encoding="utf-8")
    semantic = project / "models/semantic.yml"
    doc = yaml.safe_load(semantic.read_text(encoding="utf-8"))
    doc["semantic_models"][0]["dimensions"][1]["name"] = "channel"
    doc["metrics"][0]["label"] = "销售额"
    for n in range(32):
        doc["metrics"].append({"name": f"derived{n}", "label": "双倍销售额" if n == 0 else f"derived{n}",
                               "type": "derived", "type_params": {"expr": "revenue * 2",
                                                                  "metrics": [{"name": "revenue"}]}})
    semantic.write_text(yaml.safe_dump(doc, allow_unicode=True), encoding="utf-8")

    async def finish(build_id):
        build = builds.get(build_id)
        leased = jobs.claim(uuid4(), toolchain_version="test", build_id=build.build_id)
        source = artifacts.capture(leased.project_id, project, metadata=ArtifactMetadata(
            project_digest="catalog-test", source_commit_sha=build.commit_sha))
        assert jobs.attach_input(leased.build_id, leased.lease_token, source, project_digest="catalog-test")
        leased = leased.model_copy(update={"input_set_id": source, "project_digest": "catalog-test"})
        try:
            built = await BuildExecutor(settings, jobs, artifacts).execute(leased)
        except ExecutionError as exc:
            pytest.fail(str(exc.payload))
        assert jobs.finish(leased.build_id, leased.lease_token,
                           output_set_id=built.output_set_id, seal=artifacts.seal)

    # 通过公开 HTTP 接口提交两次请求，验证相同幂等键只创建一个构建。
    env_id = str(build_request.execution.environment_id)
    target = {"repository": build_request.repository, "environmentId": env_id, "branchName": "main"}
    key = DeploymentKey(repository=build_request.repository, environment=env_id, branch_name="main")
    deployments = DeploymentService(DeploymentStore(database), lambda *_: "b" * 40, builds=builds,
                                   catalogs=CatalogService(builds, artifacts, executor.lineage, 4096))
    transport = httpx.ASGITransport(app=create_app(settings))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as api:
        assert (await api.get("/deployments/current", params=target)).json()["activeBuildId"] is None
        args = {**build_request.model_dump(mode="json", by_alias=True), "deploymentPolicy": "ON_SUCCESS"}
        build_id = (await api.post("/builds", json=args)).raise_for_status().json()["buildId"]
        assert (await api.post("/builds", json=args)).json()["buildId"] == build_id
        await finish(build_id)
        assert deployments.reconcile(key, 1).deployment_status == "DEPLOYED"
        assert (await api.get("/deployments/current", params=target)).json()["activeBuildId"] == build_id
        root = f"/builds/{build_id}"
        assert (await api.get(root)).json()["catalogAvailable"]

        # 全量目录超限时仍可分页搜索，并以固定版本取得节点、选项和真实执行结果。
        assert (await api.get(root + "/catalog")).json()["error"]["code"] == "RESULT_TOO_LARGE"
        params = {"keyword": "销售额", "limit": 1}
        page = (await api.get(root + "/catalog/search", params=params)).raise_for_status().json()
        assert page["items"][0]["name"] == "derived0"
        page = (await api.get(root + "/catalog/search", params={**params, "cursor": page["nextCursor"]})).json()
        uid = page["items"][0]["resourceId"]
        resource = (await api.get(f"{root}/resources/{uid}")).json()["resource"]
        assert resource["name"] == "revenue"
        fixed = {"build_id": build_id}
        metric = {"metrics": [resource["name"]]}
        dims = (await api.post("/query/available-dimensions", params=fixed, json=metric)).json()["dimensions"]
        channel = next(d["optionId"] for d in dims if d["name"] == "channel")
        query = {**metric, "mode": "QUERY", "groupBy": [{"optionId": channel}],
                 "orderBy": [{"fieldId": channel, "direction": "ASC"}],
                 "startTime": "2024-01-01T00:00:00", "endTime": "2024-01-31T23:59:59"}
        result = (await api.post("/query", params=fixed, json=query)).raise_for_status().json()
        assert [(row[0], float(row[1])) for row in result["rows"]] == [("new", 2.0), ("paid", 12.34)]
        empty = (await api.post("/query", params=fixed, json={
            **query, "filters": [{"optionId": channel, "operator": "=", "value": "missing"}]})).json()
        assert empty["columns"] and empty["rows"] == []
        invalid = await api.post("/query", params=fixed, json={**query, "groupBy": [{"optionId": "invalid"}]})
        assert invalid.status_code == 422 and invalid.json()["error"]["code"] == "INVALID_DIMENSION_OPTION"

        # 部署切换后仍按调用方指定的旧构建查询，并返回被回收的构建身份。
        second = {**args, "idempotencyKey": str(uuid4()), "execution": {**args["execution"], "schema": "next"}}
        second_id = (await api.post("/builds", json=second)).raise_for_status().json()["buildId"]
        await finish(second_id)
        assert deployments.reconcile(key, 2).deployment_status == "DEPLOYED"
        assert (await api.get("/deployments/current", params=target)).json()["activeBuildId"] == second_id
        retired = (await api.post("/query", params=fixed, json=query)).json()["error"]
        assert (retired["code"], retired["buildId"]) == ("ARTIFACT_UNAVAILABLE", build_id)
