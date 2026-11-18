import json
import os
import socket
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
import uvicorn
import yaml

pytest.importorskip("mcp")
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
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
from dbt_metricflow_service.storage.records import ArtifactMetadata, LeasedJob


@contextmanager
def live_api(app):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        server = uvicorn.Server(uvicorn.Config(app, lifespan="off", log_level="warning"))
        thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
        thread.start()
        try:
            deadline = time.monotonic() + 10
            while not server.started:
                assert thread.is_alive() and time.monotonic() < deadline
                time.sleep(0.02)
            yield f"http://127.0.0.1:{sock.getsockname()[1]}"
        finally:
            server.should_exit = True
            thread.join(10)


@pytest.mark.asyncio
async def test_stdio_sales_on_oversized_catalog(database, settings, build_request, tmp_path, monkeypatch):
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
    env_id = str(build_request.execution.environment_id)
    deploy_args = {"repository": build_request.repository, "environment_id": env_id, "branch_name": "main"}
    with live_api(create_app(settings)) as url:
        launch = StdioServerParameters(command=sys.executable, args=["-m", "dbt_metricflow_service.mcp_server"],
                                       env={**os.environ, "DBT_METRICFLOW_SERVICE_URL": url})
        async with stdio_client(launch) as streams, ClientSession(*streams) as session, httpx.AsyncClient(
            base_url=url, timeout=120
        ) as http:
            await session.initialize()
            tools = (await session.list_tools()).tools
            assert len(tools) == 7 and all(t.inputSchema["type"] == "object" for t in tools)

            async def call(name, args):
                result = await session.call_tool(name, args)
                assert not result.isError, result.content
                return result.structuredContent

            assert (await call("get_current_deployment", deploy_args))["status"] == "undeployed"
            args = build_request.model_dump(mode="json", exclude_none=True)
            args["deployment_policy"] = "ON_SUCCESS"
            build_id = (await call("submit_build", args))["buildId"]
            assert (await call("submit_build", args))["buildId"] == build_id
            async def finish(build_id):
                build = builds.get(build_id)
                leased = jobs.claim(uuid4(), toolchain_version="test", job_id=build.run_id)
                source = artifacts.capture(leased.project_id, project, metadata=ArtifactMetadata(
                    project_digest="mcp-test", source_commit_sha=build.commit_sha))
                assert jobs.attach_input(leased.job_id, leased.lease_token, source, project_digest="mcp-test")
                leased = LeasedJob(**jobs.get(leased.job_id).model_dump(),
                                   attempt_id=leased.attempt_id, lease_token=leased.lease_token)
                try:
                    built = await BuildExecutor(settings, jobs, artifacts).execute(leased)
                except ExecutionError as exc:
                    pytest.fail(str(exc.payload))
                assert jobs.finish(leased.job_id, leased.lease_token,
                                   output_set_id=built.output_set_id, seal=artifacts.seal)

            await finish(build_id)
            deployments = DeploymentService(DeploymentStore(database), lambda *_: "b" * 40, builds=builds,
                                            catalogs=CatalogService(builds, artifacts, executor.lineage, 4096))
            key = DeploymentKey(repository=build_request.repository, environment=env_id, branch_name="main")
            assert deployments.reconcile(key, 1).deployment_status == "DEPLOYED"
            assert (await call("get_current_deployment", deploy_args))["activeBuildId"] == build_id
            if inspector := os.environ.get("MCP_INSPECTOR_ENTRY"):
                config = tmp_path / "inspector.json"
                config.write_text(json.dumps({"mcpServers": {"dbt": {"command": sys.executable,
                    "args": launch.args, "env": {"DBT_METRICFLOW_SERVICE_URL": url}}}}), encoding="utf-8")
                cli = ["node", inspector, "--cli", "--config", str(config), "--server", "dbt", "--method"]

                def inspect(*arguments):
                    proc = subprocess.run(cli + list(arguments), capture_output=True, encoding="utf-8", timeout=60)
                    assert proc.returncode == 0, proc.stderr
                    return json.loads(proc.stdout)

                listed = inspect("tools/list")["tools"]
                assert {t["name"]: t["inputSchema"] for t in listed} == {t.name: t.inputSchema for t in tools}
                current = inspect("tools/call", "--tool-name", "get_current_deployment", "--tool-arg",
                                  *(f"{k}={v}" for k, v in deploy_args.items()))
                assert current["structuredContent"]["activeBuildId"] == build_id
                print("Inspector and Python client verified active build", build_id)
            assert (await call("get_build", {"build_id": build_id}))["catalogAvailable"]
            full = await http.get(f"/builds/{build_id}/catalog")
            assert full.json()["error"]["code"] == "RESULT_TOO_LARGE"
            search = {"build_id": build_id, "keyword": "销售额", "limit": 1}
            started = time.monotonic()
            page = await call("search_catalog", search)
            print("First search page (ms)", round((time.monotonic() - started) * 1000))
            assert page["items"][0]["name"] == "derived0"
            page = await call("search_catalog", {**search, "cursor": page["nextCursor"]})
            uid = page["items"][0]["resourceId"]
            detail = await call("get_resource", {"build_id": build_id, "resource_id": uid})
            assert detail["resource"]["name"] == "revenue"
            selection = {"build_id": build_id, "metric_resource_ids": [uid]}
            dims = await call("get_available_dimensions", selection)
            channel = next(d for d in dims["dimensions"] if d["name"] == "channel")["optionId"]
            group = [{"optionId": channel}]
            bounds = {"start_time": "2024-01-01T00:00:00", "end_time": "2024-01-31T23:59:59"}
            ordering = [{"fieldId": channel, "direction": "ASC"}]
            result = await call("query_metrics", {**selection, **bounds, "group_by": group, "order_by": ordering})
            direct = await http.post("/query", params={"build_id": build_id}, json={
                "mode": "QUERY", "metrics": ["revenue"], "groupBy": group, "orderBy": ordering,
                "startTime": bounds["start_time"], "endTime": bounds["end_time"]})
            assert result == direct.json()
            assert [(row[0], float(row[1])) for row in result["rows"]] == [("new", 2.0), ("paid", 12.34)]
            empty = await call("query_metrics", {**selection, "group_by": group, "filters": [
                {"optionId": channel, "operator": "=", "value": "missing"}]})
            assert empty["columns"] and empty["rows"] == []
            invalid = await session.call_tool("query_metrics", {**selection, "group_by": [{"optionId": "invalid"}]})
            assert invalid.isError and invalid.structuredContent["error"]["buildId"] == build_id

            second = {**args, "idempotency_key": "second",
                      "execution": {**args["execution"], "schema_name": "next", "config_version": 2}}
            second_id = (await call("submit_build", second))["buildId"]
            await finish(second_id)
            assert deployments.reconcile(key, 2).deployment_status == "DEPLOYED"
            assert (await call("get_current_deployment", deploy_args))["activeBuildId"] == second_id
            assert (await call("query_metrics", {**selection, **bounds, "group_by": group,
                                                 "order_by": ordering})) == result
