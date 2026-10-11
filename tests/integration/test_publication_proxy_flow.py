"""无外部业务系统参与的真实构建、历史读取和四种查询。"""

import asyncio
import os
import shutil
from decimal import Decimal
from uuid import uuid4

import pytest
import yaml

from dbt_metricflow_service.application.builds import BuildService
from dbt_metricflow_service.application.catalog import CatalogService
from dbt_metricflow_service.application.deployments import DeploymentService
from dbt_metricflow_service.application.queries import QueryService
from dbt_metricflow_service.models.builds import BuildRequest
from dbt_metricflow_service.models.deployments import DeploymentKey
from dbt_metricflow_service.models.queries import OptionsRequest, QueryRequest
from dbt_metricflow_service.platform.source import is_ancestor, remote_head
from dbt_metricflow_service.runtime.completion import complete_job
from dbt_metricflow_service.runtime.executor import ExecutionError, RuntimeExecutor
from dbt_metricflow_service.runtime.service import Runtime
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.builds import BuildStore
from dbt_metricflow_service.storage.deployments import DeploymentStore
from tests.integration.helpers import FIXTURE, PROFILES, git


async def test_build_history_and_all_query_modes(tmp_path, monkeypatch):
    if os.getenv("PLATFORM_TEST_POSTGRES") != "1" or not os.getenv("SERVICE_TEST_DATABASE_URL"):
        pytest.skip("需要独立 PostgreSQL 与真实工具链环境")
    # 真实构建、验证和查询必须调用 SDK，禁止重新引入引擎 CLI 子进程。
    async def forbidden_child(*args, **kwargs):
        raise AssertionError("engine execution must stay in process")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", forbidden_child)
    repo = tmp_path / "repo"
    shutil.copytree(FIXTURE, repo)
    # 原始工程声明非默认输出位置，执行副本和封存仍必须使用同一受控路径。
    project_file = repo / "dbt_project.yml"
    project_config = yaml.safe_load(project_file.read_text(encoding="utf-8"))
    project_config["target-path"] = "custom-target"
    project_file.write_text(yaml.safe_dump(project_config), encoding="utf-8")
    git(repo, "init", "-b", "main")
    git(repo, "config", "user.name", "Test")
    git(repo, "config", "user.email", "test@example.invalid")
    git(repo, "add", ".")
    git(repo, "commit", "-m", "fixture")
    profiles = tmp_path / "profiles"
    profiles.mkdir()
    (profiles / "profiles.yml").write_text(PROFILES)
    settings = Settings(
        profiles,
        180,
        1048576,
        database_url=os.environ["SERVICE_TEST_DATABASE_URL"],
        temp_root=tmp_path / "runtime",
        toolchain_version=uuid4().hex,
    )
    runtime = Runtime(settings)
    runtime.db.initialize()
    store = BuildStore(runtime.db)
    store.register_binding(str(repo), "engine", "1", {"profileBindingId": "postgres", "environments": ["PRODUCTION"]})
    builds = BuildService(store, runtime.toolchain, 180)
    catalogs = CatalogService(store, runtime.artifacts)
    queries = QueryService(builds, catalogs, runtime.jobs)
    deployments = DeploymentService(
        DeploymentStore(runtime.db),
        lambda repository, branch: remote_head(repository, branch, settings.temp_root),
        lambda repository, first, second: is_ancestor(repository, first, second, settings.temp_root),
        builds=builds,
        catalogs=catalogs,
    )
    target = DeploymentKey(repository=str(repo), branchName="main", environment="PRODUCTION")

    async def execute(kind):
        job = runtime.jobs.claim(str(uuid4()), toolchain_version=runtime.toolchain, kinds=[kind])
        assert job
        result = await RuntimeExecutor(settings, runtime.jobs, runtime.artifacts).execute(job)
        assert complete_job(
            runtime.jobs, job["job_id"], job["lease_token"], result.payload, output_set_id=result.output_set_id
        )
        return result

    async def build():
        view = builds.submit(
            BuildRequest(
                repository=str(repo),
                branchName="main",
                commitSha=git(repo, "rev-parse", "HEAD"),
                environment="PRODUCTION",
                executionBinding="engine",
                configVersion="1",
                deploymentPolicy="ON_SUCCESS",
                idempotencyKey=uuid4().hex,
            ),
            "integration",
        )
        await execute("BUILD_RUN")
        assert builds.get(view.build_id).build_status == "SUCCEEDED"
        assert deployments.reconcile(target, view.initial_deployment["generation"]).deployment_status == "DEPLOYED"
        return view

    async def query(build_id, **fields):
        view = queries.submit(build_id, QueryRequest(idempotencyKey=uuid4().hex, **fields), "integration")
        await execute("METRIC_QUERY")
        return queries.results(view.query_id)

    try:
        first = await build()
        assert yaml.safe_load(project_file.read_text(encoding="utf-8"))["target-path"] == "custom-target"
        resources = catalogs.list(first.build_id).model_dump(mode="json", by_alias=True)["items"]
        metric = next(item["resourceId"] for item in resources if item["kind"] == "METRIC")
        dataset = next(item["resourceId"] for item in resources if "PREVIEW" in item["capabilities"])
        task = queries.submit_options(
            first.build_id, OptionsRequest(idempotencyKey=uuid4().hex, metricResourceIds=[metric]), "integration"
        )
        await execute("QUERY_OPTIONS")
        options = queries.get_options(task.options_task_id).options
        dimension = next(item["optionId"] for item in options if item["dimensionType"] != "time")
        first_result = await query(first.build_id, mode="QUERY", metricResourceIds=[metric])
        assert Decimal(str(first_result.rows[0][0])) == Decimal("31.00")
        explain = await query(first.build_id, mode="EXPLAIN", metricResourceIds=[metric])
        assert explain.sql and "select" in explain.sql.lower()
        assert (await query(first.build_id, mode="PREVIEW", datasetResourceId=dataset)).rows
        assert (
            await query(
                first.build_id, mode="DIMENSION_VALUES", metricResourceIds=[metric], dimensionOptionId=dimension
            )
        ).rows
        # 语义变更也执行完整构建，物理关系全部独立；不复用旧模型。
        before = runtime.artifacts.metadata(store.get(first.build_id)["output_set_id"])["catalog_json"]
        yaml_file = next(path for path in (repo / "models").glob("*.yml") if "revenue" in path.read_text())
        yaml_file.write_text(yaml_file.read_text() + "\n# semantic documentation update\n")
        git(repo, "commit", "-am", "semantic-change")
        semantic = await build()
        after = runtime.artifacts.metadata(store.get(semantic.build_id)["output_set_id"])["catalog_json"]
        assert {item["relation"]["identifier"] for item in after["relationBindings"]} != {
            item["relation"]["identifier"] for item in before["relationBindings"]
        }
        assert all(item["mode"] == "BUILT" for item in after["relationBindings"])
        queued = queries.submit(
            first.build_id,
            QueryRequest(idempotencyKey=uuid4().hex, mode="QUERY", metricResourceIds=[metric]),
            "integration",
        )
        # 新源码生成独立物理对象；新消费者仍可对旧构建提交查询。
        path = repo / "models" / "orders.sql"
        path.write_text(path.read_text().replace("10.25", "11.25"))
        git(repo, "commit", "-am", "new-data")
        second = await build()
        assert second.build_id != first.build_id
        assert deployments.current(target).active_build_id == second.build_id
        await execute("METRIC_QUERY")
        assert queries.results(queued.query_id).rows == first_result.rows
        assert (await query(first.build_id, mode="QUERY", metricResourceIds=[metric])).rows == first_result.rows
        assert Decimal(
            str((await query(second.build_id, mode="QUERY", metricResourceIds=[metric])).rows[0][0])
        ) == Decimal("32.00")
        # 非法模板必须在外部写入前失败，不能破坏活动部署或旧物理数据。
        unsafe = repo / "macros" / "unsafe.sql"
        unsafe.write_text("{{ run_query('drop table protected') }}")
        git(repo, "add", ".")
        git(repo, "commit", "-m", "unsafe-candidate")
        rejected = builds.submit(
            BuildRequest(
                repository=str(repo),
                branchName="main",
                commitSha=git(repo, "rev-parse", "HEAD"),
                environment="PRODUCTION",
                executionBinding="engine",
                configVersion="1",
                deploymentPolicy="ON_SUCCESS",
                idempotencyKey=uuid4().hex,
            ),
            "integration",
        )
        job = runtime.jobs.claim(str(uuid4()), toolchain_version=runtime.toolchain, kinds=["BUILD_RUN"])
        with pytest.raises((ValueError, ExecutionError)):
            await RuntimeExecutor(settings, runtime.jobs, runtime.artifacts).execute(job)
        runtime.jobs.fail(job["job_id"], job["lease_token"], "INVALID_DEFINITION")
        assert deployments.reconcile(target, rejected.initial_deployment["generation"]).deployment_status == "FAILED"
        assert deployments.current(target).active_build_id == second.build_id
        assert (await query(first.build_id, mode="QUERY", metricResourceIds=[metric])).rows == first_result.rows
    finally:
        await runtime.close()
