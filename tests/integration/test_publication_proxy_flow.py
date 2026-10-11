"""无外部业务系统参与的真实构建、历史读取和四种查询。"""

import asyncio
import os
import shutil
from decimal import Decimal
from uuid import uuid4

import pytest
import yaml
from sqlalchemy import select

from dbt_metricflow_service.application.builds import BuildService
from dbt_metricflow_service.application.catalog import CatalogService
from dbt_metricflow_service.application.deployments import DeploymentService
from dbt_metricflow_service.application.errors import ServiceError
from dbt_metricflow_service.application.queries import QueryService
from dbt_metricflow_service.models.builds import BuildRequest
from dbt_metricflow_service.models.deployments import DeploymentKey
from dbt_metricflow_service.models.queries import AvailableDimensionsRequest, QueryRequest
from dbt_metricflow_service.platform.source import is_ancestor, remote_head
from dbt_metricflow_service.runtime.executor import ExecutionError, RuntimeExecutor
from dbt_metricflow_service.runtime.service import Runtime
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.builds import BuildStore
from dbt_metricflow_service.storage.deployments import DeploymentStore
from dbt_metricflow_service.storage.entities import ArtifactFile
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
    queries = QueryService(builds, catalogs, runtime.execute_query)
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
        assert runtime.jobs.finish(
            job["job_id"], job["lease_token"], output_set_id=result.output_set_id
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
        result = await asyncio.to_thread(
            queries.submit, build_id, QueryRequest(**fields), "integration"
        )
        assert result.build_id == build_id
        return result

    try:
        first = await build()
        with runtime.db.session() as session:
            paths = set(session.scalars(select(ArtifactFile.relative_path).where(
                ArtifactFile.set_id == store.get(first.build_id)["output_set_id"],
            )))
        assert {"target/manifest.json", "target/semantic_manifest.json", "target/catalog.json",
                "target/published_catalog.json"}.issubset(paths)
        assert not any(path.startswith(("target/compiled/", "target/run/")) for path in paths)
        assert not {"target/run_results.json", "target/sources.json", "target/publication_evidence.json",
                    "target/publication_state.json"}.intersection(paths)
        assert yaml.safe_load(project_file.read_text(encoding="utf-8"))["target-path"] == "custom-target"
        resources = catalogs.list(first.build_id).model_dump(mode="json", by_alias=True)["items"]
        metric = next(item["resourceId"] for item in resources if item["kind"] == "METRIC")
        dataset = next(item["resourceId"] for item in resources if "PREVIEW" in item["capabilities"])
        result = await asyncio.to_thread(
            queries.available_dimensions, first.build_id,
            AvailableDimensionsRequest(metricResourceIds=[metric]), "integration"
        )
        dimensions = result.dimensions
        dimension = next(item["optionId"] for item in dimensions if item["dimensionType"] != "time")
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
        before = catalogs.read(first.build_id)[1]
        yaml_file = next(path for path in (repo / "models").glob("*.yml") if "revenue" in path.read_text())
        yaml_file.write_text(yaml_file.read_text() + "\n# semantic documentation update\n")
        git(repo, "commit", "-am", "semantic-change")
        semantic = await build()
        after = catalogs.read(semantic.build_id)[1]
        assert {item["relation"]["identifier"] for item in after["relationBindings"]} != {
            item["relation"]["identifier"] for item in before["relationBindings"]
        }
        assert all(item["mode"] == "BUILT" for item in after["relationBindings"])
        completed = await asyncio.to_thread(
            queries.submit, first.build_id,
            QueryRequest(mode="QUERY", metricResourceIds=[metric]),
            "integration",
        )
        # 新源码生成独立物理对象；新消费者仍可对旧构建提交查询。
        path = repo / "models" / "orders.sql"
        path.write_text(path.read_text().replace("10.25", "11.25"))
        git(repo, "commit", "-am", "new-data")
        second = await build()
        assert second.build_id != first.build_id
        assert deployments.current(target).active_build_id == second.build_id
        assert completed.rows == first_result.rows
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
        # 精简快照仍能清理非活动构建；历史目录和源码不随物理对象删除。
        first_run = store.get(first.build_id)["run_id"]
        first_schema = runtime.jobs.get(first_run)["schema_name"]
        runtime.jobs.reserve_cleanup(first_run)
        await execute("RUN_CLEANUP")
        with runtime.db.transaction() as connection:
            assert connection.exec_driver_sql(
                "SELECT 1 FROM pg_namespace WHERE nspname=%s", (first_schema,),
            ).first() is None
        assert catalogs.list(first.build_id).items
        assert catalogs.resource(first.build_id, dataset, "source")["content"]
        with pytest.raises(ServiceError):
            await query(first.build_id, mode="QUERY", metricResourceIds=[metric])
        assert (await query(second.build_id, mode="QUERY", metricResourceIds=[metric])).rows
    finally:
        await runtime.close()
