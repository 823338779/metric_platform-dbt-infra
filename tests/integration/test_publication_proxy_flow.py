"""无外部业务系统参与的真实构建、历史读取和四种查询。"""

import asyncio
import os
import shutil
from decimal import Decimal
from uuid import uuid4

import pytest
import yaml
from psycopg2 import sql
from sqlalchemy import select

from dbt_metricflow_service.application.builds import BuildService
from dbt_metricflow_service.application.catalog import CatalogService
from dbt_metricflow_service.application.deployments import DeploymentService
from dbt_metricflow_service.application.errors import ServiceError
from dbt_metricflow_service.application.queries import QueryService
from dbt_metricflow_service.models.configuration import ExecutionCredential, ExecutionInstance
from dbt_metricflow_service.models.deployments import DeploymentKey
from dbt_metricflow_service.models.execution import ExecutionSnapshot
from dbt_metricflow_service.models.execution import SnapshotBuildRequest as BuildRequest
from dbt_metricflow_service.models.native import NativeProject
from dbt_metricflow_service.models.queries import AvailableDimensionsRequest, QueryRequest
from dbt_metricflow_service.platform.source import is_ancestor, remote_head
from dbt_metricflow_service.runtime.executor import ExecutionError, RuntimeExecutor
from dbt_metricflow_service.runtime.service import Runtime
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.builds import BuildStore
from dbt_metricflow_service.storage.deployments import DeploymentStore
from dbt_metricflow_service.storage.entities import ArtifactFile
from tests.integration.helpers import FIXTURE, git


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
    project_config = NativeProject.model_validate(yaml.safe_load(project_file.read_text(encoding="utf-8")))
    project_config.target_path = "custom-target"
    project_file.write_text(yaml.safe_dump(project_config.native_details()), encoding="utf-8")
    git(repo, "init", "-b", "main")
    git(repo, "config", "user.name", "Test")
    git(repo, "config", "user.email", "test@example.invalid")
    git(repo, "add", ".")
    git(repo, "commit", "-m", "fixture")
    repository_url = "https://fixture.invalid/metrics.git"
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", f"url.{repo.as_posix()}.insteadOf")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", repository_url)
    profiles = tmp_path / "profiles"
    profiles.mkdir()
    host, port = os.environ["PLATFORM_TEST_PGHOST"], int(os.environ["PLATFORM_TEST_PGPORT"])
    monkeypatch.setenv("DBT_ENV_SECRET_FIXTURE_USER", os.environ["PLATFORM_TEST_PGUSER"])
    monkeypatch.setenv("DBT_ENV_SECRET_FIXTURE_PASSWORD", os.environ["PLATFORM_TEST_PGPASSWORD"])
    schema = "fixed_test_" + uuid4().hex
    snapshot = ExecutionSnapshot.model_validate(
        {
            "environmentId": str(uuid4()),
            "configVersion": 1,
            "connection": {"type": "postgres", "instanceId": "fixture", "host": host, "port": port},
            "credentialRef": "fixture",
            "catalog": os.environ["PLATFORM_TEST_PGDATABASE"],
            "schema": schema,
            "threads": 2,
            "businessTimezone": "UTC",
        }
    )
    settings = Settings(
        profiles,
        180,
        1048576,
        database_url=os.environ["SERVICE_TEST_DATABASE_URL"],
        temp_root=tmp_path / "runtime",
        toolchain_version=uuid4().hex,
        execution_instances={"fixture": ExecutionInstance(type="postgres", hosts=[host], port=port)},
        execution_credentials={
            "fixture": ExecutionCredential(
                user_env="DBT_ENV_SECRET_FIXTURE_USER", password_env="DBT_ENV_SECRET_FIXTURE_PASSWORD",
            )
        },
    )
    runtime = Runtime(settings)
    runtime.db.initialize()
    store = BuildStore(runtime.db)
    builds = BuildService(store, runtime.toolchain, 180)
    catalogs = CatalogService(store, runtime.artifacts)
    queries = QueryService(builds, catalogs, runtime.execute_query, runtime.available_dimensions)
    deployments = DeploymentService(
        DeploymentStore(runtime.db),
        lambda repository, branch: remote_head(repository, branch, settings.temp_root),
        lambda repository, first, second: is_ancestor(repository, first, second, settings.temp_root),
        builds=builds,
        catalogs=catalogs,
    )
    target = DeploymentKey(repository=repository_url, branchName="main", environment=str(snapshot.environment_id))

    async def execute(kind):
        job = runtime.jobs.claim(str(uuid4()), toolchain_version=runtime.toolchain, kinds=[kind])
        assert job
        result = await RuntimeExecutor(settings, runtime.jobs, runtime.artifacts).execute(job)
        assert runtime.jobs.finish(job.job_id, job.lease_token, output_set_id=result.output_set_id)
        return result

    async def build():
        view = builds.submit_snapshot(
            BuildRequest(
                repository=repository_url,
                branchName="main",
                commitSha=git(repo, "rev-parse", "HEAD"),
                execution=snapshot,
                deploymentPolicy="ON_SUCCESS",
                idempotencyKey=uuid4().hex,
            ),
            "integration",
        )
        await execute("BUILD_RUN")
        assert builds.snapshot_view(view.build_id).build_status == "SUCCEEDED"
        assert deployments.reconcile(target, view.initial_deployment.generation).deployment_status == "DEPLOYED"
        return view

    async def query(build_id, **fields):
        result = await asyncio.to_thread(queries.submit, build_id, QueryRequest(**fields), "integration")
        assert result.build_id == build_id
        return result

    try:
        first = await build()
        with runtime.db.session() as session:
            paths = set(
                session.scalars(
                    select(ArtifactFile.relative_path).where(
                        ArtifactFile.set_id == store.get(first.build_id).output_set_id,
                    )
                )
            )
        assert {
            "target/manifest.json",
            "target/semantic_manifest.json",
            "target/catalog.json",
            "target/published_catalog.json",
        }.issubset(paths)
        assert not any(path.startswith(("target/compiled/", "target/run/")) for path in paths)
        assert not {
            "target/run_results.json",
            "target/sources.json",
            "target/publication_evidence.json",
            "target/publication_state.json",
        }.intersection(paths)
        assert yaml.safe_load(project_file.read_text(encoding="utf-8"))["target-path"] == "custom-target"
        resources = catalogs.list(first.build_id).items
        metric = next(item.resource_id for item in resources if item.kind == "METRIC")
        dataset = next(item.resource_id for item in resources if "PREVIEW" in item.capabilities)
        result = await asyncio.to_thread(
            queries.available_dimensions,
            first.build_id,
            AvailableDimensionsRequest(metricResourceIds=[metric]),
            "integration",
        )
        dimensions = result.dimensions
        dimension = next(item.option_id for item in dimensions if item.dimension_type != "time")
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
        # 语义变更也执行完整构建，固定 schema 中的模型保持原表名。
        before = catalogs.read(first.build_id)[1]
        yaml_file = next(path for path in (repo / "models").glob("*.yml") if "revenue" in path.read_text())
        yaml_file.write_text(yaml_file.read_text() + "\n# semantic documentation update\n")
        git(repo, "commit", "-am", "semantic-change")
        semantic = await build()
        after = catalogs.read(semantic.build_id)[1]
        assert {item.relation.identifier for item in after.relation_bindings} == {
            item.relation.identifier for item in before.relation_bindings
        }
        assert all(item.mode == "BUILT" for item in after.relation_bindings)
        completed = await asyncio.to_thread(
            queries.submit,
            first.build_id,
            QueryRequest(mode="QUERY", metricResourceIds=[metric]),
            "integration",
        )
        # 新源码覆盖同名表，旧构建查询同样读取当前数据。
        path = repo / "models" / "orders.sql"
        path.write_text(path.read_text().replace("10.25", "11.25"))
        git(repo, "commit", "-am", "new-data")
        second = await build()
        assert second.build_id != first.build_id
        assert deployments.current(target).active_build_id == second.build_id
        assert completed.rows == first_result.rows
        assert Decimal(
            str((await query(first.build_id, mode="QUERY", metricResourceIds=[metric])).rows[0][0])
        ) == Decimal("32.00")
        assert Decimal(
            str((await query(second.build_id, mode="QUERY", metricResourceIds=[metric])).rows[0][0])
        ) == Decimal("32.00")
        # 非法模板必须在外部写入前失败，不能破坏活动部署或旧物理数据。
        unsafe = repo / "macros" / "unsafe.sql"
        unsafe.write_text("{{ run_query('drop table protected') }}")
        git(repo, "add", ".")
        git(repo, "commit", "-m", "unsafe-candidate")
        rejected = builds.submit_snapshot(
            BuildRequest(
                repository=repository_url,
                branchName="main",
                commitSha=git(repo, "rev-parse", "HEAD"),
                execution=snapshot,
                deploymentPolicy="ON_SUCCESS",
                idempotencyKey=uuid4().hex,
            ),
            "integration",
        )
        job = runtime.jobs.claim(str(uuid4()), toolchain_version=runtime.toolchain, kinds=["BUILD_RUN"])
        with pytest.raises((ValueError, ExecutionError)):
            await RuntimeExecutor(settings, runtime.jobs, runtime.artifacts).execute(job)
        runtime.jobs.fail(job.job_id, job.lease_token, "INVALID_DEFINITION")
        assert deployments.reconcile(target, rejected.initial_deployment.generation).deployment_status == "FAILED"
        assert deployments.current(target).active_build_id == second.build_id
        assert Decimal(
            str((await query(first.build_id, mode="QUERY", metricResourceIds=[metric])).rows[0][0])
        ) == Decimal("32.00")
        # 清理结束旧构建查询生命周期，保留共享物理表、历史目录和源码。
        first_run = store.get(first.build_id).run_id
        first_schema = runtime.jobs.get(first_run).schema_name
        runtime.jobs.reserve_cleanup(first_run)
        await execute("RUN_CLEANUP")
        with runtime.db.transaction() as connection:
            assert (
                connection.exec_driver_sql(
                    "SELECT 1 FROM pg_namespace WHERE nspname=%s",
                    (first_schema,),
                ).first()
                is not None
            )
        assert catalogs.list(first.build_id).items
        assert catalogs.resource(first.build_id, dataset, "source").content
        with pytest.raises(ServiceError):
            await query(first.build_id, mode="QUERY", metricResourceIds=[metric])
        assert (await query(second.build_id, mode="QUERY", metricResourceIds=[metric])).rows
    finally:
        with runtime.db.transaction() as connection:
            connection.exec_driver_sql(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
        await runtime.close()
