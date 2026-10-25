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
from dbt_metricflow_service.application.queries import QueryService
from dbt_metricflow_service.bootstrap import Runtime
from dbt_metricflow_service.execution.build import BuildExecutor
from dbt_metricflow_service.execution.errors import ExecutionError
from dbt_metricflow_service.models.configuration import ExecutionCredential, ExecutionInstance
from dbt_metricflow_service.models.deployments import DeploymentKey
from dbt_metricflow_service.models.execution import ExecutionSnapshot
from dbt_metricflow_service.models.execution import SnapshotBuildRequest as BuildRequest
from dbt_metricflow_service.models.native import NativeProject
from dbt_metricflow_service.models.queries import AvailableDimensionsRequest, QueryRequest
from dbt_metricflow_service.platform.source import is_ancestor, remote_head
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
    # 单次 build 的输入是依赖已准备好的完整工程；服务不再替用户运行 deps。
    shutil.copytree(repo / "macros/local_pkg", repo / "dbt_packages/local_pkg")
    git(repo, "add", ".")
    git(repo, "add", "-f", "dbt_packages")
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
    builds = BuildService(store, runtime.toolchain)
    catalogs = CatalogService(store, runtime.artifacts)
    queries = QueryService(builds, runtime.queries.execute_query, runtime.queries.available_dimensions)
    deployments = DeploymentService(
        DeploymentStore(runtime.db),
        lambda repository, branch: remote_head(repository, branch, settings.temp_root),
        lambda repository, first, second: is_ancestor(repository, first, second, settings.temp_root),
        builds=builds,
        catalogs=catalogs,
    )
    target = DeploymentKey(repository=repository_url, branchName="main", environment=str(snapshot.environment_id))

    async def execute():
        job = runtime.jobs.claim(str(uuid4()), toolchain_version=runtime.toolchain)
        assert job
        result = await BuildExecutor(settings, runtime.jobs, runtime.artifacts).execute(job)
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
        await execute()
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
        }.issubset(paths)
        assert "target/catalog.json" not in paths
        assert not any(path.startswith(("target/compiled/", "target/run/")) for path in paths)
        assert not {
            "target/run_results.json",
            "target/sources.json",
            "target/publication_evidence.json",
            "target/publication_state.json",
        }.intersection(paths)
        assert yaml.safe_load(project_file.read_text(encoding="utf-8"))["target-path"] == "custom-target"
        resources = catalogs.list(first.build_id)
        metric = next(iter(resources["metrics"].values()))["name"]
        dataset = next(item["name"] for item in resources["nodes"].values() if item["resource_type"] == "model")
        result = await asyncio.to_thread(
            queries.available_dimensions,
            first.build_id,
            AvailableDimensionsRequest(metrics=[metric]),
            "integration",
        )
        dimensions = result.dimensions
        dimension = next(item.option_id for item in dimensions if item.dimension_type != "time")
        first_result = await query(first.build_id, mode="QUERY", metrics=[metric])
        assert Decimal(str(first_result.rows[0][0])) == Decimal("31.00")
        explain = await query(first.build_id, mode="EXPLAIN", metrics=[metric])
        assert explain.sql and "select" in explain.sql.lower()
        assert (await query(first.build_id, mode="PREVIEW", selector=dataset)).rows
        assert (
            await query(
                first.build_id, mode="DIMENSION_VALUES", metrics=[metric], dimensionOptionId=dimension
            )
        ).rows
        # 语义变更也执行完整构建，固定 schema 中的模型保持原表名。
        before = catalogs.list(first.build_id)
        yaml_file = next(path for path in (repo / "models").glob("*.yml") if "revenue" in path.read_text())
        yaml_file.write_text(yaml_file.read_text() + "\n# semantic documentation update\n")
        git(repo, "commit", "-am", "semantic-change")
        semantic = await build()
        after = catalogs.list(semantic.build_id)
        assert {item["relation_name"] for item in after["nodes"].values() if item["resource_type"] == "model"} == {
            item["relation_name"] for item in before["nodes"].values() if item["resource_type"] == "model"
        }
        completed = await asyncio.to_thread(
            queries.submit,
            first.build_id,
            QueryRequest(mode="QUERY", metrics=[metric]),
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
            str((await query(first.build_id, mode="QUERY", metrics=[metric])).rows[0][0])
        ) == Decimal("32.00")
        assert Decimal(
            str((await query(second.build_id, mode="QUERY", metrics=[metric])).rows[0][0])
        ) == Decimal("32.00")
        # dbt 自己报告解析失败，服务记录失败且不切换当前部署。
        invalid = repo / "models" / "invalid.sql"
        invalid.write_text("{{ undefined_build_macro() }}")
        git(repo, "add", ".")
        git(repo, "commit", "-m", "invalid-candidate")
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
        job = runtime.jobs.claim(str(uuid4()), toolchain_version=runtime.toolchain)
        with pytest.raises((ValueError, ExecutionError)):
            await BuildExecutor(settings, runtime.jobs, runtime.artifacts).execute(job)
        runtime.jobs.fail(job.job_id, job.lease_token, "INVALID_DEFINITION")
        assert deployments.reconcile(target, rejected.initial_deployment.generation).deployment_status == "FAILED"
        assert deployments.current(target).active_build_id == second.build_id
        assert Decimal(
            str((await query(first.build_id, mode="QUERY", metrics=[metric])).rows[0][0])
        ) == Decimal("32.00")
        # 后续构建保留共享物理表、历史目录和源码，历史构建仍可查询。
        first_run = store.get(first.build_id).run_id
        first_schema = runtime.jobs.get(first_run).schema_name
        with runtime.db.transaction() as connection:
            assert (
                connection.exec_driver_sql(
                    "SELECT 1 FROM pg_namespace WHERE nspname=%s",
                    (first_schema,),
                ).first()
                is not None
            )
        assert catalogs.list(first.build_id)["metrics"]
        assert catalogs.resource(first.build_id, next(key for key, item in resources["nodes"].items()
            if item["name"] == dataset)).source.content
        assert (await query(first.build_id, mode="QUERY", metrics=[metric])).rows
        assert (await query(second.build_id, mode="QUERY", metrics=[metric])).rows
    finally:
        with runtime.db.transaction() as connection:
            connection.exec_driver_sql(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
        await runtime.close()
