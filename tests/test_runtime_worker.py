import asyncio
import os
from uuid import uuid4

import pytest

from dbt_metricflow_service.settings import Settings
from tests.test_publication_storage import store as store


async def test_runtime_reconciles_explicit_deployments(store, tmp_path):
    from dbt_metricflow_service.application.catalog import CatalogService
    from dbt_metricflow_service.application.deployments import DeploymentService
    from dbt_metricflow_service.models.deployments import DeploymentKey, DeploymentRequest
    from dbt_metricflow_service.runtime.service import Runtime
    from dbt_metricflow_service.storage.deployments import DeploymentStore
    from tests.test_v3_catalog_queries import ready_build

    builds, build, jobs, artifacts = ready_build(store, tmp_path)
    deployments = DeploymentService(
        DeploymentStore(store.db), lambda *_: build.commit_sha,
        builds=builds, catalogs=CatalogService(builds.store, artifacts),
    )
    deployments.submit(DeploymentRequest(
        buildId=build.build_id, branchName=build.branch_name,
        expectedTargetVersion=0, idempotencyKey=uuid4().hex,
    ), "worker-test")
    key = DeploymentKey(repository=build.repository, environment=build.environment, branchName=build.branch_name)
    runtime = Runtime(Settings(
        profiles_dir=tmp_path, database_url=os.environ["SERVICE_TEST_DATABASE_URL"],
        command_timeout_seconds=30, max_output_bytes=1024,
        temp_root=tmp_path / "worker", toolchain_version=build.toolchain_version, heartbeat_seconds=1,
    ))
    try:
        await runtime.start(deployments=deployments)
        async with asyncio.timeout(5):
            while deployments.store.current(key)["active_build_id"] is None:
                await asyncio.sleep(0.02)
        assert deployments.store.current(key)["active_build_id"] == str(build.build_id)
    finally:
        await runtime.close()


async def test_two_workers_execute_persisted_task_once(tmp_path, monkeypatch):
    dsn = os.getenv("SERVICE_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("需要独立 PostgreSQL 测试库")
    from dbt_metricflow_service.runtime.executor import ExecutionResult, RuntimeExecutor
    from dbt_metricflow_service.runtime.service import Runtime
    from dbt_metricflow_service.runtime.worker import Worker
    runtimes = [Runtime(Settings(
         profiles_dir=tmp_path, command_timeout_seconds=30,
        max_output_bytes=1024, database_url=dsn, temp_root=tmp_path / str(i),
        toolchain_version=str(uuid4()),
    )) for i in range(2)]
    runtimes[1].toolchain = runtimes[0].toolchain
    project = "worker_" + uuid4().hex
    runtimes[0].jobs.register_project(project)
    row = runtimes[0].jobs.reserve("QUERY_OPTIONS", project, {}, toolchain_version=runtimes[0].toolchain)
    executions = []

    # 只隔离外部引擎，认领、心跳和提交均经过真实数据库。
    async def execute(self, job, resources=None):
        executions.append(job["job_id"])
        await asyncio.sleep(0.05)
        return ExecutionResult({"stdout": "shared result"})
    monkeypatch.setattr(RuntimeExecutor, "execute", execute)
    workers = [Worker(runtime) for runtime in runtimes]
    for worker in workers:
        worker.start()
    try:
        for _ in range(100):
            if runtimes[0].jobs.get(row["job_id"])["status"] == "SUCCEEDED":
                break
            await asyncio.sleep(0.05)
        assert runtimes[1].jobs.result(row["job_id"])["payload_json"]["stdout"] == "shared result"
        assert executions == [row["job_id"]]
    finally:
        for worker in workers:
            await worker.close()
        for runtime in runtimes:
            runtime.db.close()
