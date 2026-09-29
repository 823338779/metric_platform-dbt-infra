import asyncio
import os
from uuid import uuid4

import pytest

from dbt_metricflow_service.settings import Settings


async def test_two_workers_execute_persisted_task_once(tmp_path, monkeypatch):
    dsn = os.getenv("SERVICE_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("需要独立 PostgreSQL 测试库")
    from dbt_metricflow_service.runtime import Runtime
    from dbt_metricflow_service.runtime_execution import ExecutionResult, RuntimeExecutor
    from dbt_metricflow_service.worker import Worker
    runtimes = [Runtime(Settings(
        projects_root=tmp_path, profiles_dir=tmp_path, command_timeout_seconds=30,
        max_output_bytes=1024, database_url=dsn, temp_root=tmp_path / str(i),
        toolchain_version=str(uuid4()),
    )) for i in range(2)]
    runtimes[1].toolchain = runtimes[0].toolchain
    project = "worker_" + uuid4().hex
    runtimes[0].jobs.register_project(project)
    row = runtimes[0].jobs.reserve("MF_COMMAND", project, {}, toolchain_version=runtimes[0].toolchain)
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
        assert runtimes[1].cli_record(row["job_id"]).stdout == "shared result"
        assert executions == [row["job_id"]]
    finally:
        for worker in workers:
            await worker.close()
        for runtime in runtimes:
            runtime.db.close()
