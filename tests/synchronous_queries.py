"""只替换外部引擎结果，复用真实同步执行、租约与完成事务。"""

import asyncio
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from dbt_metricflow_service.runtime.executor import ExecutionResult
from dbt_metricflow_service.runtime.worker import Worker
from dbt_metricflow_service.settings import Settings

NATIVE_OPTIONS = {
    "metrics": [{"name": "orders"}],
    "dimensions": [
        {"token": "order__region", "name": "region", "type": "categorical"},
        {"token": "return__region", "name": "region", "type": "categorical"},
    ],
    "timeDimensions": [{"token": "metric_time__month", "granularity": "month"}],
    "allowedFilters": ["=", "IN"],
}
QUERY_RESULT = {"sql": "select 1", "columns": [], "rows": [], "truncated": False}


def inline_executor(jobs, toolchain, *, query_result=None):
    runtime = SimpleNamespace(
        jobs=jobs, db=jobs.db, instance_id=uuid4(), toolchain=toolchain,
        artifacts=SimpleNamespace(seal=lambda *args: None),
        settings=Settings(Path("."), 60, 1024),
    )
    worker = Worker(runtime)

    async def execute(job):
        payload = NATIVE_OPTIONS if job["kind"] == "QUERY_OPTIONS" else (
            QUERY_RESULT if query_result is None else query_result
        )
        return ExecutionResult(payload)

    worker.executor.execute = execute
    return lambda job_id: asyncio.run(worker.execute_now(job_id))
