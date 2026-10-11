"""PostgreSQL 运行时的受理与读取边界，不依赖实例本地项目目录。"""
from __future__ import annotations

import asyncio
import hashlib
import json
from importlib.metadata import version
from pathlib import Path
from typing import TYPE_CHECKING, cast
from uuid import uuid4

from dbt_metricflow_service.application.errors import ServiceError
from dbt_metricflow_service.execution.embedded import run_serialized
from dbt_metricflow_service.execution.metricflow import MetricFlowRequest, MetricFlowResult, invoke_metricflow
from dbt_metricflow_service.execution.models import JobStatus
from dbt_metricflow_service.models.payloads import JsonObject
from dbt_metricflow_service.runtime.executor import build_engine_environment
from dbt_metricflow_service.runtime.workspace import materialized_workspace
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.jobs import CleanupBlocked, JobStore
from dbt_metricflow_service.storage.postgres import Database
from dbt_metricflow_service.storage.records import StoredBuild

if TYPE_CHECKING:
    from dbt_metricflow_service.application.deployments import DeploymentService


TOOLCHAIN_PACKAGES = ("dbt-core", "metricflow", "dbt-starrocks", "dbt-duckdb", "dbt-postgres")


def current_toolchain() -> str:
    """安装版本和服务代码共同决定执行兼容标识，避免不同代码误领旧任务。"""
    digest = hashlib.sha256()
    for package in TOOLCHAIN_PACKAGES:
        digest.update(f"{package}={version(package)}\n".encode())
    # runtime 已归入子包，兼容标识仍必须覆盖整个服务源码。
    source_root = Path(__file__).resolve().parents[1]
    for source in sorted(source_root.rglob("*.py")):
        digest.update(source.relative_to(source_root).as_posix().encode())
        digest.update(source.read_bytes().replace(b"\r\n", b"\n"))
    return digest.hexdigest()


class Runtime:
    def __init__(self, settings: Settings) -> None:
        # 数据库 schema 必须预先通过管理命令安装，启动不得修改其他实例任务。
        self.settings = settings
        self.instance_id = uuid4()
        self.toolchain = settings.toolchain_version or current_toolchain()
        self.db = Database(settings.database_url)
        self.db.check()
        self.jobs = JobStore(self.db, lease_seconds=settings.lease_seconds,
                             max_diagnostic_bytes=settings.max_output_bytes)
        self.artifacts = ArtifactStore(self.db, max_file_bytes=settings.max_artifact_file_bytes,
                                       max_set_bytes=settings.max_artifact_bytes)
        self.worker = None

    def execute_query(self, build: StoredBuild, payload: JsonObject) -> JsonObject:
        """直接执行固定构建的查询，输入、结果和诊断均不落库。"""
        async def before_start() -> None:
            # 无持久租约；执行线程内的 query_run 会在引擎调用前校验并保护构建。
            return None

        def invoke() -> MetricFlowResult:
            with self.jobs.query_run(cast(str, build["run_id"])) as parent:
                with materialized_workspace(
                    self.artifacts, cast(str, build["output_set_id"]), self.settings.temp_root,
                    str(uuid4()), str(uuid4()),
                ) as project:
                    environment = build_engine_environment(
                        project, self.settings.profiles_dir,
                        cast(str, parent["schema_name"]), cast(str, parent["profile_binding_id"]),
                    )
                    return invoke_metricflow(
                        parent["project_id"], MetricFlowRequest(environment, payload),
                        self.settings.max_output_bytes,
                    )

        try:
            # 先取得引擎串行槽位，再占用清理保护连接，避免排队请求耗尽连接池。
            result, timed_out = asyncio.run(run_serialized(invoke, self.settings.command_timeout_seconds, before_start))
        except CleanupBlocked as error:
            raise ServiceError("PHYSICAL_OBJECTS_REMOVED", "build is being cleaned", 410,
                               build_id=build["build_id"]) from error
        if timed_out or result.record.status == JobStatus.TIMED_OUT:
            raise ServiceError("COMMAND_TIMEOUT", "query execution timed out", 504, build_id=build["build_id"])
        if result.record.status != JobStatus.SUCCEEDED:
            code = result.error_code or "COMMAND_FAILED"
            raise ServiceError(code, "query execution failed", 422 if code == "INVALID_QUERY" else 503,
                               build_id=build["build_id"])
        value = result.payload
        if not isinstance(value, dict):
            raise ServiceError("RESULT_INVALID", "query returned an invalid result", 503, build_id=build["build_id"])
        size = 0
        for chunk in json.JSONEncoder(ensure_ascii=False).iterencode(value):
            size += len(chunk.encode())
            if size > self.settings.max_result_bytes:
                raise ServiceError("RESULT_TOO_LARGE", "query result exceeds the size limit", 422,
                                   build_id=build["build_id"])
        return value


    async def start(self, *, deployments: DeploymentService | None = None) -> None:
        # 本地目录只保存当前执行，可由空目录启动。
        from dbt_metricflow_service.runtime.worker import Worker
        self.settings.temp_root.mkdir(parents=True, exist_ok=True)
        self.worker = Worker(self, deployments=deployments)
        self.worker.start()


    async def close(self) -> None:
        if self.worker is not None:
            await self.worker.close()
        await asyncio.to_thread(self.db.close)


