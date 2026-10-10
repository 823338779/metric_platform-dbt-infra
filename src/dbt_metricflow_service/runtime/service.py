"""PostgreSQL 运行时的受理与读取边界，不依赖实例本地项目目录。"""
from __future__ import annotations

import asyncio
import hashlib
from importlib.metadata import version
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.postgres import Database

if TYPE_CHECKING:
    from dbt_metricflow_service.application.deployments import DeploymentService


BUILD = "BUILD_RUN"
QUERY = "METRIC_QUERY"
OPTIONS = "QUERY_OPTIONS"
SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"
ACTIVE = "ACTIVE"
CLEANED = "CLEANED"
READ_ONLY = "READ_ONLY"
PREPARATION_ONLY = "PREPARATION_ONLY"
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
    # HTTP 装配入口注入的部署协调器，纯 Runtime 使用时可能尚未绑定。
    deployments: DeploymentService

    def __init__(self, settings: Settings) -> None:
        # 数据库 schema 必须预先通过管理命令安装，启动不得修改其他实例任务。
        self.settings = settings
        self.instance_id = uuid4()
        self.toolchain = settings.toolchain_version or current_toolchain()
        self.db = Database(settings.database_url)
        self.db.check()
        self.jobs = JobStore(self.db, lease_seconds=settings.lease_seconds,
                             max_result_bytes=settings.max_result_bytes,
                             max_diagnostic_bytes=settings.max_output_bytes)
        self.artifacts = ArtifactStore(self.db, max_file_bytes=settings.max_artifact_file_bytes,
                                       max_set_bytes=settings.max_artifact_bytes)
        self.worker = None


    async def start(self) -> None:
        # 本地目录只保存当前执行，可由空目录启动。
        from dbt_metricflow_service.runtime.worker import Worker
        self.settings.temp_root.mkdir(parents=True, exist_ok=True)
        self.worker = Worker(self)
        self.worker.start()


    async def close(self) -> None:
        if self.worker is not None:
            await self.worker.close()
        await asyncio.to_thread(self.db.close)


