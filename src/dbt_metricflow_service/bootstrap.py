"""服务组合入口：创建依赖并管理后台 worker 生命周期。"""
from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from uuid import uuid4

from dbt_metricflow_service.execution.query import QueryExecutor
from dbt_metricflow_service.execution.toolchain import current_toolchain
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.postgres import Database

if TYPE_CHECKING:
    from dbt_metricflow_service.application.deployments import DeploymentService


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
        self.queries = QueryExecutor(settings, self.artifacts)
        self.worker = None

    async def start(self, *, deployments: DeploymentService | None = None) -> None:
        # 本地目录只保存当前执行，可由空目录启动。
        from dbt_metricflow_service.workers.build import Worker
        self.settings.temp_root.mkdir(parents=True, exist_ok=True)
        self.worker = Worker(
            self.settings, self.jobs, self.artifacts, instance_id=self.instance_id,
            toolchain=self.toolchain, deployments=deployments,
        )
        self.worker.start()


    async def close(self) -> None:
        if self.worker is not None:
            await self.worker.close()
        await asyncio.to_thread(self.db.close)
