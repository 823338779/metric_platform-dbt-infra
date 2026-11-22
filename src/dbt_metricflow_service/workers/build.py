"""有界 PostgreSQL worker，所有运行权限由数据库租约决定。"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Final
from uuid import UUID

from sqlalchemy.exc import DBAPIError as DatabaseError
from sqlalchemy.exc import TimeoutError as PoolTimeout

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.constants import COMMAND_FAILED as ERROR_COMMAND_FAILED
from dbt_metricflow_service.execution.build import BuildExecutor
from dbt_metricflow_service.execution.errors import ExecutionError
from dbt_metricflow_service.platform.execution_profile import ExecutionConfigurationError
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.records import JobErrorDetail, LeasedBuild

if TYPE_CHECKING:
    from dbt_metricflow_service.application.deployments import DeploymentService
    from dbt_metricflow_service.execution.models import ExecutionResult


logger = logging.getLogger(__name__)
POLL_SECONDS = 0.25
INPUT_LOST = strings.INPUT_LOST
INTERRUPTED = strings.EXECUTION_OUTCOME_UNKNOWN
WORKER_FAILED = strings.WORKER_FAILED
FAILURE_LOG: Final[str] = "Runtime worker operation failed: %s"
EXTERNAL_OUTCOME_UNKNOWN = strings.EXTERNAL_OUTCOME_UNKNOWN_ALIAS


class Worker:
    """每个槽位仅持有一个 attempt；故障后由共享恢复扫描处理过期租约。"""

    def __init__(
        self, settings: Settings, jobs: JobStore, artifacts: ArtifactStore, *,
        instance_id: UUID, toolchain: str, deployments: DeploymentService | None = None,
    ) -> None:
        self.settings = settings
        self.jobs = jobs
        self.artifacts = artifacts
        self.instance_id = instance_id
        self.toolchain = toolchain
        self.deployments = deployments
        self.executor = BuildExecutor(settings, jobs, artifacts)
        self._tasks: list[asyncio.Task] = []
        self._stopping = False

    def start(self) -> None:
        self._tasks = [asyncio.create_task(self._slot()) for _ in range(self.settings.worker_concurrency)]
        self._tasks.append(asyncio.create_task(self._maintain()))

    async def close(self) -> None:
        # 进程内引擎取消后等待调用退出；数据库失联时等待租约恢复。
        self._stopping = True
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def _maintain(self) -> None:
        while not self._stopping:
            try:
                await asyncio.to_thread(self.jobs.recover)
                if self.deployments is not None:
                    # 部署单独轮询持久化的待处理意图；进程重启后也能继续核对构建结果和远端分支。
                    await asyncio.to_thread(self.deployments.reconcile_pending)
            except (DatabaseError, PoolTimeout, RuntimeError) as error:
                logger.warning(FAILURE_LOG, type(error).__name__)
            await asyncio.sleep(self.settings.heartbeat_seconds)

    async def _slot(self) -> None:
        while not self._stopping:
            try:
                # 从已提交的队列中领取与本实例兼容的任务。claim 返回本次尝试的租约，后续状态写入都要核对它。
                job = await asyncio.to_thread(
                    self.jobs.claim, str(self.instance_id),
                    toolchain_version=self.toolchain,
                )
                if job is not None:
                    await self._execute(job)
                    continue
            except (DatabaseError, PoolTimeout, RuntimeError) as error:
                # 不记录连接异常原文，避免 DSN 或服务配置进入日志。
                logger.warning(FAILURE_LOG, type(error).__name__)
            await asyncio.sleep(POLL_SECONDS)

    async def _execute(self, job: LeasedBuild) -> None:
        # 构建期间并行续租；心跳确认失去执行权时会取消执行，防止旧 worker 提交结果。
        execution = asyncio.create_task(self.executor.execute(job))
        heartbeat = asyncio.create_task(self._heartbeat(job, execution))
        try:
            result = await execution
            # 在同一事务中封存产物，并在构建记录上写入成功状态和产物 ID。
            await asyncio.to_thread(
                self.jobs.finish, job.build_id, job.lease_token,
                output_set_id=result.output_set_id, seal=self.artifacts.seal,
            )
        except asyncio.CancelledError:
            execution.cancel()
            await asyncio.gather(execution, return_exceptions=True)
            # 停机或失租时等待引擎退出；未确认的外部写入继续保留引用保护。
            external = await asyncio.to_thread(self.jobs.external_started, job.attempt_id)
            await self._fail(
                job, INTERRUPTED, detail=JobErrorDetail(external_outcome_unknown=external), stopped=not external,
            )
            if self._stopping:
                raise
        except ExecutionConfigurationError as error:
            # 保留具体配置错误码，并按已记录的执行阶段标记外部写入结果是否未知。
            external = await asyncio.to_thread(self.jobs.external_started, job.attempt_id)
            await self._fail(
                job, error.code, detail=JobErrorDetail(external_outcome_unknown=external), stopped=not external,
            )
        except ExecutionError as error:
            code = INTERRUPTED if error.code == ERROR_COMMAND_FAILED and not error.stopped else error.code
            detail = error.payload.model_copy()
            detail.external_outcome_unknown = not error.stopped
            await self._fail(job, code, detail=detail, stopped=error.stopped)
        except Exception as error:
            # 不保存任意异常文本，其中可能含凭据或临时输入。
            external = await asyncio.to_thread(self.jobs.external_started, job.attempt_id)
            await self._fail(job, WORKER_FAILED,
                             detail=JobErrorDetail(type=type(error).__name__, external_outcome_unknown=external),
                             stopped=not external)
            logger.warning(FAILURE_LOG, type(error).__name__)
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)

    async def _fail(self, job: LeasedBuild, code: str, *, detail: JobErrorDetail | None=None, stopped: bool=True) -> None:
        try:
            await asyncio.to_thread(
                self.jobs.fail, job.build_id, job.lease_token, code, detail, stopped=stopped,
            )
        except (DatabaseError, PoolTimeout, RuntimeError):
            # 无法提交时不伪造终态，后续恢复扫描根据已持久阶段作出决定。
            logger.warning(FAILURE_LOG, code)

    async def _heartbeat(self, job: LeasedBuild, execution: asyncio.Task[ExecutionResult]) -> None:
        last_confirmed = time.monotonic()
        while not execution.done():
            await asyncio.sleep(self.settings.heartbeat_seconds)
            try:
                # 仅续租决定执行权限，不再轮询用户取消意图。
                valid = await asyncio.to_thread(
                    self.jobs.heartbeat, job.build_id, job.lease_token,
                )
                if not valid:
                    execution.cancel()
                    return
                last_confirmed = time.monotonic()
            except (DatabaseError, PoolTimeout, RuntimeError):
                if time.monotonic() - last_confirmed >= self.settings.lease_seconds / 2:
                    execution.cancel()
                    return
