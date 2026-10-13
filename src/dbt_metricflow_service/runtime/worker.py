"""有界 PostgreSQL worker，所有运行权限由数据库租约决定。"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Final, cast

from sqlalchemy.exc import DBAPIError as DatabaseError
from sqlalchemy.exc import TimeoutError as PoolTimeout

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.platform.execution_profile import ExecutionConfigurationError
from dbt_metricflow_service.runtime.executor import ERROR_COMMAND_FAILED, ExecutionError, RuntimeExecutor
from dbt_metricflow_service.storage.records import JobErrorDetail, LeasedJob, StoredBuild

TARGET_RELEASE_DEFERRED: Final[str] = "target release deferred to persisted attempt evidence"


if TYPE_CHECKING:
    from dbt_metricflow_service.application.deployments import DeploymentService
    from dbt_metricflow_service.runtime.executor import ExecutionResult
    from dbt_metricflow_service.runtime.service import Runtime


logger = logging.getLogger(__name__)
POLL_SECONDS = 0.25
INPUT_LOST = strings.INPUT_LOST
INTERRUPTED = strings.EXECUTION_OUTCOME_UNKNOWN
WORKER_FAILED = strings.WORKER_FAILED
FAILURE_LOG: Final[str] = "Runtime worker operation failed: %s"
EXTERNAL_OUTCOME_UNKNOWN = strings.EXTERNAL_OUTCOME_UNKNOWN_ALIAS


class Worker:
    """每个槽位仅持有一个 attempt；故障后由共享恢复扫描处理过期租约。"""

    def __init__(self, runtime: Runtime, *, deployments: DeploymentService | None = None) -> None:
        # 引用运行时依赖；内存仅记录本机进程，不承载公开任务状态。
        self.runtime = runtime
        self.deployments = deployments
        self.executor = RuntimeExecutor(runtime.settings, runtime.jobs, runtime.artifacts)
        self._tasks: list[asyncio.Task] = []
        self._stopping = False

    def start(self) -> None:
        self._tasks = [asyncio.create_task(self._slot()) for _ in range(self.runtime.settings.worker_concurrency)]
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
                await asyncio.to_thread(self.runtime.jobs.recover)
                if self.deployments is not None:
                    await asyncio.to_thread(self.deployments.reconcile_pending)
            except (DatabaseError, PoolTimeout, RuntimeError) as error:
                logger.warning(FAILURE_LOG, type(error).__name__)
            await asyncio.sleep(self.runtime.settings.heartbeat_seconds)

    async def _slot(self) -> None:
        while not self._stopping:
            try:
                job = await asyncio.to_thread(
                    self.runtime.jobs.claim, str(self.runtime.instance_id),
                    toolchain_version=self.runtime.toolchain,
                    config_versions=[self.runtime.settings.config_version],
                    kinds=[strings.BUILD_RUN, strings.RUN_CLEANUP],
                )
                if job is not None:
                    await self._execute(job)
                    continue
            except (DatabaseError, PoolTimeout, RuntimeError) as error:
                # 不记录连接异常原文，避免 DSN 或服务配置进入日志。
                logger.warning(FAILURE_LOG, type(error).__name__)
            await asyncio.sleep(POLL_SECONDS)

    async def _execute(self, job: LeasedJob) -> None:
        execution = asyncio.create_task(self.executor.execute(job))
        heartbeat = asyncio.create_task(self._heartbeat(job, execution))
        try:
            result = await execution
            # 只封存产物并提交任务状态，不保存通用结果或成功诊断副本。
            await asyncio.to_thread(
                self.runtime.jobs.finish, job.job_id, job.lease_token,
                output_set_id=result.output_set_id, seal=self.runtime.artifacts.seal,
            )
        except asyncio.CancelledError:
            execution.cancel()
            await asyncio.gather(execution, return_exceptions=True)
            # 尚未开始外部命令时可确认取消；外部写入仍保持未知及引用保护。
            external = await asyncio.to_thread(self.runtime.jobs.external_started, job.attempt_id)
            cancelled = False
            if job.request_json.build_id:
                from ..storage.builds import BuildStore

                build = await asyncio.to_thread(BuildStore(self.runtime.db).get, job.request_json.build_id)
                cancelled = cast(StoredBuild, build).cancel_requested
            code = strings.CANCELLED if cancelled and not external else INTERRUPTED
            await self._fail(job, code, detail=JobErrorDetail(external_outcome_unknown=external), stopped=not external)
            if self._stopping:
                raise
        except ExecutionConfigurationError as error:
            # 保留固定配置错误码，并依照已持久外部执行证据决定是否释放目标。
            external = await asyncio.to_thread(self.runtime.jobs.external_started, job.attempt_id)
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
            external = await asyncio.to_thread(self.runtime.jobs.external_started, job.attempt_id)
            await self._fail(job, WORKER_FAILED,
                             detail=JobErrorDetail(type=type(error).__name__, external_outcome_unknown=external),
                             stopped=not external)
            logger.warning(FAILURE_LOG, type(error).__name__)
        finally:
            from ..storage.execution_targets import ExecutionTargetStore
            if job.physical_target_key:
                try:
                    await asyncio.to_thread(ExecutionTargetStore(self.runtime.db).release,
                                            job.job_id, job.lease_token, True)
                except (DatabaseError, PoolTimeout, RuntimeError):
                    logger.warning(FAILURE_LOG, TARGET_RELEASE_DEFERRED)
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)

    async def _fail(self, job: LeasedJob, code: str, *, detail: JobErrorDetail | None=None, stopped: bool=True) -> None:
        try:
            await asyncio.to_thread(
                self.runtime.jobs.fail, job.job_id, job.lease_token, code, detail, stopped=stopped,
            )
        except (DatabaseError, PoolTimeout, RuntimeError):
            # 无法提交时不伪造终态，后续恢复扫描根据已持久阶段作出决定。
            logger.warning(FAILURE_LOG, code)

    async def _heartbeat(self, job: LeasedJob, execution: asyncio.Task[ExecutionResult]) -> None:
        last_confirmed = time.monotonic()
        while not execution.done():
            await asyncio.sleep(self.runtime.settings.heartbeat_seconds)
            try:
                # 请求取消只是意图；未知外部写入继续保留引用保护。
                if job.request_json.build_id:
                    from ..storage.builds import BuildStore

                    build = await asyncio.to_thread(BuildStore(self.runtime.db).get, job.request_json.build_id)
                    if cast(StoredBuild, build).cancel_requested:
                        execution.cancel()
                        return
                valid = await asyncio.to_thread(
                    self.runtime.jobs.heartbeat, job.job_id, job.lease_token,
                )
                if not valid:
                    execution.cancel()
                    return
                last_confirmed = time.monotonic()
            except (DatabaseError, PoolTimeout, RuntimeError):
                if time.monotonic() - last_confirmed >= self.runtime.settings.lease_seconds / 2:
                    execution.cancel()
                    return
