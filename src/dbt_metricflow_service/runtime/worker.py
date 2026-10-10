"""有界 PostgreSQL worker，所有运行权限由数据库租约决定。"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import TYPE_CHECKING, cast

from sqlalchemy.exc import DBAPIError as DatabaseError
from sqlalchemy.exc import TimeoutError as PoolTimeout

from dbt_metricflow_service.models.payloads import JsonObject
from dbt_metricflow_service.runtime.completion import complete_job
from dbt_metricflow_service.runtime.executor import ERROR_COMMAND_FAILED, ExecutionError, RuntimeExecutor
from dbt_metricflow_service.storage.records import LeasedJob, StoredBuild

if TYPE_CHECKING:
    from dbt_metricflow_service.runtime.executor import ExecutionResult
    from dbt_metricflow_service.runtime.service import Runtime


logger = logging.getLogger(__name__)
POLL_SECONDS = 0.25
INPUT_LOST = "INPUT_LOST"
INTERRUPTED = "EXECUTION_OUTCOME_UNKNOWN"
RESULT_TOO_LARGE = "RESULT_TOO_LARGE"
WORKER_FAILED = "WORKER_FAILED"
VOLATILE = "VOLATILE"
FAILURE_LOG = "Runtime worker operation failed: %s"
EXTERNAL_OUTCOME_UNKNOWN = "externalOutcomeUnknown"


class Worker:
    """每个槽位仅持有一个 attempt；故障后由共享恢复扫描处理过期租约。"""

    def __init__(self, runtime: Runtime) -> None:
        # 引用运行时依赖；内存仅记录本机进程，不承载公开任务状态。
        self.runtime = runtime
        self.executor = RuntimeExecutor(runtime.settings, runtime.jobs, runtime.artifacts)
        self._tasks: list[asyncio.Task] = []
        self._stopping = False

    def start(self) -> None:
        self._tasks = [asyncio.create_task(self._slot()) for _ in range(self.runtime.settings.worker_concurrency)]
        self._tasks.append(asyncio.create_task(self._maintain()))

    async def close(self) -> None:
        # 取消执行会先由执行器终止子进程树；数据库失联时等待租约恢复。
        self._stopping = True
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def _maintain(self) -> None:
        while not self._stopping:
            try:
                await asyncio.to_thread(self.runtime.jobs.recover)
                deployments = getattr(self.runtime, "deployments", None)
                if deployments is not None:
                    await asyncio.to_thread(deployments.reconcile_pending)
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
                    kinds=["BUILD_RUN", "METRIC_QUERY", "QUERY_OPTIONS", "RUN_CLEANUP"],
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
            if len(json.dumps(result.payload, ensure_ascii=False).encode()) > self.runtime.settings.max_result_bytes:
                raise ExecutionError(RESULT_TOO_LARGE)
            # 输出、产物封存和任务成功在同一持久事务内发布。
            await asyncio.to_thread(
                complete_job, self.runtime.jobs, job["job_id"], job["lease_token"], result.payload,
                output_set_id=result.output_set_id, seal=self.runtime.artifacts.seal,
                stdout_tail=result.payload.get("stdout", ""), stderr_tail=result.payload.get("stderr", ""),
                exit_code=result.payload.get("exit_code", 0),
                output_truncated=result.payload.get("output_truncated", False),
            )
        except asyncio.CancelledError:
            execution.cancel()
            await asyncio.gather(execution, return_exceptions=True)
            # 尚未开始外部命令时可确认取消；外部写入仍保持未知及引用保护。
            external = await asyncio.to_thread(self.runtime.jobs.external_started, job["attempt_id"])
            cancelled = False
            if job.get("request_json", {}).get("buildId"):
                from ..storage.builds import BuildStore

                build = await asyncio.to_thread(BuildStore(self.runtime.db).get, job["request_json"]["buildId"])
                cancelled = cast(StoredBuild, build)["cancel_requested"]
            code = "CANCELLED" if cancelled and not external else INTERRUPTED
            await self._fail(job, code, detail={EXTERNAL_OUTCOME_UNKNOWN: external}, stopped=not external)
            if self._stopping:
                raise
        except ExecutionError as error:
            code = INTERRUPTED if error.code == ERROR_COMMAND_FAILED and not error.stopped else error.code
            detail = {**error.payload, EXTERNAL_OUTCOME_UNKNOWN: not error.stopped}
            await self._fail(job, code, detail=detail, stopped=error.stopped)
        except Exception as error:
            # 不保存任意异常文本，其中可能含凭据或临时输入。
            external = await asyncio.to_thread(self.runtime.jobs.external_started, job["attempt_id"])
            await self._fail(job, WORKER_FAILED,
                             detail={"type": type(error).__name__, EXTERNAL_OUTCOME_UNKNOWN: external},
                             stopped=not external)
            logger.warning(FAILURE_LOG, type(error).__name__)
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)

    async def _fail(self, job: LeasedJob, code: str, *, detail: JsonObject | None=None, stopped: bool=True) -> None:
        try:
            await asyncio.to_thread(
                self.runtime.jobs.fail, job["job_id"], job["lease_token"], code, detail, stopped=stopped,
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
                if job.get("request_json", {}).get("buildId"):
                    from ..storage.builds import BuildStore

                    build = await asyncio.to_thread(BuildStore(self.runtime.db).get, job["request_json"]["buildId"])
                    if cast(StoredBuild, build)["cancel_requested"]:
                        execution.cancel()
                        return
                valid = await asyncio.to_thread(
                    self.runtime.jobs.heartbeat, job["job_id"], job["lease_token"],
                )
                if not valid:
                    execution.cancel()
                    return
                last_confirmed = time.monotonic()
            except (DatabaseError, PoolTimeout, RuntimeError):
                if time.monotonic() - last_confirmed >= self.runtime.settings.lease_seconds / 2:
                    execution.cancel()
                    return
