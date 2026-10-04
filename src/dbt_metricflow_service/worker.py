"""有界 PostgreSQL worker，所有运行权限由数据库租约决定。"""

from __future__ import annotations

import asyncio
import json
import logging
import time

from psycopg2 import Error as DatabaseError

from dbt_metricflow_service.runtime_execution import ERROR_COMMAND_FAILED, ExecutionError, RuntimeExecutor, _thread

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

    def __init__(self, runtime):
        # 引用运行时依赖；内存仅记录本机进程，不承载公开任务状态。
        self.runtime = runtime
        self.executor = RuntimeExecutor(runtime.settings, runtime.jobs, runtime.artifacts)
        self._tasks: list[asyncio.Task] = []
        self._stopping = False

    def start(self):
        self._tasks = [asyncio.create_task(self._slot()) for _ in range(self.runtime.settings.worker_concurrency)]
        self._tasks.append(asyncio.create_task(self._maintain()))
        if self.runtime.settings.branch_events_enabled:
            self._tasks.append(asyncio.create_task(self._sync_branches()))

    async def _sync_branches(self):
        # 使用现有 worker 生命周期，慢 Git 核对不能阻塞任务和输入的续租循环。
        from dbt_metricflow_service.branch_sync import BranchSynchronizer

        synchronizer = BranchSynchronizer(self.runtime)
        while not self._stopping:
            try:
                await _thread(synchronizer.scan)
            except (DatabaseError, RuntimeError) as error:
                logger.warning(FAILURE_LOG, type(error).__name__)
            await asyncio.sleep(self.runtime.settings.branch_poll_seconds)

    async def close(self):
        # 取消执行会先由执行器终止子进程树；数据库失联时等待租约恢复。
        self._stopping = True
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def _maintain(self):
        while not self._stopping:
            try:
                with self.runtime._input_lock:
                    ids = list(self.runtime._inputs)
                await asyncio.to_thread(
                    self.runtime.jobs.heartbeat_inputs, str(self.runtime.instance_id), job_ids=ids,
                )
                await asyncio.to_thread(self.runtime.jobs.recover)
                # 已经由恢复扫描终结的本机输入及时释放，避免占满接收槽位。
                for job_id in ids:
                    row = await asyncio.to_thread(self.runtime.jobs.get, job_id)
                    if row and row["status"] in ("FAILED", "SUCCEEDED"):
                        self.runtime.discard_input(job_id)
            except (DatabaseError, RuntimeError) as error:
                logger.warning(FAILURE_LOG, type(error).__name__)
            await asyncio.sleep(self.runtime.settings.heartbeat_seconds)

    async def _slot(self):
        while not self._stopping:
            try:
                job = await asyncio.to_thread(
                    self.runtime.jobs.claim, str(self.runtime.instance_id),
                    toolchain_version=self.runtime.toolchain,
                    config_versions=[self.runtime.settings.config_version],
                )
                if job is not None:
                    await self._execute(job)
                    continue
            except (DatabaseError, RuntimeError) as error:
                # 不记录连接异常原文，避免 DSN 或服务配置进入日志。
                logger.warning(FAILURE_LOG, type(error).__name__)
            await asyncio.sleep(POLL_SECONDS)

    async def _execute(self, job):
        resources = self.runtime.input_for(job["job_id"])
        if job["input_mode"] == VOLATILE and resources is None:
            await asyncio.to_thread(self.runtime.jobs.fail, job["job_id"], job["lease_token"], INPUT_LOST)
            return
        execution = asyncio.create_task(self.executor.execute(job, resources))
        heartbeat = asyncio.create_task(self._heartbeat(job, execution))
        try:
            result = await execution
            if len(json.dumps(result.payload, ensure_ascii=False).encode()) > self.runtime.settings.max_result_bytes:
                raise ExecutionError(RESULT_TOO_LARGE)
            # 输出、产物封存和任务成功在同一持久事务内发布。
            await asyncio.to_thread(
                self.runtime.jobs.finish, job["job_id"], job["lease_token"], result.payload,
                output_set_id=result.output_set_id, seal=self.runtime.artifacts.seal,
                stdout_tail=result.payload.get("stdout", ""), stderr_tail=result.payload.get("stderr", ""),
                exit_code=result.payload.get("exit_code", 0),
                output_truncated=result.payload.get("output_truncated", False),
            )
        except asyncio.CancelledError:
            execution.cancel()
            await asyncio.gather(execution, return_exceptions=True)
            await self._fail(job, INTERRUPTED, stopped=False)
            if self._stopping:
                raise
        except ExecutionError as error:
            code = INTERRUPTED if error.code == ERROR_COMMAND_FAILED and not error.stopped else error.code
            detail = {**error.payload, EXTERNAL_OUTCOME_UNKNOWN: not error.stopped}
            await self._fail(job, code, detail=detail, stopped=error.stopped)
        except Exception as error:
            # 不保存任意异常文本，其中可能含凭据或临时输入。
            await self._fail(job, WORKER_FAILED, detail={"type": type(error).__name__}, stopped=False)
            logger.warning(FAILURE_LOG, type(error).__name__)
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
            self.runtime.discard_input(job["job_id"])

    async def _fail(self, job, code, *, detail=None, stopped=True):
        try:
            await asyncio.to_thread(
                self.runtime.jobs.fail, job["job_id"], job["lease_token"], code, detail, stopped=stopped,
            )
        except (DatabaseError, RuntimeError):
            # 无法提交时不伪造终态，后续恢复扫描根据已持久阶段作出决定。
            logger.warning(FAILURE_LOG, code)

    async def _heartbeat(self, job, execution):
        last_confirmed = time.monotonic()
        while not execution.done():
            await asyncio.sleep(self.runtime.settings.heartbeat_seconds)
            try:
                valid = await asyncio.to_thread(
                    self.runtime.jobs.heartbeat, job["job_id"], job["lease_token"],
                )
                if not valid:
                    execution.cancel()
                    return
                last_confirmed = time.monotonic()
            except (DatabaseError, RuntimeError):
                if time.monotonic() - last_confirmed >= self.runtime.settings.lease_seconds / 2:
                    execution.cancel()
                    return
