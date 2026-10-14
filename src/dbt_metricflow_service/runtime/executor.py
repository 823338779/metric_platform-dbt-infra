from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Final, ParamSpec, TypeVar, cast

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.execution.dbt import run_dbt
from dbt_metricflow_service.execution.metricflow import MetricFlowRequest, ResultT, run_metricflow
from dbt_metricflow_service.execution.models import (
    CommandSpec,
    JobRecord,
    JobStatus,
)
from dbt_metricflow_service.platform.models import ProbeResult
from dbt_metricflow_service.runtime.workspace import attempt_workspace
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.records import ArtifactMetadata, JobErrorDetail, LeasedJob, StoredJob

# 保留后台线程函数的位置和关键字参数签名。
UNSUPPORTED_JOB_KIND: Final[str] = "unsupported runtime job kind"
SOURCE_BRANCH_MISSING: Final[str] = "source branch does not exist"


P = ParamSpec("P")

# 泛型保留调用方的元素或执行结果类型。
T = TypeVar("T")


# 任务类型与持久阶段使用固定协议值，避免执行器扩展公开队列状态。
BUILD_RUN = strings.BUILD_RUN
RUN_CLEANUP = strings.RUN_CLEANUP
VALIDATING = strings.VALIDATING
PROJECT_DIRECTORY = strings.PROJECT_DIRECTORY
UTF8 = strings.UTF8
JSON_MODE = strings.JSON_MODE
ERROR_LEASE_LOST = strings.LEASE_LOST
ERROR_COMMAND_FAILED = strings.COMMAND_FAILED
ERROR_COMMAND_TIMEOUT = strings.COMMAND_TIMEOUT
ERROR_RESULT_TOO_LARGE = strings.RESULT_TOO_LARGE
ERROR_RESULT_INVALID = strings.RESULT_INVALID
ERROR_INVALID_QUERY = strings.INVALID_QUERY
TARGET_DIRECTORY = strings.TARGET_DIRECTORY


def build_engine_environment(project: Path, profiles: Path, schema: str, target: str) -> dict[str, str]:
    """构建 dbt 与 MetricFlow 共用的引擎环境。"""

    return {
        **os.environ,
        strings.DBT_PROJECT_DIR: str(project),
        strings.DBT_PROFILES_DIR: str(profiles),
        strings.DBT_TARGET_PATH: str(project / TARGET_DIRECTORY),
        strings.DBT_PLATFORM_SCHEMA: schema,
        strings.DBT_SEND_ANONYMOUS_USAGE_STATS: strings.DBT_TELEMETRY_DISABLED,
        strings.DBT_TARGET: target,
        strings.PYTHONUTF8: strings.UTF8_ENV_ENABLED,
    }


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    """执行完成的产物身份；只有 worker 的带租约事务能封存产物。"""
    # 当前 attempt 产生的 STAGING 集合，交给发布事务封存。
    output_set_id: str | None = None


class ExecutionError(RuntimeError):
    """向 worker 提供稳定错误码和可以安全保存的引擎诊断。"""

    def __init__(self, code: str, payload: JobErrorDetail | None = None, *, stopped: bool = True) -> None:
        super().__init__(code)
        # 错误码不包含环境、输入 YAML 或执行目录绝对路径。
        self.code = code
        # 引擎输出已由进程内执行器脱敏并限制字节数。
        self.payload = payload or JobErrorDetail()
        # 超时不能证明目标仓库已经停止执行。
        self.stopped = stopped


async def _finish_task(task: asyncio.Task[T]) -> T:
    """重复取消只延迟调用者返回，不中断已经开始的清理或文件读写。"""

    cancelled = False
    while True:
        try:
            result = await asyncio.shield(task)
            break
        except asyncio.CancelledError:
            cancelled = True
            if task.cancelled():
                raise
        except Exception:
            if cancelled:
                raise asyncio.CancelledError from None
            raise
    if cancelled:
        raise asyncio.CancelledError
    return result


async def _thread(function: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
    """取消时等待读写线程退出，防止清理后仍向已删除目录写入。"""

    return await _finish_task(asyncio.create_task(asyncio.to_thread(function, *args, **kwargs)))


class RuntimeExecutor:
    """在一次 attempt 的独立目录中执行已认领任务，不管理公开队列。"""

    def __init__(self, settings: Settings, jobs: JobStore, artifacts: ArtifactStore) -> None:
        # 配置与持久仓储共享于本实例的执行槽位。
        self.settings = settings
        self.jobs = jobs
        self.artifacts = artifacts

    async def execute(self, job: LeasedJob) -> ExecutionResult:
        """还原固定输入，等待执行退出，再删除可重建的本地目录。"""

        # SDK 调用和文件操作均等待线程退出，之后才退出工作目录上下文。
        with attempt_workspace(self.settings.temp_root, job.job_id, job.attempt_id) as attempt:
            if job.kind == BUILD_RUN:
                return await self._build(job, attempt)
            # 固定 schema 内的表由多次构建共用，清理仅结束本构建的可查询生命周期。
            if job.kind == RUN_CLEANUP:
                return ExecutionResult()
            raise ValueError(UNSUPPORTED_JOB_KIND)

    async def _build(self, job: LeasedJob, attempt: Path) -> ExecutionResult:
        from ..platform.build import execute_publication

        if not job.request_json.build_id:
            raise ExecutionError(strings.FIXED_COMMIT_INPUT_REQUIRED)
        # 缺少快照时在 Git、产物恢复和目标占用之前拒绝旧任务。
        raw = job.request_json.execution
        if not raw:
            raise ExecutionError(strings.EXECUTION_SNAPSHOT_REQUIRED)
        job = await self._prepare_build_source(job)
        project = attempt / PROJECT_DIRECTORY
        await _thread(self.artifacts.materialize, cast(str, job.input_set_id), project)

        from ..platform.execution_profile import materialized_profile
        from ..storage.execution_targets import ExecutionTargetStore
        targets = ExecutionTargetStore(self.jobs.db)
        while not await _thread(targets.claim, job.job_id, job.lease_token):
            if not await _thread(self.jobs.heartbeat, job.job_id, job.lease_token):
                raise ExecutionError(ERROR_LEASE_LOST)
            await asyncio.sleep(0.25)
        profile = materialized_profile(raw, attempt, self.settings, project)
        with profile as profiles:
            return await execute_publication(self, job, attempt, project, profiles=profiles)

    async def _prepare_build_source(self, job: LeasedJob) -> LeasedJob:
        """受理之后获取固定源码；所有慢 I/O 均在短事务之外。"""
        import shutil

        from ..platform.bindings import ProjectBinding, resolve_commit
        from ..platform.source import remote_head
        from ..storage.builds import BuildStore

        body = job.request_json
        if job.input_set_id:
            return job
        builds = BuildStore(self.jobs.db)
        try:
            sha = body.commit_sha
            if not sha:
                sha = await _thread(remote_head, body.repository, body.branch_name, self.settings.temp_root)
                if sha is None:
                    raise ValueError(SOURCE_BRANCH_MISSING)
            await _thread(builds.pin_source, body.build_id, sha, job.lease_token)
            await _thread(self.jobs.phase, job.job_id, job.lease_token, strings.FETCHING_SOURCE)
            binding = ProjectBinding(job.project_id, body.repository, ".", cast(str, job.profile_binding_id))
            directory, digest = await _thread(resolve_commit, binding, sha, self.settings.temp_root)
            try:
                source = await _thread(self.artifacts.capture, job.project_id, directory, metadata=ArtifactMetadata(
                    source_commit_sha=sha, project_digest=digest, config_version=job.config_version,
                    toolchain_version=job.toolchain_version))
            finally:
                await _thread(shutil.rmtree, directory)
            if not await _thread(self.jobs.attach_input, job.job_id, job.lease_token, source,
                                     project_digest=digest):
                raise ExecutionError(ERROR_LEASE_LOST, stopped=True)
            return LeasedJob(
                **cast(StoredJob, await _thread(self.jobs.get, job.job_id)).model_dump(),
                attempt_id=job.attempt_id, lease_token=job.lease_token,
            )
        except (ValueError, OSError) as error:
            raise ExecutionError(strings.SOURCE_UNAVAILABLE, stopped=True) from error

    async def _authorize(self, job: LeasedJob, phase: str, *, external: bool) -> None:
        # 两类引擎都在取得共享串行锁后记录阶段并重新核对租约。
        valid = await _thread(self.jobs.phase, job.job_id, job.lease_token, phase, external=external)
        if not valid:
            raise ExecutionError(ERROR_LEASE_LOST, stopped=False)

    async def _dbt(self, job: LeasedJob, spec: CommandSpec, phase: str) -> JobRecord:
        # dbt 使用命令执行记录；不携带 MetricFlow 的请求、结果或业务错误码。
        record = await run_dbt(
            job.project_id, spec, self.settings.command_timeout_seconds,
            self.settings.max_output_bytes, lambda: self._authorize(job, phase, external=spec.write_operation),
        )
        # parse/debug 正常退出无需写入核对，写命令失联不能据退出码释放保护。
        self._check_record(record, may_write=spec.write_operation)
        return record

    @staticmethod
    def _check_record(record: JobRecord, *, may_write: bool) -> None:
        # 仅统一已脱敏的运行诊断与超时保护，不解释引擎特有的业务结果。
        if record.status is not JobStatus.SUCCEEDED:
            timed_out = record.status is JobStatus.TIMED_OUT
            raise ExecutionError(
                ERROR_COMMAND_TIMEOUT if timed_out else ERROR_COMMAND_FAILED,
                JobErrorDetail.model_validate(record, from_attributes=True),
                stopped=not timed_out and not may_write,
            )

    async def probe_query_capability(
        self, job: LeasedJob, project: Path, profiles: Path | None = None,
    ) -> ProbeResult:
        from ..platform.metricflow import probe_query_capability
        return await self._metricflow(job, project, probe_query_capability, ProbeResult, profiles)

    async def _metricflow(
        self, job: LeasedJob, project: Path, operation: Callable[[Path, Path], ResultT],
        result_type: type[ResultT], profiles: Path | None = None,
    ) -> ResultT:
        # 请求作为内存对象交给执行线程，结果也不经过临时控制文件。
        environment = build_engine_environment(
            project, profiles or self.settings.profiles_dir, cast(str, job.schema_name),
            cast(str, job.profile_binding_id),
        )
        external = job.kind == RUN_CLEANUP
        result = await run_metricflow(
            job.project_id, MetricFlowRequest(environment, operation), self.settings.command_timeout_seconds,
            self.settings.max_output_bytes,
            lambda: self._authorize(job, VALIDATING, external=external),
        )
        # 构建探测在内存中读取结果；清理结果由调用方丢弃。
        if result.record.status is JobStatus.FAILED and result.error_code == ERROR_INVALID_QUERY:
            raise ExecutionError(ERROR_INVALID_QUERY)
        self._check_record(result.record, may_write=job.kind == RUN_CLEANUP)
        value = result.payload
        if not isinstance(value, result_type):
            raise ExecutionError(ERROR_RESULT_INVALID)
        size = 0
        try:
            for chunk in json.JSONEncoder(ensure_ascii=False).iterencode(
                value.model_dump(mode=strings.PYTHON_MODE, by_alias=True, exclude_unset=True),
            ):
                size += len(chunk.encode(UTF8))
                if size > self.settings.max_result_bytes:
                    raise ExecutionError(ERROR_RESULT_TOO_LARGE)
        except (TypeError, ValueError, RecursionError) as error:
            record = result.record.with_outcome(
                status=JobStatus.FAILED, exit_code=result.record.exit_code, stderr=type(error).__name__,
            )
            raise ExecutionError(
                ERROR_COMMAND_FAILED, JobErrorDetail.model_validate(record, from_attributes=True),
                stopped=job.kind != RUN_CLEANUP,
            ) from error
        return value

    @staticmethod
    def _metadata(job: LeasedJob) -> ArtifactMetadata:
        """发布元数据只引用持久集合与配置，不保存绝对路径。"""
        return ArtifactMetadata(
            source_set_id=job.input_set_id,
            config_version=job.config_version,
            toolchain_version=job.toolchain_version,
            source_commit_sha=job.request_json.commit_sha,
            project_digest=job.request_json.project_digest,
        )
