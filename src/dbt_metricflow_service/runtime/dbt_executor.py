from __future__ import annotations

import asyncio
import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Final, ParamSpec, TypeVar, cast

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.runtime.workspace import attempt_workspace
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.records import ArtifactMetadata, JobErrorDetail, LeasedJob, StoredJob

# 保留后台线程函数的位置和关键字参数签名。
SOURCE_BRANCH_MISSING: Final[str] = "source branch does not exist"


P = ParamSpec("P")

# 泛型保留调用方的元素或执行结果类型。
T = TypeVar("T")


# 任务类型与持久阶段使用固定协议值，避免执行器扩展公开队列状态。
PROJECT_DIRECTORY = strings.PROJECT_DIRECTORY
ERROR_LEASE_LOST = strings.LEASE_LOST
ERROR_COMMAND_FAILED = strings.COMMAND_FAILED
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
        # 连接异常等失败不能证明目标仓库已经停止执行。
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
            return await self._build(job, attempt)

    async def _build(self, job: LeasedJob, attempt: Path) -> ExecutionResult:
        from ..platform.build import execute_build

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
            return await execute_build(self, job, project, profiles=profiles)

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
