"""构建源码准备、dbt 执行与 Manifest 持久化。"""
from __future__ import annotations

from functools import partial
from pathlib import Path
from typing import Final, cast

import yaml

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.engines.dbt import DbtClient
from dbt_metricflow_service.models.execution import SNAPSHOT_TARGET
from dbt_metricflow_service.models.native import NativeProject
from dbt_metricflow_service.platform.manifest_capture import ManifestCapture
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.execution_logs import ExecutionLogStore
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.records import ArtifactMetadata, LeasedBuild

from .dbt import dbt_execution
from .embedded import run_serialized
from .environment import build_engine_environment
from .errors import ExecutionError
from .log_writer import ExecutionLogWriter
from .models import ExecutionResult
from .threads import run_in_thread
from .workspace import attempt_workspace

SOURCE_BRANCH_MISSING: Final[str] = "source branch does not exist"
PROJECT_DIRECTORY = strings.PROJECT_DIRECTORY
ERROR_LEASE_LOST = strings.LEASE_LOST
MISSING_MANIFEST = "successful dbt build did not return a manifest"


class BuildExecutor:
    """在一次 attempt 的独立目录中执行已认领任务，不管理公开队列。"""

    def __init__(self, settings: Settings, jobs: JobStore, artifacts: ArtifactStore) -> None:
        # 配置与持久仓储共享于本实例的执行槽位。
        self.settings = settings
        self.jobs = jobs
        self.artifacts = artifacts

    async def execute(self, job: LeasedBuild) -> ExecutionResult:
        """还原固定输入，等待执行退出，再删除可重建的本地目录。"""

        # SDK 调用和文件操作均等待线程退出，之后才退出工作目录上下文。
        with attempt_workspace(self.settings.temp_root, job.build_id, job.attempt_id) as attempt:
            return await self._build(job, attempt)

    async def _build(self, job: LeasedBuild, attempt: Path) -> ExecutionResult:
        job = await self._prepare_build_source(job, attempt)
        project = attempt / PROJECT_DIRECTORY
        await run_in_thread(self.artifacts.materialize, cast(str, job.input_set_id), project)

        from ..platform.execution_profile import materialized_profile
        # 按受理时固定的快照和凭据引用生成临时 dbt profile，限定本次构建实际连接的目标。
        profile = materialized_profile(job.execution, attempt, self.settings, project)
        with profile as profiles:
            return await execute_build(self, job, project, profiles=profiles)

    async def _prepare_build_source(self, job: LeasedBuild, attempt: Path) -> LeasedBuild:
        """受理之后获取固定源码；所有慢 I/O 均在短事务之外。"""
        import shutil

        from ..platform.bindings import ProjectBinding, resolve_commit
        from ..platform.source import remote_head

        if job.input_set_id:
            # 准备阶段重试可直接恢复已经关联的源码快照，即使远端分支移动也使用同一份源码。
            return job
        try:
            # 优先使用指定的提交；只给分支时先查远端 HEAD，并在拉取前把 SHA 固定到构建记录。
            sha = job.commit_sha
            if not sha:
                sha = await run_in_thread(remote_head, job.repository, job.branch_name, attempt)
                if sha is None:
                    raise ValueError(SOURCE_BRANCH_MISSING)
            await run_in_thread(self.jobs.pin_source, job.build_id, sha, job.lease_token)
            await run_in_thread(self.jobs.phase, job.build_id, job.lease_token, strings.FETCHING_SOURCE)
            binding = ProjectBinding(job.project_id, job.repository, ".", SNAPSHOT_TARGET)
            directory, digest = await run_in_thread(resolve_commit, binding, sha, attempt)
            try:
                source = await run_in_thread(
                    self.artifacts.capture, job.project_id, directory, metadata=ArtifactMetadata(
                    source_commit_sha=sha, project_digest=digest,
                    toolchain_version=job.toolchain_version))
            finally:
                await run_in_thread(shutil.rmtree, directory)
            # 在租约仍有效时固定源码产物及摘要，供本次执行和后续重试复用。
            if not await run_in_thread(self.jobs.attach_input, job.build_id, job.lease_token, source,
                                     project_digest=digest):
                raise ExecutionError(ERROR_LEASE_LOST, stopped=True)
            return job.model_copy(update={"input_set_id": source, "commit_sha": sha, "project_digest": digest})
        except (ValueError, OSError) as error:
            raise ExecutionError(strings.SOURCE_UNAVAILABLE, stopped=True) from error

    async def _authorize(self, job: LeasedBuild, phase: str, *, external: bool) -> None:
        # 两类引擎都在取得共享串行锁后记录阶段并重新核对租约。
        valid = await run_in_thread(self.jobs.phase, job.build_id, job.lease_token, phase, external=external)
        if not valid:
            raise ExecutionError(ERROR_LEASE_LOST, stopped=False)

    @staticmethod
    def _metadata(job: LeasedBuild) -> ArtifactMetadata:
        """发布元数据只引用持久集合与配置，不保存绝对路径。"""
        return ArtifactMetadata(
            source_set_id=job.input_set_id,
            toolchain_version=job.toolchain_version,
            source_commit_sha=job.commit_sha,
            project_digest=job.project_digest,
        )


def _normalize_artifact_target(project: Path) -> None:
    # build 已按命令参数输出到 target；只同步执行副本，供封存和历史查询恢复。
    path = project / strings.DBT_PROJECT_FILE
    configuration = NativeProject.model_validate(yaml.safe_load(path.read_text(encoding=strings.UTF8)))
    configuration.target_path = strings.TARGET_DIRECTORY
    path.write_text(yaml.safe_dump(configuration.native_details(), sort_keys=False), encoding=strings.UTF8)


async def execute_build(
    executor: BuildExecutor, job: LeasedBuild, project: Path, *, profiles: Path,
) -> ExecutionResult:
    settings = executor.settings
    environment = build_engine_environment(
        project, profiles, job.execution.schema_name, SNAPSHOT_TARGET,
    )
    capture = ManifestCapture(settings.max_artifact_file_bytes)
    writer = ExecutionLogWriter(partial(
        ExecutionLogStore(executor.jobs.db).append, job, strings.BUILDING, strings.DBT_BUILD_COMMAND,
    ))

    def build() -> None:
        with dbt_execution(job.project_id, environment, settings.max_output_bytes, writer.emit) as context:
            # 执行一次 dbt build，回调采集本次运行的事件；成功后捕获同一次调用生成的 Manifest。
            result = DbtClient().build(
                project, profiles, target=SNAPSHOT_TARGET,
                target_path=project / strings.TARGET_DIRECTORY, callbacks=[context.callback],
            )
            context.check(result)
            if result.manifest is None:
                raise ValueError(MISSING_MANIFEST)
            capture(result.manifest)

    try:
        await run_serialized(build, lambda: executor._authorize(job, strings.BUILDING, external=True))
    finally:
        # 取消时串行执行器也会等待引擎退出，之后才关闭日志写入器。
        await run_in_thread(writer.close)
    await run_in_thread(_normalize_artifact_target, project)
    # 将 Manifest 和工程产物持久化，交给 worker 在任务完成事务中校验并封存。
    output = await run_in_thread(
        capture.persist, executor.artifacts, job.project_id, project,
        producer_attempt_id=job.attempt_id, metadata=executor._metadata(job),
    )
    return ExecutionResult(output_set_id=output)
