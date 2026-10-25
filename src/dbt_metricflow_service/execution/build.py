"""构建源码准备、目标租约、dbt 执行与 Manifest 持久化。"""
from __future__ import annotations

import asyncio
from functools import partial
from pathlib import Path
from typing import Final, cast

import yaml

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.engines.dbt import DbtClient
from dbt_metricflow_service.models.native import NativeProject
from dbt_metricflow_service.platform.manifest_capture import ManifestCapture
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.execution_logs import ExecutionLogStore
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.records import ArtifactMetadata, LeasedJob, StoredJob

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

    async def execute(self, job: LeasedJob) -> ExecutionResult:
        """还原固定输入，等待执行退出，再删除可重建的本地目录。"""

        # SDK 调用和文件操作均等待线程退出，之后才退出工作目录上下文。
        with attempt_workspace(self.settings.temp_root, job.job_id, job.attempt_id) as attempt:
            return await self._build(job, attempt)

    async def _build(self, job: LeasedJob, attempt: Path) -> ExecutionResult:
        if not job.request_json.build_id:
            raise ExecutionError(strings.FIXED_COMMIT_INPUT_REQUIRED)
        # 缺少快照时在 Git、产物恢复和目标占用之前拒绝旧任务。
        raw = job.request_json.execution
        if not raw:
            raise ExecutionError(strings.EXECUTION_SNAPSHOT_REQUIRED)
        job = await self._prepare_build_source(job, attempt)
        project = attempt / PROJECT_DIRECTORY
        await run_in_thread(self.artifacts.materialize, cast(str, job.input_set_id), project)

        from ..platform.execution_profile import materialized_profile
        from ..storage.execution_targets import ExecutionTargetStore
        targets = ExecutionTargetStore(self.jobs.db)
        while not await run_in_thread(targets.claim, job.job_id, job.lease_token):
            if not await run_in_thread(self.jobs.heartbeat, job.job_id, job.lease_token):
                raise ExecutionError(ERROR_LEASE_LOST)
            await asyncio.sleep(0.25)
        profile = materialized_profile(raw, attempt, self.settings, project)
        with profile as profiles:
            return await execute_build(self, job, project, profiles=profiles)

    async def _prepare_build_source(self, job: LeasedJob, attempt: Path) -> LeasedJob:
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
                sha = await run_in_thread(remote_head, body.repository, body.branch_name, attempt)
                if sha is None:
                    raise ValueError(SOURCE_BRANCH_MISSING)
            await run_in_thread(builds.pin_source, body.build_id, sha, job.lease_token)
            await run_in_thread(self.jobs.phase, job.job_id, job.lease_token, strings.FETCHING_SOURCE)
            binding = ProjectBinding(job.project_id, body.repository, ".", cast(str, job.profile_binding_id))
            directory, digest = await run_in_thread(resolve_commit, binding, sha, attempt)
            try:
                source = await run_in_thread(
                    self.artifacts.capture, job.project_id, directory, metadata=ArtifactMetadata(
                    source_commit_sha=sha, project_digest=digest, config_version=job.config_version,
                    toolchain_version=job.toolchain_version))
            finally:
                await run_in_thread(shutil.rmtree, directory)
            if not await run_in_thread(self.jobs.attach_input, job.job_id, job.lease_token, source,
                                     project_digest=digest):
                raise ExecutionError(ERROR_LEASE_LOST, stopped=True)
            return LeasedJob(
                **cast(StoredJob, await run_in_thread(self.jobs.get, job.job_id)).model_dump(),
                attempt_id=job.attempt_id, lease_token=job.lease_token,
            )
        except (ValueError, OSError) as error:
            raise ExecutionError(strings.SOURCE_UNAVAILABLE, stopped=True) from error

    async def _authorize(self, job: LeasedJob, phase: str, *, external: bool) -> None:
        # 两类引擎都在取得共享串行锁后记录阶段并重新核对租约。
        valid = await run_in_thread(self.jobs.phase, job.job_id, job.lease_token, phase, external=external)
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


def _normalize_artifact_target(project: Path) -> None:
    # build 已按命令参数输出到 target；只同步执行副本，供封存和历史查询恢复。
    path = project / strings.DBT_PROJECT_FILE
    configuration = NativeProject.model_validate(yaml.safe_load(path.read_text(encoding=strings.UTF8)))
    configuration.target_path = strings.TARGET_DIRECTORY
    path.write_text(yaml.safe_dump(configuration.native_details(), sort_keys=False), encoding=strings.UTF8)


async def execute_build(
    executor: BuildExecutor, job: LeasedJob, project: Path, *, profiles: Path,
) -> ExecutionResult:
    settings = executor.settings
    environment = build_engine_environment(
        project, profiles, cast(str, job.schema_name), cast(str, job.profile_binding_id),
    )
    capture = ManifestCapture(settings.max_artifact_file_bytes)
    writer = ExecutionLogWriter(partial(
        ExecutionLogStore(executor.jobs.db).append, job, strings.BUILDING, strings.DBT_BUILD_COMMAND,
    ))

    def build() -> None:
        with dbt_execution(job.project_id, environment, settings.max_output_bytes, writer.emit) as context:
            result = DbtClient().build(
                project, profiles, target=cast(str, job.profile_binding_id),
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
    output = await run_in_thread(
        capture.persist, executor.artifacts, job.project_id, project,
        producer_attempt_id=job.attempt_id, metadata=executor._metadata(job),
    )
    return ExecutionResult(output_set_id=output)
