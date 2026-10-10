from __future__ import annotations

import asyncio
import json
import os
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ParamSpec, TypeVar, cast
from uuid import UUID

from dbt_metricflow_service.execution.models import (
    CommandSpec,
    JobRecord,
    JobStatus,
)
from dbt_metricflow_service.execution.runner import JobRunner
from dbt_metricflow_service.models.payloads import JsonObject
from dbt_metricflow_service.platform.namespace import (
    run_prefix,
)
from dbt_metricflow_service.runtime.workspace import attempt_workspace
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.records import LeasedJob, StoredJob

# 保留后台线程函数的位置和关键字参数签名。
P = ParamSpec("P")

# 泛型保留调用方的元素或执行结果类型。
T = TypeVar("T")


# 任务类型与持久阶段使用固定协议值，避免执行器扩展公开队列状态。
BUILD_RUN = "BUILD_RUN"
DRAFT_VALIDATION = "DRAFT_VALIDATION"
DBT_COMMAND = "DBT_COMMAND"
METRIC_QUERY = "METRIC_QUERY"
QUERY_OPTIONS = "QUERY_OPTIONS"
RUN_CLEANUP = "RUN_CLEANUP"
VALIDATING = "VALIDATING"
PROJECT_DIRECTORY = "project"
RESOURCE_DIRECTORY = "resources"
UTF8 = "utf-8"
JSON_MODE = "json"
ERROR_LEASE_LOST = "LEASE_LOST"
ERROR_COMMAND_FAILED = "COMMAND_FAILED"
ERROR_COMMAND_TIMEOUT = "COMMAND_TIMEOUT"
ERROR_RESULT_TOO_LARGE = "RESULT_TOO_LARGE"
ERROR_RESULT_INVALID = "RESULT_INVALID"
ERROR_INVALID_QUERY = "INVALID_QUERY"
PLATFORM_MODULE = "dbt_metricflow_service.platform.metricflow"
MODULE_OPTION = "-m"
TARGET_DIRECTORY = "target"
INPUT_FILE = "input.json"
OUTPUT_FILE = "output.json"
QUERY_MODE = "QUERY"
CLEANUP_MODE = "CLEANUP"
LOCAL_DBT_COMMANDS = frozenset({"parse", "debug"})


def build_programmatic_command(
    project: Path, profiles: Path, schema: str, target: str, input_path: Path, output_path: Path,
) -> CommandSpec:
    """沿用程序化 CLI 协议，由 JobRunner 统一管理取消、超时和输出限制。"""

    environment = {
        **os.environ, "DBT_PROJECT_DIR": str(project), "DBT_PROFILES_DIR": str(profiles),
        "DBT_TARGET_PATH": str(project / TARGET_DIRECTORY), "DBT_PLATFORM_SCHEMA": schema,
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false", "DBT_TARGET": target, "PYTHONUTF8": "1",
    }
    return CommandSpec(
        argv=(sys.executable, MODULE_OPTION, PLATFORM_MODULE, str(input_path), str(output_path)),
        cwd=project, environment=environment, write_operation=False,
    )


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    """执行完成的数据；只有 worker 的带租约事务能发布结果。"""

    # 现有 HTTP 接口使用的结构化结果及受限 CLI 诊断。
    payload: dict[str, Any]
    # 当前 attempt 产生的 STAGING 集合，交给发布事务封存。
    output_set_id: str | None = None


class ExecutionError(RuntimeError):
    """向 worker 提供稳定错误码和可以安全保存的子进程诊断。"""

    def __init__(self, code: str, payload: JsonObject | None = None, *, stopped: bool = True) -> None:
        super().__init__(code)
        # 错误码不包含环境、输入 YAML 或子进程绝对路径。
        self.code = code
        # 子进程输出已由 JobRunner 脱敏并限制字节数。
        self.payload = payload or {}
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
        """还原固定输入，等待子进程退出，再删除可重建的本地目录。"""

        # 复用路径、链接与 UUID 检查，在进程停止后退出工作目录上下文。
        with attempt_workspace(self.settings.temp_root, job["job_id"], job["attempt_id"]) as attempt:
            runner = JobRunner(
                self.settings.command_timeout_seconds, self.settings.max_output_bytes,
                job_artifacts_root=attempt / RESOURCE_DIRECTORY,
            )
            try:
                if job["kind"] == BUILD_RUN:
                    return await self._build(job, runner, attempt)
                # 构建在源码固定前失败时尚未执行任何仓库命令，无目录可还原。
                if job["kind"] == RUN_CLEANUP and job["input_set_id"] is None:
                    return ExecutionResult({})
                project = attempt / PROJECT_DIRECTORY
                await _thread(self.artifacts.materialize, str(job["input_set_id"]), project)
                if job["kind"] in {METRIC_QUERY, QUERY_OPTIONS, RUN_CLEANUP}:
                    body = job["request_json"]
                    if job["kind"] == METRIC_QUERY:
                        body = {"mode": QUERY_MODE, "request": body.get("engineRequest", body)}
                    elif job["kind"] == RUN_CLEANUP:
                        body = {"mode": CLEANUP_MODE, "schema": cast(str, job["schema_name"])}
                        parent_id = UUID(job["parent_run_id"])
                        if cast(str, job["schema_name"]) != "run_" + parent_id.hex:
                            body.update({"runId": str(parent_id), "tablePrefix": run_prefix(parent_id)})
                    payload = await self._programmatic(job, runner, project, attempt, body)
                    return ExecutionResult(payload)
                raise ValueError("unsupported runtime job kind")
            finally:
                # JobRunner.close 等待其进程树清理，之后才能移除工作目录。
                await _finish_task(asyncio.create_task(runner.close()))

    async def _build(self, job: LeasedJob, runner: JobRunner, attempt: Path) -> ExecutionResult:
        from ..platform.build import execute_publication

        if job["request_json"].get("buildId"):
            job = await self._prepare_build_source(job)
        elif not job["input_set_id"] or not job["request_json"].get("releaseId"):
            raise ExecutionError("FIXED_COMMIT_INPUT_REQUIRED")
        project = attempt / PROJECT_DIRECTORY
        await _thread(self.artifacts.materialize, cast(str, job["input_set_id"]), project)
        return await execute_publication(self, job, runner, attempt, project)

    async def _prepare_build_source(self, job: LeasedJob) -> LeasedJob:
        """受理之后获取固定源码；所有慢 I/O 均在短事务之外。"""
        import shutil

        from ..application.builds import BuildService
        from ..platform.bindings import ProjectBinding, resolve_commit
        from ..platform.source import remote_head
        from ..storage.builds import BuildStore

        body = job["request_json"]
        if job["input_set_id"]:
            return job
        builds = BuildStore(self.jobs.db)
        service = BuildService(builds, job["toolchain_version"], self.settings.command_timeout_seconds)
        try:
            sha = body.get("commitSha")
            if not sha:
                sha = await _thread(remote_head, body["repository"], body["branchName"], self.settings.temp_root)
                if sha is None:
                    raise ValueError("source branch does not exist")
            await _thread(service.pin_source, body["buildId"], sha, job["lease_token"])
            await _thread(self.jobs.phase, job["job_id"], job["lease_token"], "FETCHING_SOURCE")
            binding = ProjectBinding(job["project_id"], body["repository"], ".", cast(str, job["profile_binding_id"]))
            directory, digest = await _thread(resolve_commit, binding, sha, self.settings.temp_root)
            try:
                source = await _thread(self.artifacts.capture, job["project_id"], directory, metadata={
                    "source_commit_sha": sha, "project_digest": digest, "config_version": job["config_version"],
                    "toolchain_version": job["toolchain_version"]})
            finally:
                await _thread(shutil.rmtree, directory)
            if not await _thread(self.jobs.attach_input, job["job_id"], job["lease_token"], source,
                                     project_digest=digest):
                raise ExecutionError(ERROR_LEASE_LOST, stopped=True)
            return cast(LeasedJob, cast(StoredJob, await _thread(self.jobs.get, job["job_id"])) | {
                "attempt_id": job["attempt_id"], "lease_token": job["lease_token"]})
        except (ValueError, OSError) as error:
            raise ExecutionError("SOURCE_UNAVAILABLE", stopped=True) from error

    async def _command(self, job: LeasedJob, runner: JobRunner, spec: CommandSpec, phase: str) -> JobRecord:
        # 提前记录外部执行；失去租约的节点绝不再启动子进程。
        external = spec.write_operation or job["kind"] in {METRIC_QUERY, QUERY_OPTIONS, RUN_CLEANUP}
        valid = await _thread(self.jobs.phase, job["job_id"], job["lease_token"], phase, external=external)
        if not valid:
            raise ExecutionError(ERROR_LEASE_LOST, stopped=False)
        submitted = await runner.submit(job["project_id"], spec)
        record = await runner.wait(submitted.id)
        if record.status is not JobStatus.SUCCEEDED:
            timed_out = record.status is JobStatus.TIMED_OUT
            # parse/debug 的正常退出无需仓库写入核对，其他写命令失联不能据退出码释放保护。
            may_write = spec.write_operation or job["kind"] == RUN_CLEANUP or (
                job["kind"] == DBT_COMMAND and job["request_json"].get("command") not in LOCAL_DBT_COMMANDS
            )
            raise ExecutionError(
                ERROR_COMMAND_TIMEOUT if timed_out else ERROR_COMMAND_FAILED,
                record.model_dump(mode=JSON_MODE),
                stopped=not timed_out and not may_write,
            )
        return record

    async def _programmatic(
        self, job: LeasedJob, runner: JobRunner, project: Path, attempt: Path, body: JsonObject,
    ) -> JsonObject:
        # 控制文件位于项目之外，产物采集不会收集查询参数和结果。
        input_path, output_path = attempt / INPUT_FILE, attempt / OUTPUT_FILE
        input_path.write_text(json.dumps(body), encoding=UTF8)
        spec = build_programmatic_command(
            project, self.settings.profiles_dir, cast(str, job["schema_name"]), cast(str, job["profile_binding_id"]),
            input_path, output_path,
        )
        try:
            await self._command(job, runner, spec, VALIDATING)
        except ExecutionError as error:
            # 子进程只对已验证的选项参数错误写出稳定代码，其他错误仍保留原诊断。
            if job["kind"] == QUERY_OPTIONS and output_path.is_file():
                with output_path.open("rb") as stream:
                    raw = stream.read(self.settings.max_result_bytes + 1)
                try:
                    diagnostic = json.loads(raw) if len(raw) <= self.settings.max_result_bytes else {}
                except ValueError:
                    diagnostic = {}
                if isinstance(diagnostic, dict) and diagnostic.get("errorCode") == ERROR_INVALID_QUERY:
                    raise ExecutionError(ERROR_INVALID_QUERY) from error
            raise
        # 先有界读取再解析，空字典也是有效的清理结果。
        with output_path.open("rb") as stream:
            raw = stream.read(self.settings.max_result_bytes + 1)
        if len(raw) > self.settings.max_result_bytes:
            raise ExecutionError(ERROR_RESULT_TOO_LARGE)
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ExecutionError(ERROR_RESULT_INVALID)
        return value

    @staticmethod
    def _metadata(job: LeasedJob) -> JsonObject:
        """发布元数据只引用持久集合与配置，不保存绝对路径。"""

        return {
            "source_set_id": job.get("input_set_id"),
            "config_version": job["config_version"],
            "toolchain_version": job["toolchain_version"],
            "source_commit_sha": job["request_json"].get("commitSha"),
            "project_digest": job["request_json"].get("projectDigest"),
        }
