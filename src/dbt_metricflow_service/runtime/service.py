"""PostgreSQL 运行时的受理与读取边界，不依赖实例本地项目目录。"""
from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Iterator
from contextlib import contextmanager
from importlib.metadata import version
from pathlib import Path
from typing import TYPE_CHECKING, Final, cast
from uuid import uuid4

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.application.errors import ServiceError
from dbt_metricflow_service.execution.embedded import create_output, engine_environment, run_serialized
from dbt_metricflow_service.platform.execution_profile import ExecutionConfigurationError
from dbt_metricflow_service.platform.models import DataQueryResult, DimensionQueryResult, PlatformQueryRequest
from dbt_metricflow_service.runtime.executor import build_engine_environment
from dbt_metricflow_service.runtime.workspace import materialized_workspace
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.jobs import CleanupBlocked, JobStore
from dbt_metricflow_service.storage.postgres import Database
from dbt_metricflow_service.storage.records import StoredBuild

EXECUTION_CONFIGURATION_UNAVAILABLE: Final[str] = "execution configuration unavailable"
BUILD_CLEANUP_IN_PROGRESS: Final[str] = "build is being cleaned"
QUERY_EXECUTION_TIMED_OUT: Final[str] = "query execution timed out"
QUERY_EXECUTION_FAILED: Final[str] = "query execution failed"
INVALID_QUERY_RESULT: Final[str] = "query returned an invalid result"
QUERY_RESULT_TOO_LARGE: Final[str] = "query result exceeds the size limit"


if TYPE_CHECKING:
    from dbt_metricflow_service.application.deployments import DeploymentService


TOOLCHAIN_PACKAGES = (
    strings.DBT_CORE_PACKAGE,
    strings.METRICFLOW_PACKAGE,
    strings.DBT_STARROCKS_PACKAGE,
    strings.DBT_DUCKDB_PACKAGE,
    strings.DBT_POSTGRES_PACKAGE,
)


def current_toolchain() -> str:
    """安装版本和服务代码共同决定执行兼容标识，避免不同代码误领旧任务。"""
    digest = hashlib.sha256()
    for package in TOOLCHAIN_PACKAGES:
        digest.update(f"{package}={version(package)}\n".encode())
    # runtime 已归入子包，兼容标识仍必须覆盖整个服务源码。
    source_root = Path(__file__).resolve().parents[1]
    for source in sorted(source_root.rglob(strings.PYTHON_GLOB)):
        digest.update(source.relative_to(source_root).as_posix().encode())
        digest.update(source.read_bytes().replace(b"\r\n", b"\n"))
    return digest.hexdigest()


async def _before_query_start() -> None:
    # 同步查询无持久租约；构建保护在取得串行槽位之后、执行线程内部获取。
    return None


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
        self.worker = None

    def available_dimensions(self, build: StoredBuild, metrics: tuple[str, ...]) -> DimensionQueryResult:
        """查询所选指标的维度能力，返回专用元数据结果。"""
        from ..platform.metricflow import execute_available_dimensions

        def invoke() -> DimensionQueryResult | ServiceError:
            try:
                with self._query_context(build) as (project, profiles):
                    return execute_available_dimensions(project, profiles, metrics=metrics)
            except ServiceError as error:
                # 将引擎错误带回调用线程，超时发生时优先丢弃迟到结果或错误。
                return error

        value, timed_out = asyncio.run(run_serialized(
            invoke, self.settings.command_timeout_seconds, _before_query_start,
        ))
        if timed_out:
            raise ServiceError(strings.COMMAND_TIMEOUT, QUERY_EXECUTION_TIMED_OUT, 504, build_id=build.build_id)
        if isinstance(value, ServiceError):
            raise value
        if not isinstance(value, DimensionQueryResult):
            raise ServiceError(strings.RESULT_INVALID, INVALID_QUERY_RESULT, 503, build_id=build.build_id)
        self._check_query_size(build, value)
        return value

    def execute_query(self, build: StoredBuild, request: PlatformQueryRequest) -> DataQueryResult:
        """执行数据请求，返回数据行或 SQL。"""
        from ..platform.metricflow import execute_programmatic

        def invoke() -> DataQueryResult | ServiceError:
            try:
                with self._query_context(build) as (project, profiles):
                    return execute_programmatic(project, profiles, request=request)
            except ServiceError as error:
                return error

        value, timed_out = asyncio.run(run_serialized(
            invoke, self.settings.command_timeout_seconds, _before_query_start,
        ))
        if timed_out:
            raise ServiceError(strings.COMMAND_TIMEOUT, QUERY_EXECUTION_TIMED_OUT, 504, build_id=build.build_id)
        if isinstance(value, ServiceError):
            raise value
        if not isinstance(value, DataQueryResult):
            raise ServiceError(strings.RESULT_INVALID, INVALID_QUERY_RESULT, 503, build_id=build.build_id)
        self._check_query_size(build, value)
        return value

    @contextmanager
    def _query_context(self, build: StoredBuild) -> Iterator[tuple[Path, Path]]:
        """在持有引擎串行锁的执行线程内获取资源，底层调用退出后才清理。"""
        from metricflow_semantics.errors.error_classes import DimensionValueMappingError

        from ..platform.execution_profile import materialized_profile
        from ..platform.metricflow import InvalidDimensionsRequest

        try:
            with self.jobs.query_run(cast(str, build.run_id)) as parent:
                with materialized_workspace(
                    self.artifacts, cast(str, build.output_set_id), self.settings.temp_root,
                    str(uuid4()), str(uuid4()),
                ) as project:
                    with materialized_profile(build.config_snapshot, self.settings.temp_root,
                                              self.settings, project) as profiles:
                        environment = build_engine_environment(
                            project, profiles, cast(str, parent.schema_name), cast(str, parent.profile_binding_id),
                        )
                        output = create_output(self.settings.max_output_bytes)
                        try:
                            with engine_environment(environment, output):
                                yield project, profiles
                        except (InvalidDimensionsRequest, DimensionValueMappingError):
                            raise ServiceError(strings.INVALID_QUERY, QUERY_EXECUTION_FAILED, 422,
                                               build_id=build.build_id) from None
                        except Exception:
                            raise ServiceError(strings.COMMAND_FAILED, QUERY_EXECUTION_FAILED, 503,
                                               build_id=build.build_id) from None
                        finally:
                            output.finish()
        except ExecutionConfigurationError as error:
            raise ServiceError(error.code, EXECUTION_CONFIGURATION_UNAVAILABLE, 503, retryable=True,
                               build_id=build.build_id) from error
        except CleanupBlocked as error:
            raise ServiceError(strings.PHYSICAL_OBJECTS_REMOVED, BUILD_CLEANUP_IN_PROGRESS, 410,
                               build_id=build.build_id) from error

    def _check_query_size(self, build: StoredBuild, value: DimensionQueryResult | DataQueryResult) -> None:
        """两类结果使用同一 JSON 编码和 UTF-8 大小上限。"""
        size = 0
        try:
            for chunk in json.JSONEncoder(ensure_ascii=False).iterencode(
                value.model_dump(mode=strings.PYTHON_MODE, by_alias=True, exclude_unset=True),
            ):
                size += len(chunk.encode())
                if size > self.settings.max_result_bytes:
                    raise ServiceError(strings.RESULT_TOO_LARGE, QUERY_RESULT_TOO_LARGE, 422,
                                       build_id=build.build_id)
        except (TypeError, ValueError, RecursionError) as error:
            raise ServiceError(strings.COMMAND_FAILED, QUERY_EXECUTION_FAILED, 503, build_id=build.build_id) from error


    async def start(self, *, deployments: DeploymentService | None = None) -> None:
        # 本地目录只保存当前执行，可由空目录启动。
        from dbt_metricflow_service.runtime.worker import Worker
        self.settings.temp_root.mkdir(parents=True, exist_ok=True)
        self.worker = Worker(self, deployments=deployments)
        self.worker.start()


    async def close(self) -> None:
        if self.worker is not None:
            await self.worker.close()
        await asyncio.to_thread(self.db.close)


