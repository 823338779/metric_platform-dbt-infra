"""历史快照查询、维度和血缘执行，不依赖实例本地项目目录。"""
from __future__ import annotations

import asyncio
import json
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Final, cast
from uuid import uuid4

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.application.errors import ServiceError
from dbt_metricflow_service.delegates.clients import InvalidDimensionsRequest
from dbt_metricflow_service.delegates.clients.dbt_client import DbtClient
from dbt_metricflow_service.delegates.clients.metric_flow_client import MetricFlowClient
from dbt_metricflow_service.execution.embedded import create_output, engine_environment, run_serialized
from dbt_metricflow_service.execution.environment import build_engine_environment
from dbt_metricflow_service.execution.workspace import materialized_workspace
from dbt_metricflow_service.models.execution import SNAPSHOT_TARGET
from dbt_metricflow_service.models.payloads import JsonObject
from dbt_metricflow_service.platform.execution_profile import ExecutionConfigurationError
from dbt_metricflow_service.platform.manifest_capture import ManifestCapture
from dbt_metricflow_service.platform.models import (
    DataQueryResult,
    DimensionQueryResult,
    PlatformQueryRequest,
    QueryMode,
)
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.records import StoredBuild

EXECUTION_CONFIGURATION_UNAVAILABLE: Final[str] = "execution configuration unavailable"
QUERY_EXECUTION_FAILED: Final[str] = "query execution failed"
INVALID_QUERY_RESULT: Final[str] = "query returned an invalid result"
QUERY_RESULT_TOO_LARGE: Final[str] = "query result exceeds the size limit"


async def _before_query_start() -> None:
    # 同步查询不使用持久任务租约。
    return None


class QueryExecutor:
    """从历史快照组装上下文，直接调用 dbt / MetricFlow client。"""

    def __init__(self, settings: Settings, artifacts: ArtifactStore) -> None:
        self.settings = settings
        self.artifacts = artifacts

    def available_dimensions(self, build: StoredBuild, metrics: tuple[str, ...]) -> DimensionQueryResult:
        """查询所选指标的维度能力，返回专用元数据结果。"""
        def invoke() -> DimensionQueryResult | ServiceError:
            try:
                with self._engine_context(build, build.config_snapshot.schema_name, SNAPSHOT_TARGET) as context:
                    manifest, project, profiles = context
                    client = MetricFlowClient(manifest, project=project, profiles=profiles)
                    return client.available_dimensions(metrics=metrics)
            except ServiceError as error:
                # 将引擎错误带回调用线程。
                return error

        value = asyncio.run(run_serialized(
            invoke, _before_query_start,
        ))
        if isinstance(value, ServiceError):
            raise value
        if not isinstance(value, DimensionQueryResult):
            raise ServiceError(strings.RESULT_INVALID, INVALID_QUERY_RESULT, 503, build_id=build.build_id)
        self._check_query_size(build, value)
        return value

    def execute_query(self, build: StoredBuild, request: PlatformQueryRequest) -> DataQueryResult:
        """执行数据请求，返回数据行或 SQL。"""
        def invoke() -> DataQueryResult | ServiceError:
            try:
                with self._engine_context(build, build.config_snapshot.schema_name, SNAPSHOT_TARGET) as context:
                    manifest, project, profiles = context
                    if request.mode is QueryMode.PREVIEW:
                        return DbtClient(manifest).preview(
                            cast(str, request.selector), project=project, profiles=profiles, limit=request.limit,
                        )
                    return MetricFlowClient(manifest, project=project, profiles=profiles).query(request=request)
            except ServiceError as error:
                return error

        value = asyncio.run(run_serialized(
            invoke, _before_query_start,
        ))
        if isinstance(value, ServiceError):
            raise value
        if not isinstance(value, DataQueryResult):
            raise ServiceError(strings.RESULT_INVALID, INVALID_QUERY_RESULT, 503, build_id=build.build_id)
        self._check_query_size(build, value)
        return value

    def lineage(self, build: StoredBuild, selector: str) -> list[JsonObject]:
        """元数据读取不依赖物化表存活。"""
        def invoke() -> list[JsonObject] | ServiceError:
            try:
                with self._engine_context(build, build.config_snapshot.schema_name, SNAPSHOT_TARGET) as context:
                    manifest, project, profiles = context
                    return DbtClient(manifest).lineage(selector, project=project, profiles=profiles)
            except ServiceError as error:
                return error

        value = asyncio.run(run_serialized(
            invoke, _before_query_start,
        ))
        if isinstance(value, ServiceError):
            raise value
        self._check_query_size(build, value)
        return value

    @contextmanager
    def _engine_context(self, build: StoredBuild, schema: str, target: str) -> Iterator[tuple[bytes, Path, Path]]:
        """在引擎串行锁内准备全新的项目/profile，退出时销毁，不依赖旧临时目录。"""
        from metricflow_semantics.errors.error_classes import DimensionValueMappingError

        from ..platform.execution_profile import materialized_profile

        try:
            with materialized_workspace(
                self.artifacts, cast(str, build.output_set_id), self.settings.temp_root,
                str(uuid4()), str(uuid4()),
            ) as project:
                with materialized_profile(build.config_snapshot, self.settings.temp_root,
                                          self.settings, project) as profiles:
                    environment = build_engine_environment(project, profiles, schema, target)
                    output = create_output(self.settings.max_output_bytes)
                    try:
                        with engine_environment(environment, output):
                            manifest = ManifestCapture.read(self.artifacts, cast(str, build.output_set_id))
                            yield manifest, project, profiles
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

    def _check_query_size(
        self, build: StoredBuild, value: DimensionQueryResult | DataQueryResult | list[JsonObject],
    ) -> None:
        """两类结果使用同一 JSON 编码和 UTF-8 大小上限。"""
        size = 0
        try:
            payload = value if isinstance(value, list) else value.model_dump(
                mode=strings.PYTHON_MODE, by_alias=True, exclude_unset=True,
            )
            for chunk in json.JSONEncoder(ensure_ascii=False).iterencode(payload):
                size += len(chunk.encode())
                if size > self.settings.max_result_bytes:
                    raise ServiceError(strings.RESULT_TOO_LARGE, QUERY_RESULT_TOO_LARGE, 422,
                                       build_id=build.build_id)
        except (TypeError, ValueError, RecursionError) as error:
            raise ServiceError(strings.COMMAND_FAILED, QUERY_EXECUTION_FAILED, 503, build_id=build.build_id) from error
