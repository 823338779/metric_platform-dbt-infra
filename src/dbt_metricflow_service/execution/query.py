"""历史快照查询、维度和血缘执行；每个请求只创建一次引擎上下文。"""
from __future__ import annotations

import asyncio
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from metricflow_semantics.errors.error_classes import DimensionValueMappingError

from dbt_metricflow_service import constants as strings

from ..application.bound_query_plan import compile_bound_query
from ..application.errors import ServiceError
from ..application.query_plan import compile_query, resolve_dimensions
from ..engines import InvalidDimensionsRequest
from ..engines.dbt import DbtClient
from ..engines.metricflow import MetricFlowClient
from ..engines.models import DataQueryResult, DimensionQueryResult
from ..models.contracts import JsonObject
from ..models.execution import SNAPSHOT_TARGET
from ..models.queries import BoundDimension, DimensionValuesRequest, MetricQueryRequest, PreviewRequest, QueryRequest
from ..platform.execution_profile import ExecutionConfigurationError, materialized_profile
from ..platform.manifest_capture import ManifestCapture
from ..settings import Settings
from ..storage.artifacts import ArtifactStore
from ..storage.records import StoredBuild
from .embedded import create_output, engine_environment, run_serialized
from .environment import build_engine_environment
from .workspace import materialized_workspace


class QueryExecutor:
    def __init__(self, settings: Settings, artifacts: ArtifactStore) -> None:
        self.settings = settings
        self.artifacts = artifacts

    def available_dimensions(self, build: StoredBuild, metrics: tuple[str, ...]) -> DimensionQueryResult:
        def invoke() -> DimensionQueryResult:
            try:
                # 每次请求直接读取该构建封存的 Manifest，不还原工程或生成 profile。
                manifest = ManifestCapture.read(self.artifacts, build.output_set_id)
                return MetricFlowClient(manifest).available_dimensions(metrics)
            except ServiceError:
                raise
            except (InvalidDimensionsRequest, DimensionValueMappingError) as error:
                raise ServiceError(strings.INVALID_QUERY, "query execution failed", 422,
                                   build_id=build.build_id) from error
            except Exception as error:
                raise ServiceError(strings.COMMAND_FAILED, "query execution failed", 503,
                                   build_id=build.build_id) from error
        return asyncio.run(run_serialized(invoke))

    def execute_query(self, build: StoredBuild, request: QueryRequest) -> DataQueryResult:
        def invoke() -> DataQueryResult:
            with self._engine_context(build) as (manifest, project, profiles):
                if isinstance(request, PreviewRequest):
                    return DbtClient(manifest).preview(
                        request.selector, project=project, profiles=profiles, limit=request.limit,
                    )
                client = MetricFlowClient(manifest, project=project, profiles=profiles)
                if isinstance(request, MetricQueryRequest) and any(
                    isinstance(group, BoundDimension) for group in request.group_by
                ):
                    per_metric = {
                        metric: resolve_dimensions(build.build_id, [metric], client.available_dimensions((metric,)))
                        for metric in request.metrics
                    }
                    common = {}
                    if request.filters or any(not isinstance(group, BoundDimension) for group in request.group_by):
                        common = resolve_dimensions(
                            build.build_id, request.metrics, client.available_dimensions(tuple(request.metrics)),
                        )
                    return client.query_bound(compile_bound_query(request, common, per_metric))
                dimensions = {}
                if isinstance(request, DimensionValuesRequest) or request.group_by or request.filters:
                    native = client.available_dimensions(tuple(request.metrics))
                    dimensions = resolve_dimensions(build.build_id, request.metrics, native)
                return client.query(compile_query(request, dimensions))
        return asyncio.run(run_serialized(invoke))

    def lineage(self, build: StoredBuild, selector: str, manifest: bytes) -> list[JsonObject]:
        def invoke() -> list[JsonObject]:
            with self._engine_context(build, manifest) as (content, project, profiles):
                return DbtClient(content).lineage(selector, project=project, profiles=profiles)
        return asyncio.run(run_serialized(invoke))

    @contextmanager
    def _engine_context(self, build: StoredBuild, manifest: bytes | None = None) -> Iterator[tuple[bytes, Path, Path]]:
        try:
            with materialized_workspace(
                self.artifacts, build.output_set_id, self.settings.temp_root, str(uuid4()), str(uuid4()),
            ) as project:
                with materialized_profile(build.config_snapshot, self.settings.temp_root,
                                          self.settings, project) as profiles:
                    environment = build_engine_environment(
                        project, profiles, build.config_snapshot.schema_name, SNAPSHOT_TARGET,
                    )
                    output = create_output(self.settings.max_output_bytes)
                    try:
                        with engine_environment(environment, output):
                            content = manifest if manifest is not None else ManifestCapture.read(
                                self.artifacts, build.output_set_id,
                            )
                            yield content, project, profiles
                    finally:
                        output.finish()
        except ServiceError:
            raise
        except ExecutionConfigurationError as error:
            raise ServiceError(error.code, "execution configuration unavailable", 503, retryable=True,
                               build_id=build.build_id) from error
        except (InvalidDimensionsRequest, DimensionValueMappingError) as error:
            raise ServiceError(strings.INVALID_QUERY, "query execution failed", 422,
                               build_id=build.build_id) from error
        except Exception as error:
            # 工作目录、产物、初始化和执行故障都保留服务错误，不冒充非法用户输入。
            raise ServiceError(strings.COMMAND_FAILED, "query execution failed", 503,
                               build_id=build.build_id) from error
