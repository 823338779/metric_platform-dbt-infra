"""固定构建的实时查询；请求和结果仅在当前调用中传递。"""

from __future__ import annotations

from collections.abc import Callable
from typing import Final, cast
from uuid import UUID

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.application.builds import BuildService
from dbt_metricflow_service.models.payloads import AvailableDimensions, DimensionOption
from dbt_metricflow_service.models.queries import (
    AvailableDimensionsRequest,
    AvailableDimensionsView,
    QueryRequest,
    QueryResult,
)
from dbt_metricflow_service.platform.models import DataQueryResult, DimensionQueryResult, PlatformQueryRequest
from dbt_metricflow_service.storage.records import StoredBuild

from ..platform.dimensions import DimensionBinding, map_available_dimensions
from ..platform.query_time import canonical_query, normalize_query_time, query_time_metadata
from .errors import ServiceError
from .query_plan import compile_query

QUERY_RESPONSE_LIMIT_MESSAGE: Final = "query response exceeds the size limit"
UNAVAILABLE_BUILD_MESSAGE: Final = "build is not available for new engine tasks"

MAX_RESPONSE_BYTES = 8 * 1024 * 1024


class QueryService:
    def __init__(
        self, builds: BuildService,
        execute: Callable[[StoredBuild, PlatformQueryRequest], DataQueryResult],
        discover_dimensions: Callable[[StoredBuild, tuple[str, ...]], DimensionQueryResult],
    ) -> None:
        self.builds = builds
        self.execute = execute
        self.discover_dimensions = discover_dimensions

    def _available(self, build: StoredBuild) -> None:
        code = self.builds.query_unavailable_reason(build)
        if code:
            status = (
                410 if code == strings.PHYSICAL_OBJECTS_REMOVED else 409 if code == strings.BUILD_NOT_SUCCEEDED else 503
            )
            raise ServiceError(code, UNAVAILABLE_BUILD_MESSAGE, status, build_id=build.build_id)

    def available_dimensions(
        self, build_id: UUID | str, request: AvailableDimensionsRequest, caller: str,
    ) -> AvailableDimensionsView:
        build = self.builds.load(build_id)
        self._available(build)
        selected = sorted(set(request.metrics))
        result, _ = self._available_dimensions(build, selected)
        return AvailableDimensionsView(
            build_id=result.build_id, metrics=result.metrics, dimensions=result.dimensions
        )

    def _available_dimensions(
        self, build: StoredBuild, selected: list[str],
    ) -> tuple[AvailableDimensions, dict[str, DimensionBinding]]:
        native = self.discover_dimensions(build, tuple(selected))
        return map_available_dimensions(build.build_id, selected, native)

    def submit(self, build_id: UUID | str, request: QueryRequest, caller: str) -> QueryResult:
        build = self.builds.load(build_id)
        self._available(build)
        timezone = build.config_snapshot.business_timezone
        request = normalize_query_time(request, timezone)
        body = canonical_query(request)
        dimensions: list[DimensionOption] = []
        mapping: dict[str, DimensionBinding] = {}
        if request.group_by or request.filters or request.dimension_option_id:
            available, mapping = self._available_dimensions(build, body.metrics)
            dimensions = available.dimensions
        engine = compile_query(request, cast(str, build.run_id), dimensions, mapping)
        payload = self.execute(build, engine)
        metadata = query_time_metadata(body, timezone)
        result = QueryResult(
            build_id=build.build_id, mode=request.mode, commit_sha=build.commit_sha,
            business_timezone=metadata.business_timezone, normalized_time_range=metadata.normalized_time_range,
            boundary_policy=metadata.boundary_policy, sql=payload.sql, columns=payload.columns,
            rows=payload.rows, result_truncated=payload.truncated,
        )
        if len(result.model_dump_json(by_alias=True).encode()) > MAX_RESPONSE_BYTES:
            raise ServiceError(strings.RESULT_TOO_LARGE, QUERY_RESPONSE_LIMIT_MESSAGE, build_id=build.build_id)
        return result
