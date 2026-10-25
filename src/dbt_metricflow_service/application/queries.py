"""固定构建的实时查询用例及按模式定义的公开响应。"""
from __future__ import annotations

from collections.abc import Callable
from uuid import UUID

from .. import constants as strings
from ..engines.models import DataQueryResult, DimensionQueryResult
from ..models.queries import (
    AvailableDimensionsRequest,
    AvailableDimensionsView,
    ExplainResult,
    MetricQueryResult,
    PreviewRequest,
    PreviewResult,
    QueryMode,
    QueryRequest,
    QueryResult,
)
from ..storage.records import StoredBuild
from .builds import BuildService
from .errors import ServiceError
from .query_plan import normalize_query, resolve_dimensions, time_range
from .results import check_result_size


class QueryService:
    def __init__(
        self, builds: BuildService,
        execute: Callable[[StoredBuild, QueryRequest], DataQueryResult],
        discover_dimensions: Callable[[StoredBuild, tuple[str, ...]], DimensionQueryResult],
        max_result_bytes: int,
    ) -> None:
        self.builds = builds
        self.execute = execute
        self.discover_dimensions = discover_dimensions
        self.max_result_bytes = max_result_bytes

    def available_dimensions(
        self, build_id: UUID | str, request: AvailableDimensionsRequest,
    ) -> AvailableDimensionsView:
        build = self.builds.load(build_id)
        metrics = sorted(set(request.metrics))
        native = self.discover_dimensions(build, tuple(metrics))
        dimensions = resolve_dimensions(build.build_id, metrics, native)
        result = AvailableDimensionsView(build_id=build.build_id, metrics=metrics,
                                         dimensions=[entry.option for entry in dimensions.values()])
        check_result_size(result, self.max_result_bytes, build.build_id)
        return result

    def submit(self, build_id: UUID | str, request: QueryRequest) -> QueryResult:
        build = self.builds.load(build_id)
        timezone = build.config_snapshot.business_timezone
        if not isinstance(request, PreviewRequest):
            request = normalize_query(request, timezone)
        payload = self.execute(build, request)
        result: QueryResult
        if isinstance(request, PreviewRequest):
            result = PreviewResult(build_id=build.build_id, commit_sha=build.commit_sha,
                                   columns=payload.columns, rows=payload.rows, result_truncated=payload.truncated)
        elif request.mode == QueryMode.EXPLAIN:
            if payload.sql is None:
                raise ServiceError(strings.RESULT_INVALID, "explain returned no SQL", 503, build_id=build.build_id)
            result = ExplainResult(build_id=build.build_id, commit_sha=build.commit_sha, sql=payload.sql,
                                   business_timezone=timezone, normalized_time_range=time_range(request))
        else:
            result = MetricQueryResult(build_id=build.build_id, commit_sha=build.commit_sha, mode=request.mode,
                                       business_timezone=timezone, normalized_time_range=time_range(request),
                                       columns=payload.columns, rows=payload.rows, result_truncated=payload.truncated)
        check_result_size(result, self.max_result_bytes, build.build_id)
        return result
