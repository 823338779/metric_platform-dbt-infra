"""固定构建的实时查询；请求和结果仅在当前调用中传递。"""

from __future__ import annotations

from collections.abc import Callable
from typing import cast
from uuid import UUID

from dbt_metricflow_service.application.builds import BuildService
from dbt_metricflow_service.application.catalog import CatalogService
from dbt_metricflow_service.models.payloads import AvailableDimensions, DimensionOption, JsonObject
from dbt_metricflow_service.models.queries import (
    AvailableDimensionsRequest,
    AvailableDimensionsView,
    QueryRequest,
    QueryResult,
)
from dbt_metricflow_service.storage.records import StoredBuild

from ..platform.dimensions import DimensionBinding, map_available_dimensions
from ..platform.query_time import canonical_query, normalize_query_time, query_time_metadata
from .errors import ServiceError
from .query_plan import compile_query

MAX_RESPONSE_BYTES = 8 * 1024 * 1024


class QueryService:
    def __init__(
        self, builds: BuildService, catalogs: CatalogService,
        execute: Callable[[StoredBuild, JsonObject], JsonObject],
    ) -> None:
        self.builds = builds
        self.catalogs = catalogs
        self.execute = execute

    def _available(self, build: StoredBuild) -> None:
        code = self.builds.query_unavailable_reason(build)
        if code:
            status = 410 if code == "PHYSICAL_OBJECTS_REMOVED" else 409 if code == "BUILD_NOT_SUCCEEDED" else 503
            raise ServiceError(code, "build is not available for new engine tasks", status, build_id=build["build_id"])

    def available_dimensions(
        self, build_id: UUID | str, request: AvailableDimensionsRequest, caller: str,
    ) -> AvailableDimensionsView:
        build = self.builds.load(build_id)
        self._available(build)
        catalog = self.catalogs.read_build(build)
        selected = sorted(set(request.metric_resource_ids))
        result, _ = self._available_dimensions(build, selected, catalog)
        return AvailableDimensionsView.model_validate(result)

    @staticmethod
    def _metrics(catalog: JsonObject, selected: list[str]) -> dict[str, JsonObject]:
        indexed = {item["resourceId"]: item for item in catalog["resources"]}
        if any(
            key not in indexed or indexed[key]["kind"] != "METRIC" or "QUERY" not in indexed[key]["capabilities"]
            for key in selected
        ):
            raise ServiceError("INVALID_METRIC_RESOURCE", "selected metric does not belong to this build")
        return indexed

    def _available_dimensions(
        self, build: StoredBuild, selected: list[str], catalog: JsonObject,
    ) -> tuple[AvailableDimensions, dict[str, DimensionBinding]]:
        indexed = self._metrics(catalog, selected)
        native = self.execute(build, {"mode": "OPTIONS", "metrics": [indexed[key]["name"] for key in selected]})
        return map_available_dimensions(build["build_id"], selected, catalog, native)

    def submit(self, build_id: UUID | str, request: QueryRequest, caller: str) -> QueryResult:
        build = self.builds.load(build_id)
        self._available(build)
        timezone = build["config_snapshot"]["businessTimezone"]
        request = normalize_query_time(request, timezone)
        body = canonical_query(request)
        catalog = self.catalogs.read_build(build)
        indexed = self._metrics(catalog, body["metricResourceIds"])
        dimensions: list[DimensionOption] = []
        mapping: dict[str, DimensionBinding] = {}
        if request.group_by or request.filters or request.dimension_option_id:
            available, mapping = self._available_dimensions(build, body["metricResourceIds"], catalog)
            dimensions = available["dimensions"]
        engine = compile_query(request, cast(str, build["run_id"]), indexed, dimensions, mapping)
        payload = self.execute(build, {"mode": "QUERY", "request": engine.model_dump(mode="json", by_alias=True)})
        result = QueryResult.model_validate({
            "buildId": build["build_id"], "mode": request.mode, "commitSha": build["commit_sha"],
            **query_time_metadata(body, timezone),
            "sql": payload.get("sql"), "columns": payload.get("columns", []), "rows": payload.get("rows", []),
            "resultTruncated": payload.get("truncated", False),
        })
        if len(result.model_dump_json(by_alias=True).encode()) > MAX_RESPONSE_BYTES:
            raise ServiceError("RESULT_TOO_LARGE", "query response exceeds the size limit", build_id=build["build_id"])
        return result
