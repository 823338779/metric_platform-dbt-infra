"""实时维度与同步查询 HTTP 边界；结果仅在本次响应中返回。"""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.application.queries import QueryService

from ..models.queries import AvailableDimensionsRequest, AvailableDimensionsView, QueryRequest, QueryResult


def router(service: QueryService) -> APIRouter:
    routes = APIRouter(prefix=strings.ROUTE_QUERY)

    @routes.post(strings.ROUTE_AVAILABLE_DIMENSIONS, response_model=AvailableDimensionsView)
    def available_dimensions(
        request: AvailableDimensionsRequest, build_id: UUID,
    ) -> AvailableDimensionsView:
        return service.available_dimensions(build_id, request)

    @routes.post("", response_model=QueryResult)
    def submit(
        request: QueryRequest, build_id: UUID,
    ) -> QueryResult:
        return service.submit(build_id, request)

    return routes
