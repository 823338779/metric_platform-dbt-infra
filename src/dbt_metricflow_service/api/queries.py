"""实时维度与同步查询 HTTP 边界；结果仅在本次响应中返回。"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from uuid import UUID

from fastapi import APIRouter, Depends

from dbt_metricflow_service.application.queries import QueryService

from ..models.queries import AvailableDimensionsRequest, AvailableDimensionsView, QueryRequest, QueryResult


def router(service: QueryService, guard: Callable[..., Awaitable[str]]) -> APIRouter:
    dependency = Depends(guard)
    routes = APIRouter(prefix="/v3/query")

    @routes.post("/available-dimensions", response_model=AvailableDimensionsView)
    def available_dimensions(
        request: AvailableDimensionsRequest, build_id: UUID, caller: str=dependency,
    ) -> AvailableDimensionsView:
        return service.available_dimensions(build_id, request, caller)

    @routes.post("", response_model=QueryResult)
    def submit(
        request: QueryRequest, build_id: UUID, caller: str=dependency,
    ) -> QueryResult:
        return service.submit(build_id, request, caller)

    return routes
