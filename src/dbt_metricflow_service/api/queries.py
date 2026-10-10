"""异步选项与查询 HTTP 边界。"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from uuid import UUID

from fastapi import APIRouter, Depends, Query

from dbt_metricflow_service.application.queries import QueryService

from ..models.queries import OptionsRequest, OptionsTaskView, QueryRequest, QueryView, ResultPage


def router(service: QueryService, guard: Callable[..., Awaitable[str]]) -> APIRouter:
    dependency = Depends(guard)
    routes = APIRouter(prefix="/v3")

    @routes.post("/builds/{build_id}/query-options", response_model=OptionsTaskView, status_code=202)
    def options(build_id: UUID, request: OptionsRequest, caller: str=dependency) -> OptionsTaskView:
        return service.submit_options(build_id, request, caller)

    @routes.get("/query-options/{options_task_id}", response_model=OptionsTaskView)
    def get_options(options_task_id: UUID) -> OptionsTaskView:
        return service.get_options(options_task_id)

    @routes.post("/builds/{build_id}/queries", response_model=QueryView, status_code=202)
    def submit(build_id: UUID, request: QueryRequest, caller: str=dependency) -> QueryView:
        return service.submit(build_id, request, caller)

    @routes.get("/queries/{query_id}/status", response_model=QueryView)
    def status(query_id: UUID) -> QueryView:
        return service.status(query_id)

    @routes.get("/queries/{query_id}/results", response_model=ResultPage)
    def results(
        query_id: UUID, offset: int = Query(default=0, ge=0), limit: int = Query(default=100, ge=1, le=200)
    ) -> ResultPage:
        return service.results(query_id, offset, limit)

    return routes
