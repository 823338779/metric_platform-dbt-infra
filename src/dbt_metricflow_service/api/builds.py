"""构建 HTTP 边界，同步用例在线程池运行。"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse

from dbt_metricflow_service.application.builds import BuildService

from ..models.builds import BuildRequest, BuildView, ChangePage, LogPage, Page


def router(service: BuildService, guard: Callable[..., Awaitable[str]]) -> APIRouter:
    dependency = Depends(guard)
    routes = APIRouter(prefix="/v3")

    @routes.post("/builds", response_model=BuildView, status_code=202)
    def submit(request: BuildRequest, caller: str=dependency) -> BuildView:
        return service.submit(request, caller)

    @routes.get("/builds", response_model=Page[BuildView])
    def history(
        repository: str,
        branchName: str | None = None,
        cursor: str | None = None,
        limit: int = Query(default=50, ge=1, le=200),
    ) -> Page[BuildView]:
        return service.list(repository, branchName, cursor, limit)

    @routes.get("/builds/{build_id}", response_model=BuildView)
    def get(build_id: UUID) -> BuildView:
        return service.get(build_id)

    @routes.get("/builds/{build_id}/logs", response_model=LogPage)
    def logs(build_id: UUID, cursor: str | None = None) -> LogPage:
        return service.logs(build_id, cursor)

    @routes.post("/builds/{build_id}/cancel", response_model=BuildView, status_code=202)
    def cancel(build_id: UUID, caller: str=dependency) -> JSONResponse:
        view, status = service.cancel(build_id, caller)
        return JSONResponse(view.model_dump(mode="json", by_alias=True), status_code=status)

    @routes.get("/changes", response_model=ChangePage)
    def changes(cursor: str | None = None, limit: int = Query(default=50, ge=1, le=200)) -> ChangePage:
        return service.changes(cursor, limit)

    return routes
