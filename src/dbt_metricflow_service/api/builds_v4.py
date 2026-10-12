"""外部执行快照的构建门面；服务不提供环境注册接口。"""

from collections.abc import Awaitable, Callable
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse

from ..application.builds import BuildService
from ..application.errors import ServiceError
from ..models.builds import BuildStatus, LogPage, Page
from ..models.execution import SnapshotBuildRequest, SnapshotBuildView
from ..platform.execution_profile import validate_execution_target
from ..settings import Settings

PREFIX = "/v4"


def router(service: BuildService, guard: Callable[..., Awaitable[str]], settings: Settings) -> APIRouter:
    routes = APIRouter(prefix=PREFIX)
    dependency = Depends(guard)

    @routes.post("/builds", response_model=SnapshotBuildView, status_code=202)
    def submit(request: SnapshotBuildRequest, caller: str = dependency) -> SnapshotBuildView:
        try:
            validate_execution_target(request.execution, settings)
        except ValueError as error:
            raise ServiceError(str(error), "execution target is not verified") from error
        return service.submit_snapshot(request, caller)

    @routes.get("/builds", response_model=Page[SnapshotBuildView])
    def history(environmentId: UUID, repository: str | None = None, branchName: str | None = None,
                buildStatus: BuildStatus | None = None, cursor: str | None = None,
                limit: int = Query(default=50, ge=1, le=200)) -> Page[SnapshotBuildView]:
        return service.list_by_environment(environmentId, repository, branchName, buildStatus, cursor, limit)

    @routes.get("/builds/{build_id}", response_model=SnapshotBuildView)
    def get(build_id: UUID) -> SnapshotBuildView:
        return service.snapshot_view(build_id)

    @routes.get("/builds/{build_id}/logs", response_model=LogPage)
    def logs(build_id: UUID, cursor: str | None = None) -> LogPage:
        return service.logs(build_id, cursor)

    @routes.post("/builds/{build_id}/cancel", response_model=SnapshotBuildView, status_code=202)
    def cancel(build_id: UUID, caller: str = dependency) -> JSONResponse:
        view, status = service.cancel_snapshot(build_id, caller)
        return JSONResponse(view.model_dump(mode="json", by_alias=True), status_code=status)

    return routes
