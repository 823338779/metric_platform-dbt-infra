"""外部执行快照的构建门面；服务不提供环境注册接口。"""

from collections.abc import Awaitable, Callable
from typing import Final
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse

from dbt_metricflow_service import constants as strings

from ..application.builds import BuildService
from ..application.errors import ServiceError
from ..models.builds import BuildStatus, ExecutionLogPage, Page
from ..models.execution import SnapshotBuildRequest, SnapshotBuildView
from ..platform.execution_profile import validate_execution_target
from ..settings import Settings

EXECUTION_TARGET_UNVERIFIED_MESSAGE: Final[str] = "execution target is not verified"


PREFIX = ""


def router(service: BuildService, guard: Callable[..., Awaitable[str]], settings: Settings) -> APIRouter:
    routes = APIRouter(prefix=PREFIX)
    dependency = Depends(guard)

    @routes.post(strings.ROUTE_BUILDS, response_model=SnapshotBuildView, status_code=202)
    def submit(request: SnapshotBuildRequest, caller: str = dependency) -> SnapshotBuildView:
        try:
            validate_execution_target(request.execution, settings)
        except ValueError as error:
            raise ServiceError(str(error), EXECUTION_TARGET_UNVERIFIED_MESSAGE) from error
        return service.submit_snapshot(request, caller)

    @routes.get(strings.ROUTE_BUILDS, response_model=Page[SnapshotBuildView])
    def history(environmentId: UUID, repository: str | None = None, branchName: str | None = None,
                buildStatus: BuildStatus | None = None, cursor: str | None = None,
                limit: int = Query(default=50, ge=1, le=200)) -> Page[SnapshotBuildView]:
        return service.list_by_environment(environmentId, repository, branchName, buildStatus, cursor, limit)

    @routes.get(strings.ROUTE_BUILDS_BUILD_ID, response_model=SnapshotBuildView)
    def get(build_id: UUID) -> SnapshotBuildView:
        return service.snapshot_view(build_id)

    @routes.get(strings.ROUTE_BUILDS_BUILD_ID_EXECUTION_LOGS, response_model=ExecutionLogPage)
    def execution_logs(
        build_id: UUID, cursor: str | None = Query(default=None, max_length=4096),
        limit: int = Query(default=100, ge=1, le=200),
    ) -> ExecutionLogPage:
        """读取持久化 dbt 详细日志；空页仍返回游标，供前端增量轮询。"""
        return service.execution_logs(build_id, cursor, limit)

    @routes.post(strings.ROUTE_BUILDS_BUILD_ID_CANCEL, response_model=SnapshotBuildView, status_code=202)
    def cancel(build_id: UUID, caller: str = dependency) -> JSONResponse:
        view, status = service.cancel_snapshot(build_id, caller)
        return JSONResponse(view.model_dump(mode=strings.JSON_MODE, by_alias=True), status_code=status)

    return routes
