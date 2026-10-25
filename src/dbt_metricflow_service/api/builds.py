"""外部执行快照的构建门面；服务不提供环境注册接口。"""

from collections.abc import Awaitable, Callable
from uuid import UUID

from fastapi import APIRouter, Depends, Query

from dbt_metricflow_service import constants as strings

from ..application.builds import BuildService
from ..models.builds import BuildRequest, BuildStatus, BuildView, ExecutionLogPage
from ..models.contracts import Page

PREFIX = ""


def router(service: BuildService, guard: Callable[..., Awaitable[None]]) -> APIRouter:
    routes = APIRouter(prefix=PREFIX)
    dependency = Depends(guard)

    @routes.post(strings.ROUTE_BUILDS, response_model=BuildView, status_code=202)
    def submit(request: BuildRequest, _authenticated: None = dependency) -> BuildView:
        return service.submit(request, strings.AUTH_SERVICE_IDENTITY)

    @routes.get(strings.ROUTE_BUILDS, response_model=Page[BuildView])
    def history(environmentId: UUID, repository: str | None = None, branchName: str | None = None,
                buildStatus: BuildStatus | None = None, cursor: str | None = None,
                limit: int = Query(default=50, ge=1, le=200)) -> Page[BuildView]:
        return service.list_by_environment(environmentId, repository, branchName, buildStatus, cursor, limit)

    @routes.get(strings.ROUTE_BUILDS_BUILD_ID, response_model=BuildView)
    def get(build_id: UUID) -> BuildView:
        return service.get(build_id)

    @routes.get(strings.ROUTE_BUILDS_BUILD_ID_EXECUTION_LOGS, response_model=ExecutionLogPage)
    def execution_logs(
        build_id: UUID, cursor: str | None = Query(default=None, max_length=4096),
        limit: int = Query(default=100, ge=1, le=200),
    ) -> ExecutionLogPage:
        """读取持久化 dbt 详细日志；空页仍返回游标，供前端增量轮询。"""
        return service.execution_logs(build_id, cursor, limit)

    return routes
