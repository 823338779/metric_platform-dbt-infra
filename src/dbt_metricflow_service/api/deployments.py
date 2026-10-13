"""新环境部署门面复用部署事实，不创建环境配置。"""

from collections.abc import Awaitable, Callable
from uuid import UUID

from fastapi import APIRouter, Depends, Query

from dbt_metricflow_service import constants as strings

from ..application.deployments import DeploymentService
from ..application.errors import ServiceError
from ..models.builds import Page
from ..models.deployments import (
    DeploymentAttemptView,
    DeploymentKey,
    DeploymentRequest,
    DeploymentTargetView,
)
from ..storage.deployments import attempt_view


def router(service: DeploymentService, guard: Callable[..., Awaitable[str]]) -> APIRouter:
    routes = APIRouter(prefix="")
    dependency = Depends(guard)

    @routes.post(strings.ROUTE_DEPLOYMENTS, response_model=DeploymentAttemptView, status_code=202)
    def submit(request: DeploymentRequest, caller: str = dependency) -> DeploymentAttemptView:
        return service.submit(request, caller)

    @routes.get(strings.ROUTE_DEPLOYMENTS_CURRENT, response_model=DeploymentTargetView)
    def current(repository: str, environmentId: UUID, branchName: str) -> DeploymentTargetView:
        return service.current(DeploymentKey(repository=repository, environment=str(environmentId),
                                                     branch_name=branchName))

    @routes.get(strings.ROUTE_DEPLOYMENTS, response_model=Page[DeploymentAttemptView])
    def history(repository: str, environmentId: UUID, branchName: str | None = None,
                cursor: str | None = None, limit: int = Query(default=50, ge=1, le=200)
                ) -> Page[DeploymentAttemptView]:
        try:
            rows, following = service.store.list(repository, str(environmentId), branchName, cursor, limit)
        except ValueError as error:
            raise ServiceError(strings.INVALID_CURSOR, str(error)) from error
        return Page[DeploymentAttemptView](items=[attempt_view(row) for row in rows], next_cursor=following)

    return routes
