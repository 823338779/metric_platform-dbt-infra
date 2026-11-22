"""新环境部署门面复用部署事实，不创建环境配置。"""

from uuid import UUID

from fastapi import APIRouter, Query

from dbt_metricflow_service import constants as strings

from ..application.deployments import DeploymentService
from ..models.contracts import Page
from ..models.deployments import (
    DeploymentAttemptView,
    DeploymentKey,
    DeploymentTargetView,
)


def router(service: DeploymentService) -> APIRouter:
    routes = APIRouter(prefix="")
    @routes.get(strings.ROUTE_DEPLOYMENTS_CURRENT, response_model=DeploymentTargetView)
    def current(
        repository: str,
        environmentId: UUID,
        branchName: str,
        refreshSource: bool = Query(False, description="主动刷新远端分支状态，默认返回最近观察值"),
    ) -> DeploymentTargetView:
        key = DeploymentKey(repository=repository, environment=str(environmentId), branch_name=branchName)
        return service.current(key, refresh_source=refreshSource)

    @routes.get(strings.ROUTE_DEPLOYMENTS, response_model=Page[DeploymentAttemptView])
    def history(repository: str, environmentId: UUID, branchName: str | None = None,
                cursor: str | None = None, limit: int = Query(default=50, ge=1, le=200)
                ) -> Page[DeploymentAttemptView]:
        return service.list(repository, str(environmentId), branchName, cursor, limit)

    return routes
