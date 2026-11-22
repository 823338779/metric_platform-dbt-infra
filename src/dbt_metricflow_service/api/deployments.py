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
    @routes.get(strings.ROUTE_DEPLOYMENTS_CURRENT, response_model=DeploymentTargetView,
                summary="查看分支当前生效的部署")
    def current(
        repository: str,
        environmentId: UUID,
        branchName: str,
        refreshSource: bool = Query(False, description="主动刷新远端分支状态，默认返回最近观察值"),
    ) -> DeploymentTargetView:
        """按仓库、环境和分支读取当前生效的 `activeBuildId`、提交 SHA 及最近一次部署尝试。
        尚无生效部署时 `activeBuildId` 为空。默认返回已记录的远端分支状态，传入 `refreshSource=true` 可主动刷新。
        """
        key = DeploymentKey(repository=repository, environment=str(environmentId), branch_name=branchName)
        return service.current(key, refresh_source=refreshSource)

    @routes.get(strings.ROUTE_DEPLOYMENTS, response_model=Page[DeploymentAttemptView], summary="列出部署尝试历史")
    def history(repository: str, environmentId: UUID, branchName: str | None = None,
                cursor: str | None = None, limit: int = Query(default=50, ge=1, le=200)
                ) -> Page[DeploymentAttemptView]:
        """按仓库和环境分页读取部署尝试，可指定分支。每条记录包含关联构建、部署状态及原因，
        可用于追踪哪些构建已部署、仍在等待或未能生效。
        """
        return service.list(repository, str(environmentId), branchName, cursor, limit)

    return routes
