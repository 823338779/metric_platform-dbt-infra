"""部署 HTTP 边界，不承担分支管理或审批。"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from fastapi import APIRouter, Depends, Query

from dbt_metricflow_service.application.deployments import DeploymentService

from ..models.builds import Environment, Page
from ..models.deployments import DeploymentAttemptView, DeploymentKey, DeploymentRequest, DeploymentTargetView


def router(service: DeploymentService, guard: Callable[..., Awaitable[str]]) -> APIRouter:
    dependency = Depends(guard)
    routes = APIRouter(prefix="/v3/deployments")

    @routes.post("", response_model=DeploymentAttemptView, status_code=202)
    def submit(request: DeploymentRequest, caller: str=dependency) -> DeploymentAttemptView:
        return service.submit(request, caller)

    @routes.get("/current", response_model=DeploymentTargetView)
    def current(repository: str, environment: Environment, branchName: str) -> DeploymentTargetView:
        return service.current(
            DeploymentKey.model_validate(
                {"repository": repository, "environment": environment, "branchName": branchName}
            )
        )

    @routes.get("", response_model=Page[DeploymentAttemptView])
    def history(
        repository: str,
        environment: Environment | None = None,
        branchName: str | None = None,
        cursor: str | None = None,
        limit: int = Query(default=50, ge=1, le=200),
    ) -> Page[DeploymentAttemptView]:
        return service.list(repository, environment, branchName, cursor, limit)

    return routes
