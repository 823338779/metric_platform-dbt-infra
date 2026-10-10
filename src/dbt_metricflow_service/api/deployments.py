"""部署 HTTP 边界，不承担分支管理或审批。"""

from fastapi import APIRouter, Depends, Query

from ..models.builds import Environment, Page
from ..models.deployments import DeploymentAttemptView, DeploymentKey, DeploymentRequest, DeploymentTargetView


def router(service, guard):
    dependency = Depends(guard)
    routes = APIRouter(prefix="/v3/deployments")

    @routes.post("", response_model=DeploymentAttemptView, status_code=202)
    def submit(request: DeploymentRequest, caller=dependency):
        return service.submit(request, caller)

    @routes.get("/current", response_model=DeploymentTargetView)
    def current(repository: str, environment: Environment, branchName: str):
        return service.current(DeploymentKey(repository=repository, environment=environment, branchName=branchName))

    @routes.get("", response_model=Page[DeploymentAttemptView])
    def history(
        repository: str,
        environment: Environment | None = None,
        branchName: str | None = None,
        cursor: str | None = None,
        limit: int = Query(default=50, ge=1, le=200),
    ):
        return service.list(repository, environment, branchName, cursor, limit)

    return routes
