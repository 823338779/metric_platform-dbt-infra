"""新环境部署门面复用部署事实，不创建环境配置。"""

from collections.abc import Awaitable, Callable
from uuid import UUID

from fastapi import APIRouter, Depends, Query

from ..application.deployments import DeploymentService
from ..application.errors import ServiceError
from ..models.builds import Page
from ..models.deployments import (
    DeploymentRequest,
    SnapshotDeploymentAttemptView,
    SnapshotDeploymentKey,
    SnapshotDeploymentTargetView,
)
from ..storage.deployments import attempt_view


def router(service: DeploymentService, guard: Callable[..., Awaitable[str]]) -> APIRouter:
    routes = APIRouter(prefix="/v4")
    dependency = Depends(guard)

    @routes.post("/deployments", response_model=SnapshotDeploymentAttemptView, status_code=202)
    def submit(request: DeploymentRequest, caller: str = dependency) -> SnapshotDeploymentAttemptView:
        build = service.build_service.load(request.build_id)
        if build.get("protocol_version", 3) != 4:
            raise ServiceError("PROTOCOL_VERSION_MISMATCH", "use the original build deployment protocol", 409)
        return service.submit(request, caller)

    @routes.get("/deployments/current", response_model=SnapshotDeploymentTargetView)
    def current(repository: str, environmentId: UUID, branchName: str) -> SnapshotDeploymentTargetView:
        return service.current(SnapshotDeploymentKey(repository=repository, environment=str(environmentId),
                                                     branch_name=branchName))

    @routes.get("/deployments", response_model=Page[SnapshotDeploymentAttemptView])
    def history(repository: str, environmentId: UUID, branchName: str | None = None,
                cursor: str | None = None, limit: int = Query(default=50, ge=1, le=200)
                ) -> Page[SnapshotDeploymentAttemptView]:
        try:
            rows, following = service.store.list(repository, str(environmentId), branchName, cursor, limit)
        except ValueError as error:
            raise ServiceError("INVALID_CURSOR", str(error)) from error
        return Page[SnapshotDeploymentAttemptView](items=[attempt_view(row) for row in rows], next_cursor=following)

    return routes
