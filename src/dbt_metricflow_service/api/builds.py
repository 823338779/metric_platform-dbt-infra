"""外部执行快照的构建门面；服务不提供环境注册接口。"""

from uuid import UUID

from fastapi import APIRouter, Query

from dbt_metricflow_service import constants as strings

from ..application.builds import BuildService
from ..models.builds import BuildRequest, BuildStatus, BuildView, ExecutionLogPage
from ..models.contracts import Page

PREFIX = ""


def router(service: BuildService) -> APIRouter:
    routes = APIRouter(prefix=PREFIX)

    @routes.post(strings.ROUTE_BUILDS, response_model=BuildView, status_code=202, summary="提交异步 dbt 构建")
    def submit(request: BuildRequest) -> BuildView:
        """提交仓库、分支、可选提交 SHA 和执行环境快照，返回 HTTP 202 和可用于跟踪进度的 `buildId`。

        每个新构建使用新的 UUID 作为 `idempotencyKey`。重复提交同一 UUID 返回首次受理的构建，即使参数有变化。
        默认仅构建。指定 `deploymentPolicy=ON_SUCCESS` 时，服务会在构建成功后尝试部署。
        """
        # FastAPI 已用 BuildRequest 校验请求体及 UUID 格式。HTTP 入口以固定服务身份记录提交来源。
        # BuildStore.accept 按幂等键复用原构建或写入一条排队中的构建。BuildService.submit 组装 202 响应。
        # Worker._slot 领取任务，BuildExecutor 执行 dbt；ON_SUCCESS 的部署由 DeploymentService.reconcile 处理。
        return service.submit(request, strings.AUTH_SERVICE_IDENTITY)

    @routes.get(strings.ROUTE_BUILDS, response_model=Page[BuildView], summary="列出环境的构建历史")
    def history(environmentId: UUID, repository: str | None = None, branchName: str | None = None,
                buildStatus: BuildStatus | None = None, cursor: str | None = None,
                limit: int = Query(default=50, ge=1, le=200)) -> Page[BuildView]:
        """按必填的 `environmentId` 查询构建，可按仓库、分支或构建状态筛选。
        结果按创建时间从新到旧排列，使用 `nextCursor` 续页。
        """
        return service.list_by_environment(environmentId, repository, branchName, buildStatus, cursor, limit)

    @routes.get(strings.ROUTE_BUILDS_BUILD_ID, response_model=BuildView, summary="查看单次构建的状态和结果")
    def get(build_id: UUID) -> BuildView:
        """按构建 ID 查看执行快照、实际提交 SHA、构建状态、当前阶段及错误摘要。
        `catalogAvailable` 表示该构建的目录是否可读，`initialDeployment` 给出随构建提交的部署尝试及其状态。
        """
        return service.get(build_id)

    @routes.get(strings.ROUTE_BUILDS_BUILD_ID_EXECUTION_LOGS, response_model=ExecutionLogPage,
                summary="增量读取构建执行日志")
    def execution_logs(
        build_id: UUID, cursor: str | None = Query(default=None, max_length=4096),
        limit: int = Query(default=100, ge=1, le=200),
    ) -> ExecutionLogPage:
        """按日志 ID 从小到大返回该构建已落库的 dbt 事件。
        `hasMore` 表示当前还有日志可读。即使本页为空，也可将 `nextCursor` 作为下次请求的 `cursor` 继续轮询。
        """
        return service.execution_logs(build_id, cursor, limit)

    return routes
