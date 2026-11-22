"""固定构建的 dbt 原生目录与节点只读入口。"""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Query

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.application.catalog import CatalogService

from ..models.catalog import ResourceSummary, ResourceView
from ..models.contracts import JsonObject, Page


def router(service: CatalogService) -> APIRouter:
    routes = APIRouter(prefix=strings.ROUTE_BUILDS)

    @routes.get(strings.ROUTE_BUILD_ID_CATALOG, response_model=list[JsonObject], summary="列出构建的业务资源目录")
    def catalog(build_id: UUID) -> list[JsonObject]:
        """从成功构建封存的 Manifest 中列出模型、指标、语义模型等业务资源，一次返回 dbt ls 的原生节点数组。
        需要按关键词或类型分页查找时，使用同一构建的 `catalog/search`。
        """
        return service.list(build_id)

    @routes.get(strings.ROUTE_BUILD_ID_CATALOG + "/search", summary="搜索构建目录中的资源")
    def search(build_id: UUID, keyword: str | None = None, resourceId: str | None = None,
               resourceType: str | None = None, cursor: str | None = None,
               limit: int = Query(default=50, ge=1, le=200)) -> Page[ResourceSummary]:
        """在指定构建中选择一种查找方式：用 `keyword` 匹配资源 ID、名称、标签或描述中的文本（忽略大小写），
        或用 `resourceId` 精确查找。两者都可用 `resourceType` 限定类型。
        返回分页资源摘要。关键词无匹配时返回空列表，精确查找无匹配时返回 404。
        """
        return service.search(build_id, keyword, resourceId, resourceType, cursor, limit)

    @routes.get(strings.ROUTE_BUILD_ID_LINEAGE, response_model=list[JsonObject], summary="按 dbt 选择器查看资源血缘")
    def lineage(build_id: UUID, selector: str) -> list[JsonObject]:
        """在指定构建中执行 dbt 图选择表达式，返回匹配的业务节点数组。
        例如 `+orders` 选择 orders 及其上游，`orders+` 选择 orders 及其下游。
        """
        return service.lineage(build_id, selector)

    @routes.get(strings.ROUTE_BUILD_ID_RESOURCES_RESOURCE_ID, response_model=ResourceView, summary="读取资源定义和源码")
    def resource(build_id: UUID, resource_id: str) -> ResourceView:
        """按 dbt 资源唯一 ID 返回该构建 Manifest 中的完整节点 `resource`。
        有对应的源码快照时，`source` 包含原始文件路径和内容，可用于核对模型 SQL 或指标定义。
        """
        return service.resource(build_id, resource_id)

    return routes
