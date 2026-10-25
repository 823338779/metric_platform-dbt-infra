"""固定构建的 dbt 原生目录与节点只读入口。"""

from __future__ import annotations

from collections.abc import Callable
from uuid import UUID

from fastapi import APIRouter

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.application.catalog import CatalogService
from dbt_metricflow_service.storage.records import StoredBuild

from ..models.catalog import ResourceView
from ..models.payloads import JsonObject


def router(
    service: CatalogService, execute_lineage: Callable[[StoredBuild, str], list[JsonObject]],
) -> APIRouter:
    routes = APIRouter(prefix=strings.ROUTE_BUILDS)

    @routes.get(strings.ROUTE_BUILD_ID_CATALOG, response_model=JsonObject)
    def catalog(build_id: UUID) -> JsonObject:
        return service.list(build_id)

    @routes.get(strings.ROUTE_BUILD_ID_LINEAGE, response_model=list[JsonObject])
    def lineage(build_id: UUID, selector: str) -> list[JsonObject]:
        build, _ = service.read(build_id)
        return execute_lineage(build, selector)

    @routes.get(strings.ROUTE_BUILD_ID_RESOURCES_RESOURCE_ID, response_model=ResourceView)
    def resource(build_id: UUID, resource_id: str) -> ResourceView:
        return service.resource(build_id, resource_id)

    return routes
