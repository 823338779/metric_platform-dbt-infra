"""封存资源的只读 HTTP 入口。"""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Query

from dbt_metricflow_service.application.catalog import CatalogService
from dbt_metricflow_service.models.payloads import JsonObject

from ..models.catalog import CatalogPage, ResourceLineageView, ResourceNativeView, ResourceSourceView, ResourceView


def router(service: CatalogService) -> APIRouter:
    routes = APIRouter(prefix="/v3/builds")

    @routes.get("/{build_id}/catalog", response_model=CatalogPage)
    def catalog(
        build_id: UUID,
        q: str = "",
        kind: str | None = None,
        cursor: str | None = None,
        limit: int = Query(default=50, ge=1, le=200),
    ) -> CatalogPage:
        return service.list(build_id, q, kind, cursor, limit)

    @routes.get("/{build_id}/resources/{resource_id}", response_model=ResourceView)
    def resource(build_id: UUID, resource_id: str) -> JsonObject:
        return service.resource(build_id, resource_id)

    @routes.get("/{build_id}/resources/{resource_id}/{view}",
                response_model=ResourceSourceView | ResourceLineageView | ResourceNativeView)
    def detail(build_id: UUID, resource_id: str, view: str) -> JsonObject:
        return service.resource(build_id, resource_id, view)

    return routes
