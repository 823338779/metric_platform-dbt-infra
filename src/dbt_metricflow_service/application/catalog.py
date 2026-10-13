"""只读取封存资源，不调用引擎、不读取有效部署指针。"""

from __future__ import annotations

import hashlib
from typing import Final
from uuid import UUID

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.models.artifacts import PublishedCatalog
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.builds import BuildStore
from dbt_metricflow_service.storage.records import StoredBuild

from ..models.catalog import CatalogPage, ResourceLineageView, ResourceNativeView, ResourceSourceView, ResourceView
from ..storage.paging import page
from .errors import ServiceError

CATALOG_DIGEST_CONFLICT_MESSAGE: Final = "catalog digest differs from committed reference"
INCOMPLETE_CATALOG_MESSAGE: Final = "complete catalog is not available"
MISSING_BUILD_MESSAGE: Final = strings.BUILD_NOT_FOUND_MESSAGE
RESOURCE_BUILD_CONFLICT_MESSAGE: Final = "resource does not belong to this build"
UNAVAILABLE_CATALOG_MESSAGE: Final = "sealed catalog is unavailable"
UNAVAILABLE_SOURCE_MESSAGE: Final = "sealed source is unavailable"
UNREADABLE_CATALOG_MESSAGE: Final = "sealed catalog cannot be read"
UNSUPPORTED_RESOURCE_VIEW_MESSAGE: Final = "unsupported resource view"

CATALOG_PATH = strings.PUBLISHED_CATALOG_PATH
VIEWS = frozenset({strings.CATALOG_LINEAGE_VIEW, strings.CATALOG_SOURCE_VIEW, strings.CATALOG_NATIVE_DETAILS_VIEW})


class CatalogService:
    def __init__(self, builds: BuildStore, artifacts: ArtifactStore) -> None:
        # 历史读取仅需构建索引与可靠产物，不持有运行容器。
        self.builds = builds
        self.artifacts = artifacts

    def read(self, build_id: UUID | str) -> tuple[StoredBuild, PublishedCatalog]:
        build = self.builds.get(str(build_id))
        if not build:
            raise ServiceError(strings.BUILD_NOT_FOUND, MISSING_BUILD_MESSAGE, 404)
        return build, self.read_build(build)

    def read_build(self, build: StoredBuild) -> PublishedCatalog:
        """读取已有构建快照的封存目录，仍校验产物字节及摘要。"""
        if build.build_status != strings.SUCCEEDED:
            raise ServiceError(strings.CATALOG_NOT_READY, INCOMPLETE_CATALOG_MESSAGE, 409)
        if not build.output_set_id:
            raise ServiceError(strings.ARTIFACT_UNAVAILABLE, UNAVAILABLE_CATALOG_MESSAGE, 503)
        try:
            content = self.artifacts.read_file(build.output_set_id, CATALOG_PATH)
            if build.catalog_digest and hashlib.sha256(content).hexdigest() != build.catalog_digest:
                raise ValueError(CATALOG_DIGEST_CONFLICT_MESSAGE)
            catalog = PublishedCatalog.model_validate_json(content)
        except (ValueError, KeyError) as error:
            raise ServiceError(strings.ARTIFACT_UNAVAILABLE, UNREADABLE_CATALOG_MESSAGE, 503) from error
        return catalog

    def list(
        self, build_id: UUID | str, q: str = "", kind: str | None = None, cursor: str | None = None, limit: int = 50
    ) -> CatalogPage:
        _, catalog = self.read(build_id)
        resources = [
            item
            for item in catalog.resources
            if (kind is None or item.kind == kind)
            and (not q or any(q.casefold() in (value or "").casefold()
                              for value in (item.name, item.display_name, item.description)))
        ]
        try:
            items, next_cursor = page(
                resources,
                scope=[str(build_id), q, kind],
                cursor=cursor,
                limit=limit,
                identity=lambda item: item.resource_id,
            )
        except ValueError as error:
            raise ServiceError(strings.INVALID_CURSOR, str(error)) from error
        return CatalogPage(build_id=build_id, items=items, next_cursor=next_cursor)

    def resource(
        self, build_id: UUID | str, resource_id: str, view: str | None = None,
    ) -> ResourceView | ResourceSourceView | ResourceLineageView | ResourceNativeView:
        build, catalog = self.read(build_id)
        resource = next((item for item in catalog.resources if item.resource_id == resource_id), None)
        if resource is None:
            raise ServiceError(strings.RESOURCE_NOT_FOUND, RESOURCE_BUILD_CONFLICT_MESSAGE, 404)
        if view is not None and view not in VIEWS:
            raise ServiceError(strings.INVALID_RESOURCE_VIEW, UNSUPPORTED_RESOURCE_VIEW_MESSAGE)
        if view == strings.CATALOG_LINEAGE_VIEW:
            return ResourceLineageView(build_id=build_id, resource_id=resource_id,
                dependencies=[edge for edge in catalog.relations
                              if resource_id in (edge.upstream_resource_id, edge.downstream_resource_id)])
        if view == strings.CATALOG_NATIVE_DETAILS_VIEW:
            return ResourceNativeView(
                build_id=build_id, resource_id=resource_id, native_details=resource.native_details
            )
        if view == strings.CATALOG_SOURCE_VIEW:
            path = resource.source_path
            job = self.builds.jobs.get(build.run_id)
            if not path or not job or not job.input_set_id:
                raise ServiceError(strings.SOURCE_UNAVAILABLE, UNAVAILABLE_SOURCE_MESSAGE, 404)
            try:
                content = self.artifacts.read_file(job.input_set_id, path).decode()
            except (ValueError, KeyError) as error:
                raise ServiceError(strings.SOURCE_UNAVAILABLE, UNAVAILABLE_SOURCE_MESSAGE, 503) from error
            return ResourceSourceView(build_id=build_id, resource_id=resource_id, path=path, content=content)
        return ResourceView(build_id=build_id, resource_id=resource.resource_id, native_id=resource.native_id,
            name=resource.name, display_name=resource.display_name, description=resource.description,
            source_path=resource.source_path, capabilities=resource.capabilities,
            native_details=resource.native_details,
            kind=resource.kind, attributes=resource.attributes)
