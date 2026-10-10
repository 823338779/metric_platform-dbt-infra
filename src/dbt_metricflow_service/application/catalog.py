"""只读取封存资源，不调用引擎、不读取有效部署指针。"""

from __future__ import annotations

import hashlib
import json
from uuid import UUID

from dbt_metricflow_service.models.payloads import JsonObject
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.builds import BuildStore
from dbt_metricflow_service.storage.records import StoredBuild

from ..models.catalog import CatalogPage
from ..storage.paging import page
from .errors import ServiceError

CATALOG_PATH = "target/published_catalog.json"
VIEWS = frozenset({"lineage", "source", "native-details"})
SEARCH_FIELDS = ("name", "displayName", "description")


class CatalogService:
    def __init__(self, builds: BuildStore, artifacts: ArtifactStore) -> None:
        # 历史读取仅需构建索引与可靠产物，不持有运行容器。
        self.builds = builds
        self.artifacts = artifacts

    def read(self, build_id: UUID | str) -> tuple[StoredBuild, JsonObject]:
        build = self.builds.get(str(build_id))
        if not build:
            raise ServiceError("BUILD_NOT_FOUND", "build does not exist", 404)
        if build["build_status"] != "SUCCEEDED":
            raise ServiceError("CATALOG_NOT_READY", "complete catalog is not available", 409)
        if not build["output_set_id"]:
            raise ServiceError("ARTIFACT_UNAVAILABLE", "sealed catalog is unavailable", 503)
        try:
            content = self.artifacts.read_file(build["output_set_id"], CATALOG_PATH)
            if build["catalog_digest"] and hashlib.sha256(content).hexdigest() != build["catalog_digest"]:
                raise ValueError("catalog digest differs from committed reference")
            catalog = json.loads(content)
            if not isinstance(catalog.get("resources"), list):
                raise ValueError("invalid catalog")
        except (ValueError, KeyError) as error:
            raise ServiceError("ARTIFACT_UNAVAILABLE", "sealed catalog cannot be read", 503) from error
        return build, catalog

    def list(
        self, build_id: UUID | str, q: str = "", kind: str | None = None, cursor: str | None = None, limit: int = 50
    ) -> CatalogPage:
        _, catalog = self.read(build_id)
        resources = [
            item
            for item in catalog["resources"]
            if (kind is None or item["kind"] == kind)
            and (not q or any(q.casefold() in (item.get(name) or "").casefold() for name in SEARCH_FIELDS))
        ]
        try:
            items, next_cursor = page(
                resources,
                scope=[str(build_id), q, kind],
                cursor=cursor,
                limit=limit,
                identity=lambda item: item["resourceId"],
            )
        except ValueError as error:
            raise ServiceError("INVALID_CURSOR", str(error)) from error
        return CatalogPage.model_validate({'build_id': build_id, 'items': items, 'next_cursor': next_cursor})

    def resource(self, build_id: UUID | str, resource_id: str, view: str | None=None) -> JsonObject:
        build, catalog = self.read(build_id)
        resource = next((item for item in catalog["resources"] if item["resourceId"] == resource_id), None)
        if resource is None:
            raise ServiceError("RESOURCE_NOT_FOUND", "resource does not belong to this build", 404)
        if view is not None and view not in VIEWS:
            raise ServiceError("INVALID_RESOURCE_VIEW", "unsupported resource view")
        envelope = {"buildId": str(build_id), "resourceId": resource_id}
        if view == "lineage":
            return {
                **envelope,
                "dependencies": [
                    edge
                    for edge in catalog["relations"]
                    if resource_id in (edge["upstreamResourceId"], edge["downstreamResourceId"])
                ],
            }
        if view == "native-details":
            return {**envelope, "nativeDetails": resource["nativeDetails"]}
        if view == "source":
            path = resource.get("sourcePath")
            job = self.builds.jobs.get(build["run_id"])
            if not path or not job or not job["input_set_id"]:
                raise ServiceError("SOURCE_UNAVAILABLE", "sealed source is unavailable", 404)
            try:
                content = self.artifacts.read_file(job["input_set_id"], path).decode()
            except (ValueError, KeyError) as error:
                raise ServiceError("SOURCE_UNAVAILABLE", "sealed source is unavailable", 503) from error
            return {**envelope, "path": path, "content": content}
        return {**envelope, **resource}
