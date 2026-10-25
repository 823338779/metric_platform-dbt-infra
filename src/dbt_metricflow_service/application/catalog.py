"""直接读取固定构建的 Manifest，不重建资源模型或推导依赖。"""

from __future__ import annotations

from typing import Final, cast
from uuid import UUID

from pydantic import TypeAdapter

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.models.catalog import ResourceSourceView
from dbt_metricflow_service.models.payloads import JsonObject
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.builds import BuildStore
from dbt_metricflow_service.storage.records import StoredBuild

from .errors import ServiceError

INCOMPLETE_CATALOG_MESSAGE: Final = "complete catalog is not available"
RESOURCE_BUILD_CONFLICT_MESSAGE: Final = "resource does not belong to this build"
UNAVAILABLE_CATALOG_MESSAGE: Final = "sealed manifest is unavailable"
UNAVAILABLE_SOURCE_MESSAGE: Final = "sealed source is unavailable"
UNREADABLE_CATALOG_MESSAGE: Final = "sealed manifest cannot be read"
UNSUPPORTED_RESOURCE_VIEW_MESSAGE: Final = "unsupported resource view"

NODE_SECTIONS = (strings.DBT_METRICS_FIELD, strings.DBT_SEMANTIC_MODELS_FIELD,
                 strings.DBT_NODES_FIELD, strings.DBT_SOURCES_FIELD)
DEPENDENCY_SECTIONS = (strings.DBT_PARENT_MAP_FIELD, strings.DBT_CHILD_MAP_FIELD)
CATALOG_SECTIONS = (strings.DBT_METADATA_FIELD, *NODE_SECTIONS, *DEPENDENCY_SECTIONS)
JSON_OBJECT = TypeAdapter(JsonObject)


class CatalogService:
    def __init__(self, builds: BuildStore, artifacts: ArtifactStore) -> None:
        self.builds = builds
        self.artifacts = artifacts

    def read(self, build_id: UUID | str) -> tuple[StoredBuild, JsonObject]:
        build = self.builds.get(str(build_id))
        if not build:
            raise ServiceError(strings.BUILD_NOT_FOUND, strings.BUILD_NOT_FOUND_MESSAGE, 404)
        return build, self.read_build(build)

    def read_build(self, build: StoredBuild) -> JsonObject:
        """存储层负责解压和摘要校验，JSON 字段与封存内容保持一致。"""
        if build.build_status != strings.SUCCEEDED:
            raise ServiceError(strings.CATALOG_NOT_READY, INCOMPLETE_CATALOG_MESSAGE, 409)
        if not build.output_set_id:
            raise ServiceError(strings.ARTIFACT_UNAVAILABLE, UNAVAILABLE_CATALOG_MESSAGE, 503)
        try:
            content = self.artifacts.read_file(build.output_set_id, strings.MANIFEST_PATH)
            manifest = JSON_OBJECT.validate_json(content)
            if not isinstance(manifest.get(strings.DBT_NODES_FIELD), dict):
                raise ValueError(UNREADABLE_CATALOG_MESSAGE)
        except (ValueError, KeyError) as error:
            raise ServiceError(strings.ARTIFACT_UNAVAILABLE, UNREADABLE_CATALOG_MESSAGE, 503) from error
        return manifest

    def list(self, build_id: UUID | str) -> JsonObject:
        _, manifest = self.read(build_id)
        return {key: manifest.get(key, {}) for key in CATALOG_SECTIONS}

    def resource(
        self, build_id: UUID | str, resource_id: str, view: str | None = None,
    ) -> JsonObject | ResourceSourceView:
        build, manifest = self.read(build_id)
        resource = next((cast(dict[str, JsonObject], manifest.get(section, {}))[resource_id]
                         for section in NODE_SECTIONS if resource_id in manifest.get(section, {})), None)
        if resource is None:
            raise ServiceError(strings.RESOURCE_NOT_FOUND, RESOURCE_BUILD_CONFLICT_MESSAGE, 404)
        if view == strings.CATALOG_LINEAGE_VIEW:
            return {key: {resource_id: cast(JsonObject, manifest[key])[resource_id]}
                    if resource_id in manifest.get(key, {}) else {} for key in DEPENDENCY_SECTIONS}
        if view == strings.CATALOG_SOURCE_VIEW:
            path = cast(str | None, resource.get(strings.DBT_ORIGINAL_FILE_PATH_FIELD))
            job = self.builds.jobs.get(build.run_id)
            if not path or not job or not job.input_set_id:
                raise ServiceError(strings.SOURCE_UNAVAILABLE, UNAVAILABLE_SOURCE_MESSAGE, 404)
            path = path.replace("\\", "/")
            try:
                content = self.artifacts.read_file(job.input_set_id, path).decode(strings.UTF8)
            except (ValueError, KeyError) as error:
                raise ServiceError(strings.SOURCE_UNAVAILABLE, UNAVAILABLE_SOURCE_MESSAGE, 503) from error
            return ResourceSourceView(build_id=build_id, resource_id=resource_id, path=path, content=content)
        if view not in (None, strings.CATALOG_NATIVE_DETAILS_VIEW):
            raise ServiceError(strings.INVALID_RESOURCE_VIEW, UNSUPPORTED_RESOURCE_VIEW_MESSAGE)
        return resource
