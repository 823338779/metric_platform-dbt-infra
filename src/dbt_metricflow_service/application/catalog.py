"""直接读取固定构建的 Manifest，不重建资源模型或推导依赖。"""

from __future__ import annotations

from typing import Final
from uuid import UUID

from dbt_common.exceptions import DbtInternalError

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.delegates.clients.dbt_client import DbtClient
from dbt_metricflow_service.models.catalog import ResourceSourceView
from dbt_metricflow_service.models.payloads import JsonObject
from dbt_metricflow_service.platform.manifest_capture import ManifestCapture
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.builds import BuildStore
from dbt_metricflow_service.storage.records import StoredBuild

from .errors import ServiceError

INCOMPLETE_CATALOG_MESSAGE: Final = "complete catalog is not available"
RESOURCE_BUILD_CONFLICT_MESSAGE: Final = "resource does not belong to this build"
UNAVAILABLE_CATALOG_MESSAGE: Final = "sealed manifest is unavailable"
UNAVAILABLE_SOURCE_MESSAGE: Final = "sealed source is unavailable"
UNREADABLE_CATALOG_MESSAGE: Final = "sealed manifest cannot be read"


class CatalogService:
    def __init__(self, builds: BuildStore, artifacts: ArtifactStore) -> None:
        self.builds = builds
        self.artifacts = artifacts

    def read(self, build_id: UUID | str) -> tuple[StoredBuild, bytes]:
        build = self.builds.get(str(build_id))
        if not build:
            raise ServiceError(strings.BUILD_NOT_FOUND, strings.BUILD_NOT_FOUND_MESSAGE, 404)
        return build, self.read_build(build)

    def read_build(self, build: StoredBuild) -> bytes:
        """存储层负责解压和摘要校验，JSON 字段与封存内容保持一致。"""
        if build.build_status != strings.SUCCEEDED:
            raise ServiceError(strings.CATALOG_NOT_READY, INCOMPLETE_CATALOG_MESSAGE, 409)
        if not build.output_set_id:
            raise ServiceError(strings.ARTIFACT_UNAVAILABLE, UNAVAILABLE_CATALOG_MESSAGE, 503)
        try:
            manifest = ManifestCapture.read(self.artifacts, build.output_set_id)
        except (ValueError, KeyError) as error:
            raise ServiceError(strings.ARTIFACT_UNAVAILABLE, UNREADABLE_CATALOG_MESSAGE, 503) from error
        return manifest

    def list(self, build_id: UUID | str) -> JsonObject:
        _, manifest = self.read(build_id)
        try:
            return DbtClient(manifest).catalog()
        except Exception as error:
            raise ServiceError(strings.ARTIFACT_UNAVAILABLE, UNREADABLE_CATALOG_MESSAGE, 503) from error

    def _resource(
        self, build_id: UUID | str, resource_id: str,
    ) -> tuple[StoredBuild, DbtClient, JsonObject]:
        """详情和源码共用固定构建节点校验，不提供通用视图分派。"""
        build, manifest = self.read(build_id)
        client = DbtClient(manifest)
        try:
            resource = client.resource(resource_id)
        except DbtInternalError as error:
            raise ServiceError(strings.RESOURCE_NOT_FOUND, RESOURCE_BUILD_CONFLICT_MESSAGE, 404) from error
        except Exception as error:
            raise ServiceError(strings.ARTIFACT_UNAVAILABLE, UNREADABLE_CATALOG_MESSAGE, 503) from error
        return build, client, resource

    def resource(self, build_id: UUID | str, resource_id: str) -> JsonObject:
        _, _, resource = self._resource(build_id, resource_id)
        return resource

    def source(self, build_id: UUID | str, resource_id: str) -> ResourceSourceView:
        build, client, _ = self._resource(build_id, resource_id)
        path = client.source_path(resource_id)
        job = self.builds.jobs.get(build.run_id)
        if not path or not job or not job.input_set_id:
            raise ServiceError(strings.SOURCE_UNAVAILABLE, UNAVAILABLE_SOURCE_MESSAGE, 404)
        try:
            content = self.artifacts.read_file(job.input_set_id, path).decode(strings.UTF8)
        except (ValueError, KeyError) as error:
            raise ServiceError(strings.SOURCE_UNAVAILABLE, UNAVAILABLE_SOURCE_MESSAGE, 503) from error
        return ResourceSourceView(build_id=build_id, resource_id=resource_id, path=path, content=content)
