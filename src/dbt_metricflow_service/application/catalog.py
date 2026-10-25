"""直接读取固定构建的 Manifest，不重建资源模型或推导依赖。"""

from __future__ import annotations

from collections.abc import Callable
from typing import Final
from uuid import UUID

from dbt_common.exceptions import DbtInternalError

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.engines.dbt import DbtClient
from dbt_metricflow_service.models.catalog import ResourceSourceView, ResourceView
from dbt_metricflow_service.models.contracts import JsonObject
from dbt_metricflow_service.platform.manifest_capture import ManifestCapture
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.builds import BuildStore
from dbt_metricflow_service.storage.records import StoredBuild

from .errors import ServiceError
from .results import check_result_size

INCOMPLETE_CATALOG_MESSAGE: Final = "complete catalog is not available"
RESOURCE_BUILD_CONFLICT_MESSAGE: Final = "resource does not belong to this build"
UNAVAILABLE_CATALOG_MESSAGE: Final = "sealed manifest is unavailable"
UNAVAILABLE_SOURCE_MESSAGE: Final = "sealed source is unavailable"
UNREADABLE_CATALOG_MESSAGE: Final = "sealed manifest cannot be read"


class CatalogService:
    def __init__(
        self, builds: BuildStore, artifacts: ArtifactStore,
        execute_lineage: Callable[[StoredBuild, str, bytes], list[JsonObject]], max_result_bytes: int,
    ) -> None:
        self.builds = builds
        self.artifacts = artifacts
        self.execute_lineage = execute_lineage
        self.max_result_bytes = max_result_bytes

    def lineage(self, build_id: UUID | str, selector: str) -> list[JsonObject]:
        build, manifest = self.read(build_id)
        result = self.execute_lineage(build, selector, manifest)
        check_result_size(result, self.max_result_bytes, build.build_id)
        return result

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

    def list(self, build_id: UUID | str) -> list[JsonObject]:
        # 复用 dbt 原生 ls，以封存 Manifest 为上下文列出白名单内的业务资源。
        return self.lineage(build_id, "*")

    def resource(self, build_id: UUID | str, resource_id: str) -> ResourceView:
        # 一次读取固定构建的 Manifest，取得完整节点和对应源码路径。
        build, manifest = self.read(build_id)
        client = DbtClient(manifest)
        try:
            resource = client.resource(resource_id)
            path = client.source_path(resource_id)
        except DbtInternalError as error:
            raise ServiceError(strings.RESOURCE_NOT_FOUND, RESOURCE_BUILD_CONFLICT_MESSAGE, 404) from error
        except Exception as error:
            raise ServiceError(strings.ARTIFACT_UNAVAILABLE, UNREADABLE_CATALOG_MESSAGE, 503) from error

        # 无源码证据时仍返回节点；已有快照读取失败则保留明确错误，不掩盖损坏。
        source = None
        job = self.builds.jobs.get(build.run_id) if path else None
        if job and job.input_set_id:
            try:
                content = self.artifacts.read_file(job.input_set_id, path).decode(strings.UTF8)
            except (ValueError, KeyError) as error:
                raise ServiceError(strings.SOURCE_UNAVAILABLE, UNAVAILABLE_SOURCE_MESSAGE, 503) from error
            source = ResourceSourceView(path=path, content=content)
        return ResourceView(build_id=build_id, resource_id=resource_id, resource=resource, source=source)
