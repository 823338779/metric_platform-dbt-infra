"""直接读取固定构建的 Manifest，不重建资源模型或推导依赖。"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Final
from uuid import UUID

from dbt_common.exceptions import DbtInternalError

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.engines.dbt import BUSINESS_RESOURCE_TYPES, DbtClient
from dbt_metricflow_service.models.catalog import ResourceSourceView, ResourceSummary, ResourceView
from dbt_metricflow_service.models.contracts import JsonObject, Page
from dbt_metricflow_service.platform.manifest_capture import ManifestCapture
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.builds import BuildStore
from dbt_metricflow_service.storage.paging import page, write_cursor
from dbt_metricflow_service.storage.records import StoredBuild

from .errors import ServiceError
from .results import check_result_size

INCOMPLETE_CATALOG_MESSAGE: Final = "complete catalog is not available"
RESOURCE_BUILD_CONFLICT_MESSAGE: Final = "resource does not belong to this build"
RETIRED_OUTPUT_MESSAGE: Final = "sealed build output has been retired"
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
            raise ServiceError(strings.ARTIFACT_UNAVAILABLE, RETIRED_OUTPUT_MESSAGE, 410, build_id=build.build_id)
        try:
            manifest = ManifestCapture.read(self.artifacts, build.output_set_id)
        except (ValueError, KeyError) as error:
            raise ServiceError(strings.ARTIFACT_UNAVAILABLE, UNREADABLE_CATALOG_MESSAGE, 503) from error
        return manifest

    def list(self, build_id: UUID | str) -> list[JsonObject]:
        # 复用 dbt 原生 ls，以封存 Manifest 为上下文列出白名单内的业务资源。
        return self.lineage(build_id, "*")

    def search(self, build_id: UUID, keyword: str | None, resource_id: str | None, resource_type: str | None,
               cursor: str | None, limit: int) -> Page[ResourceSummary]:
        term = (keyword or "").strip().casefold()
        if (keyword is None) == (resource_id is None) or (keyword is not None and not term):
            raise ServiceError(strings.INVALID_REQUEST, "provide a keyword or an exact resource ID")
        if resource_type is not None and resource_type not in BUSINESS_RESOURCE_TYPES:
            raise ServiceError(strings.INVALID_REQUEST, "unsupported resource type")
        _, content = self.read(build_id)
        matches = []
        metric_names: list[str] = []
        try:
            manifest = json.loads(content)
            for section in ("nodes", "sources", "metrics", "semantic_models", "saved_queries", "exposures"):
                for uid, node in manifest.get(section, {}).items():
                    kind = node["resource_type"]
                    if kind not in BUSINESS_RESOURCE_TYPES or (resource_type and kind != resource_type):
                        continue
                    name = node["name"]
                    if resource_id is not None and kind == "metric":
                        metric_names.append(name)
                    if resource_id is not None and uid != resource_id:
                        continue
                    label, description = node.get("label"), node.get("description") or ""
                    if term and not any(term in field.casefold() for field in (uid, name, label or "", description)):
                        continue
                    matches.append(ResourceSummary(resource_id=uid, name=name, resource_type=kind,
                                                   label=label, description=description))
        except (ValueError, KeyError, TypeError, AttributeError) as error:
            raise ServiceError(strings.ARTIFACT_UNAVAILABLE, UNREADABLE_CATALOG_MESSAGE, 503) from error
        if resource_id is not None:
            if not matches:
                raise ServiceError(strings.RESOURCE_NOT_FOUND, RESOURCE_BUILD_CONFLICT_MESSAGE, 404)
            if matches[0].resource_type == "metric":
                matches[0].query_name_unique = metric_names.count(matches[0].name) == 1
        scope = [str(build_id), term, resource_type, resource_id]
        try:
            selected, following = page(matches, scope=scope, cursor=cursor, limit=limit,
                                       identity=lambda item: item.resource_id)
        except ValueError as error:
            raise ServiceError(strings.INVALID_CURSOR, strings.INVALID_CURSOR_MESSAGE) from error
        while True:
            result = Page[ResourceSummary](items=selected, next_cursor=following)
            try:
                check_result_size(result, self.max_result_bytes, str(build_id))
                return result
            except ServiceError:
                if len(selected) <= 1:
                    raise
                selected.pop()
                following = write_cursor(scope, selected[-1].resource_id)

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
