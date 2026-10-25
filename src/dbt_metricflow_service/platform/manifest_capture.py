"""dbt Manifest 的捕获、持久化与原文字节传递。"""

from __future__ import annotations

import json
from itertools import chain
from pathlib import Path
from typing import TYPE_CHECKING, Final

import yaml
from dbt_common.utils.encoding import JSONEncoder

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.models.native import NativeProject

if TYPE_CHECKING:
    from dbt.contracts.graph.manifest import Manifest

    from dbt_metricflow_service.storage.artifacts import ArtifactStore
    from dbt_metricflow_service.storage.records import ArtifactMetadata

MANIFEST_UNAVAILABLE: Final[str] = "dbt manifest is unavailable"
MANIFEST_TOO_LARGE: Final[str] = "manifest exceeds artifact file size limit"
ARTIFACT_CONTEXT: Final[str] = "artifact"
RUNTIME_PATH_FIELDS: Final = ("root_path", "compiled_path", "build_path")


class ManifestCapture:
    def __init__(self, max_file_bytes: int) -> None:
        self.max_file_bytes = max_file_bytes
        self._content: bytes | None = None
        self._manifest: Manifest | None = None

    def __call__(self, manifest: Manifest) -> None:
        # 每次命令使用独立实例；失败的捕获不保留旧快照。
        self._content, self._manifest = None, None
        payload = manifest.writable_manifest().to_dict(
            omit_none=False, context={ARTIFACT_CONTEXT: True},
        )
        # 仅清理产物节点的运行路径，不递归删除 meta/config 中用户定义的同名字段。
        resources = chain(
            payload["nodes"].values(), payload["functions"].values(),
            chain.from_iterable((payload.get("disabled") or {}).values()),
        )
        for resource in resources:
            for field in RUNTIME_PATH_FIELDS:
                resource.pop(field, None)
        content = json.dumps(payload, cls=JSONEncoder, ensure_ascii=False, separators=(",", ":")).encode(strings.UTF8)
        if len(content) > self.max_file_bytes:
            raise ValueError(MANIFEST_TOO_LARGE)
        self._manifest = manifest
        self._content = content

    @property
    def manifest(self) -> Manifest:
        if self._manifest is None:
            raise ValueError(MANIFEST_UNAVAILABLE)
        return self._manifest

    @property
    def content(self) -> bytes:
        if self._content is None:
            raise ValueError(MANIFEST_UNAVAILABLE)
        return self._content

    def persist(
        self, artifacts: ArtifactStore, project_id: str, project: Path, *,
        producer_attempt_id: str | None = None, metadata: ArtifactMetadata | None = None,
    ) -> str:
        """将内存快照和工程作为同一产物集合保存，不读取磁盘 Manifest。"""
        content = self.content
        config = NativeProject.model_validate(yaml.safe_load((project / strings.DBT_PROJECT_FILE).read_bytes()))
        target = config.target_path or strings.TARGET_DIRECTORY
        path = f"{target}/{strings.MANIFEST_FILE}"
        return artifacts.capture(
            project_id, project, producer_attempt_id=producer_attempt_id, kind=strings.EXECUTION,
            metadata=metadata, memory_files={path: content},
        )

    @staticmethod
    def read(artifacts: ArtifactStore, set_id: str) -> bytes:
        """仅传递已解压并校验完整性的原文，不解析 Manifest 内容。"""
        return artifacts.read_file(set_id, strings.MANIFEST_PATH)
