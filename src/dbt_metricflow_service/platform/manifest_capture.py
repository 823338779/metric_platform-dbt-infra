"""dbt Manifest 的捕获、持久化与原文字节传递。"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from itertools import chain
from pathlib import Path
from typing import TYPE_CHECKING, Final

import yaml
from dbt.artifacts.resources.v1.config import list_str
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
SPARSE_EDGE_MAPS: Final = "_dbt_service_sparse_edge_maps"
# WritableManifest v12 的这些顶层字段是映射；即使为空，部分字段仍是 SDK 构造函数的必填参数。
MANIFEST_MAPS: Final = (
    "nodes", "sources", "macros", "docs", "exposures", "metrics", "groups", "selectors", "disabled",
    "parent_map", "child_map", "group_map", "saved_queries", "semantic_models", "unit_tests", "functions",
)


def _omit_empty_defaults(native: object, data: object) -> None:
    """只省略 dbt 能按默认值还原的字段。字典条目和列表元素是业务数据，保留其键与位置。"""
    if isinstance(data, dict) and is_dataclass(native):
        for field in fields(native):
            key = field.metadata.get("alias", field.name)
            if key not in data:
                continue
            value = data[key]
            if isinstance(value, (list, dict)):
                member = getattr(native, field.name)
                _omit_empty_defaults(member, value)
                # 先精简子项，再判断当前字段能否整个省略。保留能区分显式空值的字段。
                if ((value == {} and field.default_factory in (dict, type(member)))
                        or (value == [] and field.default_factory in (list, list_str))):
                    del data[key]
            elif value is None and field.default is None:
                del data[key]
    elif isinstance(data, dict) and isinstance(native, Mapping):
        for key, item in native.items():
            value = data.get(key)
            if isinstance(value, (dict, list)):
                _omit_empty_defaults(item, value)
    elif isinstance(data, list) and isinstance(native, (list, tuple)):
        for item, value in zip(native, data, strict=False):
            _omit_empty_defaults(item, value)


class ManifestCapture:
    def __init__(self, max_file_bytes: int) -> None:
        self.max_file_bytes = max_file_bytes
        self._content: bytes | None = None
        self._manifest: Manifest | None = None

    def __call__(self, manifest: Manifest) -> None:
        # 每次命令使用独立实例；失败的捕获不保留旧快照。
        self._content, self._manifest = None, None
        writable = manifest.writable_manifest()
        payload = writable.to_dict(
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
        _omit_empty_defaults(writable, payload)
        # dbt 从资源 ID 和组名生成这三张索引，空列表的键可在读取时精确补回。
        for key in ("parent_map", "child_map", "group_map"):
            edges = payload.get(key)
            if edges:
                populated = {name: links for name, links in edges.items() if links}
                if len(populated) != len(edges):
                    payload[key] = populated
                    payload[SPARSE_EDGE_MAPS] = True
        # 子项全部省略后，继续省略已变空的顶层映射。
        for key in MANIFEST_MAPS:
            if payload.get(key) == {}:
                del payload[key]
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
