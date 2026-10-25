"""接收 dbt 原生 Manifest，保留原对象并生成持久化字节。"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Final

from dbt_common.utils.encoding import JSONEncoder

from dbt_metricflow_service import constants as strings

if TYPE_CHECKING:
    from dbt.contracts.graph.manifest import Manifest

MANIFEST_UNAVAILABLE: Final[str] = "dbt manifest is unavailable"
MANIFEST_TOO_LARGE: Final[str] = "manifest exceeds artifact file size limit"
ARTIFACT_CONTEXT: Final[str] = "artifact"


class ManifestCapture:
    def __init__(self, max_file_bytes: int) -> None:
        self.max_file_bytes = max_file_bytes
        self._content: bytes | None = None
        self._manifest: Manifest | None = None

    def __call__(self, manifest: Manifest) -> None:
        # 每次命令使用独立实例；失败的捕获不保留旧快照。
        self._content, self._manifest = None, None
        payload = manifest.writable_manifest().to_dict(omit_none=False, context={ARTIFACT_CONTEXT: True})
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
