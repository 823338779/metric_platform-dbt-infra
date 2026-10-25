"""每次命令独立捕获 dbt 原生 Manifest，只序列化、不转换模型。"""

import json
from types import SimpleNamespace

import pytest
from dbt.contracts.graph.manifest import Manifest
from dbt.flags import get_flags


def manifest_object(payload):
    def to_dict(*, omit_none, context):
        assert omit_none is False
        assert context == {"artifact": True}
        return payload

    return SimpleNamespace(writable_manifest=lambda: SimpleNamespace(to_dict=to_dict))


def test_capture_preserves_native_manifest_and_provides_compact_bytes(monkeypatch):
    from dbt_metricflow_service.platform.manifest_capture import ManifestCapture

    monkeypatch.setattr(get_flags(), "SEND_ANONYMOUS_USAGE_STATS", False, raising=False)
    manifest = Manifest()
    manifest.metadata.project_name = "指标"
    payload = manifest.writable_manifest().to_dict(omit_none=False, context={"artifact": True})
    capture = ManifestCapture(1024 * 1024)
    capture(manifest)
    assert capture.manifest is manifest
    assert json.loads(capture.content)["nodes"] == payload["nodes"]
    assert '"project_name":"指标"'.encode() in capture.content
    manifest.metadata.project_name = "changed"
    assert json.loads(capture.content)["metadata"]["project_name"] == "指标"


def test_missing_command_snapshot_cannot_reuse_another_capture():
    from dbt_metricflow_service.platform.manifest_capture import ManifestCapture

    previous = ManifestCapture(1024)
    previous(manifest_object({"nodes": {}}))
    current = ManifestCapture(1024)
    with pytest.raises(ValueError, match="manifest is unavailable"):
        _ = current.manifest
    with pytest.raises(ValueError, match="manifest is unavailable"):
        _ = current.content


def test_capture_checks_utf8_size_before_accepting_snapshot():
    from dbt_metricflow_service.platform.manifest_capture import ManifestCapture

    capture = ManifestCapture(30)
    with pytest.raises(ValueError, match="size limit"):
        capture(manifest_object({"description": "指标" * 10}))
    with pytest.raises(ValueError, match="manifest is unavailable"):
        _ = capture.content
