"""每次命令独立捕获 dbt 原生 Manifest，只序列化、不转换模型。"""

import json
from types import SimpleNamespace

import dbt.flags
import pytest
from dbt.contracts.graph.manifest import Manifest


def manifest_object(payload):
    def to_dict(*, omit_none, context):
        assert omit_none is False
        assert context == {"artifact": True, "portable": True}
        return payload

    return SimpleNamespace(writable_manifest=lambda: SimpleNamespace(to_dict=to_dict))


def test_capture_preserves_native_manifest_and_provides_compact_bytes(monkeypatch):
    from dbt_metricflow_service.platform.manifest_capture import ManifestCapture

    monkeypatch.setattr(dbt.flags, "GLOBAL_FLAGS", SimpleNamespace(SEND_ANONYMOUS_USAGE_STATS=False))
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


def test_capture_omits_execution_paths_without_mutating_native_manifest(tmp_path, monkeypatch):
    from dbt.contracts.graph.nodes import SeedNode

    from dbt_metricflow_service.delegates.clients.dbt_client import DbtClient
    from dbt_metricflow_service.platform.manifest_capture import ManifestCapture
    from tests.test_publication_catalog import package

    monkeypatch.setattr(dbt.flags, "GLOBAL_FLAGS", SimpleNamespace(SEND_ANONYMOUS_USAGE_STATS=False))
    native = DbtClient(json.dumps(package()).encode())._manifest
    model = native.nodes["model.sample.orders"]
    model.compiled_path = str(tmp_path / "target/compiled/orders.sql")
    model.build_path = str(tmp_path / "target/run/orders.sql")
    seed = SeedNode.from_dict({
        "name": "orders_seed", "resource_type": "seed", "package_name": "sample",
        "path": "orders.csv", "original_file_path": "seeds/orders.csv",
        "unique_id": "seed.sample.orders_seed", "fqn": ["sample", "orders_seed"],
        "database": "db", "schema": "s", "alias": "orders_seed",
        "checksum": {"name": "sha256", "checksum": "fixture"}, "root_path": str(tmp_path),
    })
    native.nodes[seed.unique_id] = seed
    capture = ManifestCapture(1024 * 1024)
    capture(native)
    nodes = json.loads(capture.content)["nodes"]
    assert nodes[model.unique_id].get("compiled_path") is None
    assert nodes[model.unique_id].get("build_path") is None
    assert nodes[seed.unique_id].get("root_path") is None
    assert nodes[model.unique_id]["original_file_path"] == "models/orders.yml"
    assert nodes[seed.unique_id]["original_file_path"] == "seeds/orders.csv"
    assert seed.root_path == str(tmp_path)
    assert model.compiled_path == str(tmp_path / "target/compiled/orders.sql")
    assert model.build_path == str(tmp_path / "target/run/orders.sql")
    # 普通 dbt 输出保持原有行为，只有历史快照序列化不带运行目录。
    assert native.writable_manifest().to_dict(omit_none=False, context={"artifact": True})[
        "nodes"][seed.unique_id]["root_path"] == str(tmp_path)


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


def test_manifest_read_only_transfers_bytes():
    from dbt_metricflow_service.platform.manifest_capture import ManifestCapture
    content = b"opaque to the service"
    assert ManifestCapture.read(SimpleNamespace(read_file=lambda *_: content), "snapshot") is content


def test_persist_owns_manifest_path_and_passes_snapshot_to_generic_store(tmp_path):
    from dbt_metricflow_service.platform.manifest_capture import ManifestCapture
    from dbt_metricflow_service.storage.records import ArtifactMetadata

    (tmp_path / "dbt_project.yml").write_text("name: sample\ntarget-path: custom-target\n")
    capture = ManifestCapture(1024)
    capture(manifest_object({"nodes": {}, "captured": True}))
    calls = []
    def save(*args, **kwargs):
        calls.append((args, kwargs))
        return "output"
    metadata = ArtifactMetadata()
    assert capture.persist(SimpleNamespace(capture=save), "sample", tmp_path,
                           producer_attempt_id="attempt", metadata=metadata) == "output"
    assert calls == [(("sample", tmp_path), {
        "kind": "EXECUTION", "producer_attempt_id": "attempt", "metadata": metadata,
        "memory_files": {"custom-target/manifest.json": capture.content},
    })]


def test_unavailable_manifest_cannot_start_persistence(tmp_path):
    from dbt_metricflow_service.platform.manifest_capture import ManifestCapture

    def forbidden(*args, **kwargs):
        pytest.fail("no snapshot may be stored without a captured manifest")
    with pytest.raises(ValueError, match="manifest is unavailable"):
        ManifestCapture(1024).persist(SimpleNamespace(capture=forbidden), "sample", tmp_path)
