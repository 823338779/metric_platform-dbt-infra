"""最终 Manifest 通过 SDK 内存返回并入库，持久化不读取磁盘 JSON。"""

import gzip
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from dbt.cli.main import dbtRunner
from dbt.contracts.graph.manifest import Manifest
from dbt_common.utils.encoding import JSONEncoder

from dbt_metricflow_service.execution.dbt import run_dbt
from dbt_metricflow_service.execution.models import CommandSpec, JobStatus
from dbt_metricflow_service.runtime.executor import build_engine_environment
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.entities import ArtifactFile, ArtifactSet
from tests.resource_helpers import make_resource_project
from tests.schema_helpers import isolated_database


def test_runner_returns_final_manifest_for_parse_build_and_docs(tmp_path, monkeypatch):
    monkeypatch.setenv("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
    project, profiles, _ = make_resource_project(tmp_path)
    runner = dbtRunner()
    for command in (["parse"], ["build"], ["docs", "generate"]):
        result = runner.invoke([
            *command, "--project-dir", str(project), "--profiles-dir", str(profiles),
        ])
        assert result.success, result.exception
        assert isinstance(result.manifest, Manifest)
        if command == ["parse"]:
            assert result.manifest is result.result
        else:
            assert result.manifest is not result.result
            nodes = result.manifest.writable_manifest().to_dict(omit_none=False, context={"artifact": True})["nodes"]
            assert any(node.get("compiled_code") for node in nodes.values())
            disk = json.loads((project / "target/manifest.json").read_text(encoding="utf-8"))
            assert nodes == disk["nodes"]
    assert runner.invoke(["--help"]).manifest is None


def test_runner_without_manifest_does_not_reuse_previous_result():
    runner = dbtRunner()
    assert runner.invoke(["--help"]).manifest is None
    assert runner.invoke(["not-a-command"]).manifest is None


async def test_dbt_passes_manifest_object_without_serializing_or_logging_it(tmp_path, monkeypatch):
    manifest = Manifest()

    def unexpected_serialization(_self):
        raise AssertionError("the caller owns manifest serialization")

    monkeypatch.setattr(Manifest, "writable_manifest", unexpected_serialization)
    monkeypatch.setattr(dbtRunner, "invoke", lambda *_: SimpleNamespace(
        success=True, exception=None, manifest=manifest,
    ))
    received = []

    async def authorize():
        pass

    record = await run_dbt("memory", CommandSpec(("dbt", "docs", "generate"), tmp_path, {}, False),
                           5, 1024, authorize, manifest_sink=received.append)
    assert record.status == JobStatus.SUCCEEDED
    assert len(received) == 1
    assert received[0] is manifest
    assert "manifest" not in record.model_dump_json()


@pytest.mark.parametrize("success", [False, True])
async def test_failed_or_missing_manifest_cannot_provide_payload(tmp_path, monkeypatch, success):
    monkeypatch.setattr(dbtRunner, "invoke", lambda *_: SimpleNamespace(
        success=success, exception=None, manifest=None,
    ))
    received = []

    async def authorize():
        pass

    record = await run_dbt("memory", CommandSpec(("dbt", "docs", "generate"), tmp_path, {}, False),
                           5, 1024, authorize, manifest_sink=received.append)
    assert record.status == JobStatus.FAILED
    assert received == []


async def test_real_docs_manifest_roundtrips_through_database_without_disk_capture(tmp_path, monkeypatch):
    project, profiles, _ = make_resource_project(tmp_path)
    environment = build_engine_environment(project, profiles, "main", "test")
    received = []

    def capture_manifest(manifest):
        assert isinstance(manifest, Manifest)
        payload = manifest.writable_manifest().to_dict(omit_none=False, context={"artifact": True})
        received.append(json.dumps(payload, cls=JSONEncoder, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))

    async def authorize():
        pass

    for args in (("build",), ("docs", "generate")):
        spec = CommandSpec(("dbt", *args, "--project-dir", str(project), "--profiles-dir", str(profiles)),
                           project, environment, args == ("build",))
        result = await run_dbt("memory", spec, 60, 1024, authorize,
                               manifest_sink=capture_manifest if args[0] == "docs" else None)
        assert result.status == JobStatus.SUCCEEDED, result.stderr
    assert len(received) == 1
    disk = project / "target/manifest.json"
    assert json.loads(received[0]) == json.loads(disk.read_bytes())
    original_open = Path.open

    def guarded_open(path, *args, **kwargs):
        assert path != disk, "capture must use the SDK payload"
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)
    with isolated_database() as db:
        db.initialize()
        with db.transaction() as connection:
            connection.exec_driver_sql("INSERT INTO runtime_project(project_id) VALUES ('memory')")
        store = ArtifactStore(db)
        identifier = store.capture("memory", project, kind="EXECUTION", manifest_json=received[0])
        assert store.read_file(identifier, "target/manifest.json") == received[0]
        with db.session() as session:
            file = session.get(ArtifactFile, (identifier, "target/manifest.json"))
            assert file.codec == "gzip"
            assert file.stored_size < file.raw_size
            assert gzip.decompress(file.content) == received[0]


@pytest.mark.parametrize("payload,codec", [
    (json.dumps({"description": "指标说明" * 256}, ensure_ascii=False, separators=(",", ":")).encode(), "gzip"),
    (b"{}", "raw"),
], ids=["compressible", "tiny-raw"])
def test_manifest_compression_preserves_integrity_and_raw_limits(tmp_path, payload, codec):
    project_text = b"name: memory\n"
    (tmp_path / "dbt_project.yml").write_bytes(project_text)
    with isolated_database() as db:
        db.initialize()
        with db.transaction() as connection:
            connection.exec_driver_sql("INSERT INTO runtime_project(project_id) VALUES ('memory')")
        store = ArtifactStore(db, max_file_bytes=max(len(payload), len(project_text)))
        identifier = store.capture("memory", tmp_path, kind="EXECUTION", manifest_json=payload)
        with db.session() as session:
            row = session.get(ArtifactFile, (identifier, "target/manifest.json"))
            assert row.codec == codec
            assert row.raw_size == len(payload)
            assert row.stored_size == len(row.content)
            assert row.stored_size <= row.raw_size
            assert row.raw_sha256 == hashlib.sha256(payload).hexdigest()
            if codec == "gzip":
                assert gzip.decompress(row.content) == payload
            source = session.get(ArtifactFile, (identifier, "dbt_project.yml"))
            assert source.codec == "raw"
            assert source.content == project_text
            metadata = session.get(ArtifactSet, identifier)
            assert metadata.raw_bytes == len(payload) + len(project_text)
        assert store.read_file(identifier, "target/manifest.json") == payload
        store.materialize(identifier, tmp_path / "restored")
        assert (tmp_path / "restored/target/manifest.json").read_bytes() == payload


@pytest.mark.parametrize("on_disk", [False, True])
@pytest.mark.parametrize("target", ["target", "custom-target"])
def test_capture_uses_memory_manifest_even_without_disk_file(tmp_path, monkeypatch, on_disk, target):
    (tmp_path / "dbt_project.yml").write_text(
        f"name: memory\nversion: '1.0'\ntarget-path: {target}\n", encoding="utf-8",
    )
    relative = f"{target}/manifest.json"
    disk = tmp_path / relative
    if on_disk:
        disk.parent.mkdir()
        disk.write_bytes(b"stale disk content")
    original_open = Path.open

    def guarded_open(path, *args, **kwargs):
        assert path != disk, "capture must not read the disk manifest"
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)
    payload = b'{"nodes": {}, "metadata": {"source": "memory"}}'
    with isolated_database() as db:
        db.initialize()
        with db.transaction() as connection:
            connection.exec_driver_sql("INSERT INTO runtime_project(project_id) VALUES ('memory')")
        store = ArtifactStore(db)
        identifier = store.capture("memory", tmp_path, kind="EXECUTION", manifest_json=payload)
        assert store.read_file(identifier, relative) == payload
        destination = tmp_path / "restored"
        store.materialize(identifier, destination)
        assert (destination / relative).read_bytes() == payload
        assert list(destination.rglob("manifest.json")) == [destination / relative]


@pytest.mark.parametrize("file_limit,set_limit", [(64, 1024), (1024, 64)])
def test_memory_manifest_obeys_capture_size_limits(tmp_path, file_limit, set_limit):
    (tmp_path / "dbt_project.yml").write_text("name: memory\n", encoding="utf-8")
    store = ArtifactStore(None, max_file_bytes=file_limit, max_set_bytes=set_limit)
    with pytest.raises(ValueError, match="size limit"):
        store.capture("memory", tmp_path, kind="EXECUTION", manifest_json=b"x" * 65)
