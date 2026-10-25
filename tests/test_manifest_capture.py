import json

from dbt.cli.main import dbtRunner
from test_sdk_integration import write_project

from dbt_metricflow_service.engines.dbt import DbtClient
from dbt_metricflow_service.platform.manifest_capture import ManifestCapture


def test_capture_strips_runtime_paths_without_mutating_native_nodes(tmp_path):
    write_project(tmp_path)
    (tmp_path / "seeds").mkdir()
    (tmp_path / "seeds/example.csv").write_text("id\n1\n", encoding="utf-8")
    (tmp_path / "models/disabled.sql").write_text(
        "{{ config(enabled=false) }} select 1 as id", encoding="utf-8",
    )
    (tmp_path / "profiles.yml").write_text(
        "execution:\n  target: test\n  outputs:\n    test:\n      type: duckdb\n      path: ':memory:'\n",
        encoding="utf-8",
    )
    result = dbtRunner().invoke([
        "parse", "--quiet", "--project-dir", str(tmp_path), "--profiles-dir", str(tmp_path),
    ])
    assert result.success, result.exception
    manifest = result.result
    model = manifest.nodes["model.api_review.orders"]
    disabled = manifest.disabled["model.api_review.disabled"][0]
    seed = manifest.nodes["seed.api_review.example"]
    for node in (model, disabled):
        node.compiled = True
        node.build_path = "runtime/build.sql"
        node.compiled_path = "runtime/compiled.sql"
    model.meta = {"root_path": "business metadata", "compiled_path": "keep this"}
    original_seed_root = seed.root_path
    assert original_seed_root
    raw = manifest.writable_manifest().to_dict(omit_none=False, context={"artifact": True})
    assert raw["nodes"][model.unique_id]["compiled_path"]
    assert raw["nodes"][seed.unique_id]["root_path"]

    capture = ManifestCapture(64 * 1024 * 1024)
    capture(manifest)
    saved = json.loads(capture.content)
    for node in (saved["nodes"][model.unique_id], saved["nodes"][seed.unique_id],
                 saved["disabled"][disabled.unique_id][0]):
        assert not {"root_path", "build_path", "compiled_path"}.intersection(node)
        assert node["original_file_path"]
    assert saved["nodes"][model.unique_id]["meta"] == model.meta
    assert model.compiled_path == disabled.compiled_path == "runtime/compiled.sql"
    assert model.build_path == disabled.build_path == "runtime/build.sql"
    assert seed.root_path == original_seed_root
    assert capture.manifest is manifest
    assert DbtClient(capture.content).resource(model.unique_id)["name"] == "orders"
