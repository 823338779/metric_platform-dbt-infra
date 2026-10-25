"""历史目录直接返回封存 Manifest，不重建统一资源与依赖。"""

import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from dbt_metricflow_service.application.catalog import CatalogService
from dbt_metricflow_service.application.errors import ServiceError

MANIFEST_SCHEMA = "https://schemas.getdbt.com/dbt/manifest/v12.json"


def package():
    manifest = {"metadata": {"dbt_schema_version": MANIFEST_SCHEMA}, "nodes": {},
                "semantic_models": {}, "metrics": {}}
    for name in ("orders", "returns"):
        model_id, dataset_id = f"model.sample.{name}", f"semantic_model.sample.{name}"
        manifest["nodes"][model_id] = {
            "name": name, "resource_type": "model", "config": {"materialized": "table"},
            "database": "db", "schema": "s", "alias": name, "relation_name": f'"db"."s"."{name}"',
            "columns": {"region": {"name": "region", "description": "地区", "data_type": "varchar"}},
        }
        manifest["semantic_models"][dataset_id] = {
            "name": name, "original_file_path": "models/orders.yml",
            "dimensions": [{"name": "region", "type": "categorical"}],
            "entities": [], "measures": [], "depends_on": {"nodes": [model_id]},
        }
        manifest["metrics"][f"metric.sample.{name}"] = {
            "name": name, "label": f"展示 {name}", "type": "simple",
            "type_params": {"metric_aggregation_params": {"semantic_model": name, "agg": "sum"}},
            "depends_on": {"nodes": [dataset_id]},
        }
    manifest["metrics"]["metric.sample.net"] = {
        "name": "net", "type": "derived", "type_params": {
            "expr": "orders - returns", "metrics": [{"name": "orders"}, {"name": "returns"}]},
    }
    manifest["parent_map"] = {
        "metric.sample.net": ["metric.sample.orders", "metric.sample.returns"],
        "metric.sample.orders": ["semantic_model.sample.orders"],
    }
    manifest["child_map"] = {"metric.sample.orders": ["metric.sample.net"]}
    from dbt.artifacts.schemas.manifest import WritableManifest
    from dbt.contracts.graph.nodes import Metric, ModelNode, SemanticModel
    manifest["metrics"]["metric.sample.net"]["depends_on"] = {
        "nodes": ["metric.sample.orders", "metric.sample.returns"]}
    for section, cls, resource_type in (("nodes", ModelNode, "model"),
                                         ("semantic_models", SemanticModel, "semantic_model"),
                                         ("metrics", Metric, "metric")):
        for key, value in manifest[section].items():
            value.update(unique_id=key, resource_type=resource_type, package_name="sample", path="orders.yml",
                         original_file_path="models/orders.yml", fqn=["sample", value["name"]], created_at=1.0)
            if section == "nodes":
                value["checksum"] = {"name": "sha256", "checksum": "fixture"}
            elif section == "semantic_models":
                value.update(model="{{ ref('" + value["name"] + "') }}", node_relation=None)
            else:
                value.setdefault("label", value["name"])
                value.setdefault("description", "")
            manifest[section][key] = cls.from_dict(value).to_resource().to_dict(omit_none=False)
    for section in ("sources", "macros", "docs", "exposures", "groups", "selectors", "disabled",
                    "group_map", "saved_queries", "unit_tests"):
        manifest.setdefault(section, {})
    manifest["metadata"].update(generated_at="2026-10-25T00:00:00Z", dbt_version="1.12.0-a1")
    return WritableManifest.from_dict(manifest).to_dict(omit_none=False, context={"artifact": True})



def service_for(manifest):
    stored = SimpleNamespace(build_status="SUCCEEDED", output_set_id="historical", run_id="run")
    artifacts = SimpleNamespace(read_file=lambda *_: json.dumps(manifest).encode())
    return CatalogService(SimpleNamespace(get=lambda _: stored), artifacts)


def build(manifest=None):
    return service_for(package() if manifest is None else manifest).list(uuid4())


def test_catalog_returns_dbt_sdk_serialization():
    from dbt.artifacts.schemas.manifest import WritableManifest
    manifest = package()
    assert service_for(manifest).list(uuid4()) == WritableManifest.from_dict(manifest).to_dict(
        omit_none=False, context={"artifact": True})


def test_detail_and_native_details_return_exact_native_object():
    from dbt_metricflow_service.delegates.clients.dbt_client import DbtClient
    manifest = package()
    service = service_for(manifest)
    native = DbtClient(json.dumps(manifest).encode())._manifest
    for section in ("metrics", "nodes", "semantic_models"):
        key = next(iter(manifest[section]))
        assert service.resource(uuid4(), key) == native.expect(key).to_dict(omit_none=False)
        assert service.resource(uuid4(), key, "native-details") == native.expect(key).to_dict(omit_none=False)
    with pytest.raises(ServiceError) as error:
        service.resource(uuid4(), "semantic_model.sample.orders.dimension.region")
    assert error.value.status == 404


def test_catalog_service_reads_only_saved_manifest():
    calls = []
    def read_file(set_id, path):
        calls.append((set_id, path))
        return json.dumps(package()).encode()
    stored = SimpleNamespace(build_status="SUCCEEDED", output_set_id="historical")
    service = CatalogService(None, SimpleNamespace(read_file=read_file))
    assert json.loads(service.read_build(stored)) == package()
    assert calls == [("historical", "target/manifest.json")]


def test_source_lookup_uses_database_snapshot_relative_path_without_local_io(monkeypatch):
    from pathlib import Path

    content = json.dumps(package()).encode()
    calls = []

    def read_file(set_id, path):
        calls.append((set_id, path))
        return content if set_id == "output" else b"models: []"

    def forbidden(*args, **kwargs):
        pytest.fail("historical source lookup must not open a local file")

    stored = SimpleNamespace(build_status="SUCCEEDED", output_set_id="output", run_id="run")
    builds = SimpleNamespace(get=lambda _: stored, jobs=SimpleNamespace(
        get=lambda _: SimpleNamespace(input_set_id="source"),
    ))
    service = CatalogService(builds, SimpleNamespace(read_file=read_file))
    monkeypatch.setattr(Path, "open", forbidden)
    source = service.resource(uuid4(), "model.sample.orders", "source")
    assert source.path == "models/orders.yml"
    assert source.content == "models: []"
    assert calls == [("output", "target/manifest.json"), ("source", "models/orders.yml")]


@pytest.mark.parametrize("content", [b"broken-json", b"[]", b"{}"])
def test_catalog_service_rejects_corrupt_manifest(content):
    stored = SimpleNamespace(build_status="SUCCEEDED", output_set_id="historical")
    service = CatalogService(SimpleNamespace(get=lambda _: stored), SimpleNamespace(read_file=lambda *_: content))
    with pytest.raises(ServiceError) as error:
        service.list(uuid4())
    assert error.value.status == 503
