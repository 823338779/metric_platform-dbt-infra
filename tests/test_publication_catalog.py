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
    return manifest



def service_for(manifest):
    stored = SimpleNamespace(build_status="SUCCEEDED", output_set_id="historical", run_id="run")
    artifacts = SimpleNamespace(read_file=lambda *_: json.dumps(manifest).encode())
    return CatalogService(SimpleNamespace(get=lambda _: stored), artifacts)


def build(manifest=None):
    return service_for(package() if manifest is None else manifest).read(uuid4())[1]


def test_catalog_returns_native_sections_without_resource_translation():
    manifest = package()
    manifest["metrics"]["metric.sample.net"]["vendor_extension"] = {"raw_key": None}
    manifest["macros"] = {"macro.sample.secret": {"macro_sql": "not catalog data"}}
    result = service_for(manifest).list(uuid4())
    assert result == {key: manifest.get(key, {}) for key in (
        "metadata", "metrics", "semantic_models", "nodes", "sources", "parent_map", "child_map")}
    assert result["metrics"]["metric.sample.net"] == manifest["metrics"]["metric.sample.net"]
    assert "dimensions" not in result
    assert result["semantic_models"]["semantic_model.sample.orders"]["dimensions"] == [{
        "name": "region", "type": "categorical"}]


def test_detail_and_native_details_return_exact_native_object():
    manifest = package()
    service = service_for(manifest)
    for section in ("metrics", "nodes", "semantic_models"):
        key = next(iter(manifest[section]))
        assert service.resource(uuid4(), key) == manifest[section][key]
        assert service.resource(uuid4(), key, "native-details") == manifest[section][key]
    with pytest.raises(ServiceError) as error:
        service.resource(uuid4(), "semantic_model.sample.orders.dimension.region")
    assert error.value.status == 404


def test_lineage_returns_native_maps_without_inference():
    manifest = package()
    manifest["parent_map"]["metric.sample.orders"] = ["semantic_model.sample.orders", "model.sample.ephemeral"]
    service = service_for(manifest)
    assert service.resource(uuid4(), "metric.sample.orders", "lineage") == {
        "parent_map": {"metric.sample.orders": manifest["parent_map"]["metric.sample.orders"]},
        "child_map": {"metric.sample.orders": ["metric.sample.net"]},
    }
    del manifest["parent_map"]["metric.sample.net"]
    assert service.resource(uuid4(), "metric.sample.net", "lineage")["parent_map"] == {}


def test_catalog_service_reads_only_saved_manifest():
    calls = []
    def read_file(set_id, path):
        calls.append((set_id, path))
        return json.dumps(package()).encode()
    stored = SimpleNamespace(build_status="SUCCEEDED", output_set_id="historical")
    service = CatalogService(None, SimpleNamespace(read_file=read_file))
    assert service.read_build(stored) == package()
    assert calls == [("historical", "target/manifest.json")]


@pytest.mark.parametrize("content", [b"broken-json", b"[]", b"{}"])
def test_catalog_service_rejects_corrupt_manifest(content):
    stored = SimpleNamespace(build_status="SUCCEEDED", output_set_id="historical")
    service = CatalogService(None, SimpleNamespace(read_file=lambda *_: content))
    with pytest.raises(ServiceError) as error:
        service.read_build(stored)
    assert error.value.status == 503
