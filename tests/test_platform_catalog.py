from __future__ import annotations

import json
from pathlib import Path

import pytest

from dbt_metricflow_service.platform.catalog import catalog_from_artifacts


def artifacts(tmp_path: Path) -> Path:
    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_text(json.dumps({
        "metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json"},
        "metrics": {"metric.sample.revenue": {
            "unique_id": "metric.sample.revenue", "name": "revenue", "description": "Revenue",
            "original_file_path": "models/orders.yml", "depends_on": {"nodes": ["semantic_model.sample.orders"]},
        }},
        "semantic_models": {"semantic_model.sample.orders": {
            "unique_id": "semantic_model.sample.orders", "name": "orders", "original_file_path": "models/orders.yml",
            "dimensions": [{"name": "ordered_at", "type": "time", "type_params": {"time_granularity": "day"}}],
            "depends_on": {"nodes": ["model.sample.orders"]},
        }},
        "nodes": {
            "model.sample.orders": {
                "unique_id": "model.sample.orders", "name": "orders", "resource_type": "model",
                "original_file_path": "models/orders.sql", "relation_name": '"db"."run_a"."orders"',
                "config": {"materialized": "table"}, "depends_on": {"nodes": []},
            },
            "model.sample.view": {
                "unique_id": "model.sample.view", "name": "view", "resource_type": "model",
                "original_file_path": "models/view.sql", "relation_name": '"db"."run_a"."view"',
                "config": {"materialized": "view"}, "depends_on": {"nodes": ["model.sample.orders"]},
            },
        },
    }), encoding="utf-8")
    (target / "semantic_manifest.json").write_text(json.dumps({"semantic_models": [], "metrics": []}), encoding="utf-8")
    (target / "catalog.json").write_text(json.dumps({
        "metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/catalog/v1.json"},
        "nodes": {"model.sample.orders": {"columns": {"order_id": {"name": "order_id", "type": "integer"}}},
                  "model.sample.view": {"columns": {}}},
    }), encoding="utf-8")
    return target


def test_catalog_maps_metric_dimension_semantic_model_table_and_view(tmp_path: Path) -> None:
    result = catalog_from_artifacts(artifacts(tmp_path))
    kinds = {resource["kind"] for resource in result["resources"]}
    assert kinds == {"METRIC", "DIMENSION", "SEMANTIC_MODEL", "TABLE", "VIEW"}
    table = next(item for item in result["resources"] if item["kind"] == "TABLE")
    assert table["columns"]["order_id"]["type"] == "integer"
    assert result["dependencies"]
    assert result["artifactSchemas"]["manifest"].endswith("v12.json")


def test_catalog_rejects_schema_version_or_missing_relation(tmp_path: Path) -> None:
    target = artifacts(tmp_path)
    manifest_path = target / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["metadata"]["dbt_schema_version"] = "https://schemas.getdbt.com/dbt/manifest/v13.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError):
        catalog_from_artifacts(target)
    manifest["metadata"]["dbt_schema_version"] = "https://schemas.getdbt.com/dbt/manifest/v12.json"
    manifest["nodes"]["model.sample.orders"]["relation_name"] = None
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError):
        catalog_from_artifacts(target)


def test_catalog_source_path_uses_git_separators_on_windows(tmp_path: Path) -> None:
    target = artifacts(tmp_path)
    manifest_path = target / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["metrics"]["metric.sample.revenue"]["original_file_path"] = "models\\orders.yml"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    metric = next(item for item in catalog_from_artifacts(target)["resources"] if item["kind"] == "METRIC")

    assert metric["sourcePath"] == "models/orders.yml"


def test_catalog_includes_external_source_table(tmp_path: Path) -> None:
    target = artifacts(tmp_path)
    manifest_path = target / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    source_id = "source.sample.ecommerce_raw.orders"
    manifest["sources"] = {source_id: {
        "unique_id": source_id, "name": "orders", "resource_type": "source",
        "description": "原始订单", "original_file_path": "models/sources.yml",
        "relation_name": "`ecommerce_raw`.`orders`", "depends_on": {"nodes": []},
    }}
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    catalog_path = target / "catalog.json"
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    catalog["sources"] = {source_id: {"columns": {"order_id": {"name": "order_id", "type": "bigint"}}}}
    catalog_path.write_text(json.dumps(catalog), encoding="utf-8")

    table = next(item for item in catalog_from_artifacts(target)["resources"] if item["nativeId"] == source_id)

    assert table["kind"] == "TABLE"
    assert table["relation"] == "`ecommerce_raw`.`orders`"
    assert table["columns"]["order_id"]["type"] == "bigint"
    assert table["queryable"] is True
