"""原生字段不再经过服务的类型、路径、依赖转换。"""

import pytest

from tests.test_publication_catalog import build, package


@pytest.mark.parametrize("resource_type,materialized", [("model", "incremental"), ("seed", "seed"),
                                                       ("snapshot", "snapshot"), ("model", "ephemeral")])
def test_native_nodes_are_preserved(resource_type, materialized):
    manifest = package()
    node = manifest["nodes"]["model.sample.orders"]
    node.update(resource_type=resource_type, config={"materialized": materialized},
                original_file_path="models\\orders.sql", relation_name=None)
    manifest["metadata"]["dbt_schema_version"] = "https://schemas.getdbt.com/dbt/manifest/v13.json"
    assert build(manifest) == manifest


def test_external_source_columns_and_dependencies_are_not_rewritten():
    manifest = package()
    manifest["sources"] = {"source.sample.raw.orders": {
        "name": "orders", "resource_type": "source", "schema": "raw", "identifier": "orders",
        "columns": {"order_id": {"data_type": "bigint", "description": None}},
    }}
    manifest["nodes"]["model.sample.orders"]["depends_on"] = {"nodes": ["source.sample.raw.orders"]}
    assert build(manifest) == manifest
