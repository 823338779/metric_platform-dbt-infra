"""复用物理关系必须通过受控宏进入工具链，并验证最终原生产物。"""

import json
from uuid import uuid4

import pytest

from dbt_metricflow_service.publication_build import (
    apply_relation_bindings,
    validate_bound_manifest,
    validate_readonly_sql,
)
from tests.test_publication_catalog import package


def test_reused_view_and_final_binding_agree(tmp_path):
    _, _, bindings = package()
    reused = bindings[0]
    apply_relation_bindings(tmp_path, uuid4(), "candidate", [reused])
    macro = (tmp_path / "macros" / "generate_alias_name.sql").read_text()
    assert reused["nativeId"] in macro
    assert reused["relation"]["identifier"] in macro
    target = tmp_path / "target"
    target.mkdir()
    manifest = {"nodes": {reused["nativeId"]: {
        "resource_type": "model", "config": {"materialized": "table"},
        "database": "db", "schema": "s", "alias": "orders", "relation_name": '"db"."s"."orders"'}}}
    (target / "manifest.json").write_text(json.dumps(manifest))
    validate_bound_manifest(target, "candidate", "rv_new_", [reused])
    manifest["nodes"][reused["nativeId"]]["alias"] = "rv_new_orders"
    (target / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        validate_bound_manifest(target, "candidate", "rv_new_", [reused])


@pytest.mark.parametrize("sql", [
    "select 1; drop table active_model",
    "with removed as (delete from active_model returning *) select * from removed",
    "select * into active_model from source_table",
])
def test_compiled_model_cannot_escape_readonly_query(sql):
    with pytest.raises(ValueError):
        validate_readonly_sql(sql, "postgres")
