"""复用物理关系必须通过受控宏进入工具链，并验证最终原生产物。"""

import json
import os
import subprocess
import sys
from uuid import uuid4

import pytest
import yaml
from resource_helpers import make_resource_project

from dbt_metricflow_service.publications.build import (
    prepare_publication_project,
    validate_bound_manifest,
    validate_readonly_sql,
)


@pytest.mark.parametrize(("freshness", "expected"), [
    (None, False),
    ({"warn_after": {"count": None, "period": None},
      "error_after": {"count": None, "period": None}}, False),
    ({"warn_after": {"count": 0, "period": "hour"}}, True),
    ({"error_after": {"count": 2, "period": "day"}}, True),
])
def test_only_configured_freshness_thresholds_require_execution(freshness, expected):
    """原生 manifest 的空阈值对象不是 freshness 规则，零阈值则是有效规则。"""
    from dbt_metricflow_service.publications.build import requires_source_freshness

    assert requires_source_freshness({"freshness": freshness}) is expected


def test_manifest_cannot_bind_a_previous_run(tmp_path):
    prefix = prepare_publication_project(tmp_path, uuid4(), "candidate")
    target = tmp_path / "target"
    target.mkdir()
    node = {"resource_type": "model", "config": {"materialized": "table"}, "database": "db",
            "schema": "candidate", "alias": prefix + "orders", "relation_name": '"candidate"."' + prefix + 'orders"'}
    path = target / "manifest.json"
    path.write_text(json.dumps({"nodes": {"model.p.orders": node}}))
    validate_bound_manifest(target, "candidate", prefix)
    node.update(alias="old_orders", relation_name='"candidate"."old_orders"')
    path.write_text(json.dumps({"nodes": {"model.p.orders": node}}))
    with pytest.raises(ValueError):
        validate_bound_manifest(target, "candidate", prefix)


@pytest.mark.parametrize("sql", [
    "select 1; drop table active_model",
    "with removed as (delete from active_model returning *) select * from removed",
    "select * into active_model from source_table",
])
def test_compiled_model_cannot_escape_readonly_query(sql):
    with pytest.raises(ValueError):
        validate_readonly_sql(sql, "postgres")


@pytest.mark.parametrize(("test_config", "writes"), [
    ({}, False), ({"store_failures": False}, False), ({"store_failures": True}, True),
    ({"store_failures_as": "ephemeral"}, False), ({"store_failures_as": "view"}, True),
])
def test_actual_dbt_tests_parse_under_publication_mapping(tmp_path, test_config, writes):
    """dbt 普通测试自带 audit schema；允许解析，但写失败结果的测试仍须被发布边界拒绝。"""
    project, profiles, _ = make_resource_project(tmp_path)
    definition = project / "models/orders.yml"
    document = yaml.safe_load(definition.read_text(encoding="utf-8"))
    document["models"][1]["columns"][0]["data_tests"] = [
        {"not_null": {"config": test_config}}]
    definition.write_text(yaml.safe_dump(document, allow_unicode=True), encoding="utf-8")
    prefix = prepare_publication_project(project, uuid4(), "main")
    parsed = subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", "parse", "--no-partial-parse",
         "--project-dir", str(project), "--profiles-dir", str(profiles)],
        capture_output=True, text=True, encoding="utf-8", timeout=60,
        env={**os.environ, "PYTHONUTF8": "1", "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"})
    assert parsed.returncode == 0, parsed.stdout + parsed.stderr
    target = project / "target"
    manifest = json.loads((target / "manifest.json").read_text(encoding="utf-8"))
    tests = [node for node in manifest["nodes"].values() if node["resource_type"] == "test"]
    assert tests and all(node["schema"] == "main" for node in tests)
    if writes:
        with pytest.raises(ValueError, match="发布测试禁止写入失败结果表"):
            validate_bound_manifest(target, "main", prefix)
    else:
        validate_bound_manifest(target, "main", prefix)
