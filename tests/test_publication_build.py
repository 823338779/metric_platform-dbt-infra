"""复用物理关系必须通过受控宏进入工具链，并验证最终原生产物。"""

import json
import os
import subprocess
import sys
from types import SimpleNamespace
from uuid import uuid4

import pytest
import yaml
from resource_helpers import make_resource_project

from dbt_metricflow_service.models.native import NativeNode
from dbt_metricflow_service.platform.build import (
    execute_publication,
    prepare_publication_project,
    validate_bound_manifest,
    validate_readonly_sql,
)
from dbt_metricflow_service.platform.models import ProbeResult
from dbt_metricflow_service.storage.records import ArtifactValidation, JobRequest
from tests.data_helpers import leased_job


@pytest.fixture
def publication_executor(tmp_path):
    """工具链边界提供原生产物，发布验证和目录封存仍执行真实代码。"""
    from dbt_metricflow_service.platform.catalog import CATALOG_SCHEMA, MANIFEST_SCHEMA
    from dbt_metricflow_service.runtime.executor import RuntimeExecutor

    project = tmp_path / "project"
    project.mkdir()
    (project / "dbt_project.yml").write_text("name: sample\nversion: '1.0'\n", encoding="utf-8")
    target = project / "target"
    target.mkdir()
    job = leased_job(project_id="sample", request_json=JobRequest(build_id=str(uuid4())),
                     schema_name="candidate", profile_binding_id="test", toolchain_version="1")
    alias = "orders"
    manifest = {"metadata": {"dbt_schema_version": MANIFEST_SCHEMA, "adapter_type": "postgres"},
                "nodes": {"model.sample.orders": {"resource_type": "model", "name": "orders",
                          "config": {"materialized": "table"}, "database": "db", "schema": "candidate",
                          "alias": alias, "relation_name": f'"candidate"."{alias}"', "compiled_code": "select 1"},
                          "test.sample.orders": {"resource_type": "test", "config": {},
                                                 "compiled_code": "select 1 where false"}}}
    results = [{"unique_id": key, "status": "success"} for key in manifest["nodes"]]
    physical = {"metadata": {"dbt_schema_version": CATALOG_SCHEMA},
                "nodes": {"model.sample.orders": {"columns": {}}}}
    captured = {}

    async def dbt(_job, spec, _phase):
        if spec.argv[1] in ("parse", "compile", "build"):
            (target / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            (target / "semantic_manifest.json").write_text('{"semantic_models": [], "metrics": []}')
        if spec.argv[1] == "build":
            (target / "run_results.json").write_text(json.dumps({"results": results}))
        if spec.argv[1] == "docs":
            (target / "catalog.json").write_text(json.dumps(physical))
            (target / "run_results.json").write_text('{"results": []}')

    async def metricflow(*_args):
        return ProbeResult(query_capability=True)

    def capture(_project_id, _project, **kwargs):
        captured.update(kwargs)
        return "sealed-output"

    executor = SimpleNamespace(settings=SimpleNamespace(profiles_dir=tmp_path, max_artifact_file_bytes=1024 * 1024),
                               _dbt=dbt, probe_query_capability=metricflow, _metadata=RuntimeExecutor._metadata,
                               artifacts=SimpleNamespace(capture=capture))
    return executor, job, project, results, physical, captured


async def test_publication_persists_catalog_file_and_minimum_validation_flags(publication_executor):
    executor, job, project, _results, _physical, captured = publication_executor
    await execute_publication(executor, job, project.parent, project)

    catalog = json.loads((project / "target/published_catalog.json").read_text(encoding="utf-8"))
    assert catalog["releaseId"] == job.request_json.build_id
    assert catalog["relationBindings"][0]["mode"] == "BUILT"
    assert not (project / "target/publication_evidence.json").exists()
    assert captured["metadata"].validation_json == ArtifactValidation(
        all_tests_passed=True, representative_query_passed=True, relations_verified=True,
    )
    assert not hasattr(captured["metadata"], "catalog_json")


@pytest.mark.parametrize("missing", ["model.sample.orders", "test.sample.orders"])
async def test_full_build_rejects_missing_model_or_test_execution(publication_executor, missing):
    executor, job, project, results, _physical, captured = publication_executor
    results[:] = [row for row in results if row["unique_id"] != missing]
    with pytest.raises(ValueError, match="模型或必需测试执行不完整"):
        await execute_publication(executor, job, project.parent, project)
    assert not captured


async def test_full_build_rejects_missing_physical_relation(publication_executor):
    executor, job, project, _results, physical, captured = publication_executor
    physical["nodes"] = {}
    with pytest.raises(ValueError, match="物理对象缺失"):
        await execute_publication(executor, job, project.parent, project)
    assert not captured


@pytest.mark.parametrize(("freshness", "expected"), [
    (None, False),
    ({"warn_after": {"count": None, "period": None},
      "error_after": {"count": None, "period": None}}, False),
    ({"warn_after": {"count": 0, "period": "hour"}}, True),
    ({"error_after": {"count": 2, "period": "day"}}, True),
])
def test_only_configured_freshness_thresholds_require_execution(freshness, expected):
    """原生 manifest 的空阈值对象不是 freshness 规则，零阈值则是有效规则。"""
    from dbt_metricflow_service.platform.build import requires_source_freshness

    assert requires_source_freshness(NativeNode.model_validate({"freshness": freshness})) is expected


def test_manifest_cannot_escape_fixed_schema(tmp_path):
    prepare_publication_project(tmp_path, "candidate")
    target = tmp_path / "target"
    target.mkdir()
    node = {"resource_type": "model", "config": {"materialized": "table"}, "database": "db",
            "schema": "candidate", "alias": "orders", "relation_name": '"candidate"."orders"'}
    path = target / "manifest.json"
    path.write_text(json.dumps({"nodes": {"model.p.orders": node}}))
    validate_bound_manifest(target, "candidate")
    node.update(schema="other", relation_name='"other"."orders"')
    path.write_text(json.dumps({"nodes": {"model.p.orders": node}}))
    with pytest.raises(ValueError):
        validate_bound_manifest(target, "candidate")


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
    prepare_publication_project(project, "main")
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
    models = [node for node in manifest["nodes"].values() if node["resource_type"] == "model"]
    assert models and all(node["alias"] == node["name"] for node in models)
    if writes:
        with pytest.raises(ValueError, match="发布测试禁止写入失败结果表"):
            validate_bound_manifest(target, "main")
    else:
        validate_bound_manifest(target, "main")
def test_custom_target_directory_is_normalized_for_sealing(tmp_path):
    """命令和封存使用同一输出目录，原始源码仍由输入快照保留。"""
    import yaml

    from dbt_metricflow_service.platform.build import prepare_publication_project

    project_file = tmp_path / "dbt_project.yml"
    project_file.write_text("name: sample\nversion: '1.0'\ntarget-path: custom-target\n", encoding="utf-8")
    prepare_publication_project(tmp_path, "analytics")
    config = yaml.safe_load(project_file.read_text(encoding="utf-8"))
    assert config["target-path"] == "target"


def test_repeated_dbt_build_overwrites_original_and_custom_alias_tables(tmp_path):
    import duckdb

    project, profiles, _ = make_resource_project(tmp_path)
    model = project / "models/orders.sql"
    source = model.read_text(encoding="utf-8")
    model.write_text("{{ config(alias='sales_orders') }}\n" + source, encoding="utf-8")
    prepare_publication_project(project, "main")
    for value in (10, 20):
        model.write_text(
            "{{ config(alias='sales_orders') }}\n" + source.replace("10 as revenue", f"{value} as revenue"),
            encoding="utf-8",
        )
        result = subprocess.run(
            [sys.executable, "-m", "dbt.cli.main", "build", "--no-partial-parse",
             "--project-dir", str(project), "--profiles-dir", str(profiles)],
            capture_output=True, text=True, encoding="utf-8", timeout=60,
            env={**os.environ, "PYTHONUTF8": "1", "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"},
        )
        assert result.returncode == 0, result.stdout + result.stderr
        validate_bound_manifest(project / "target", "main")
        with duckdb.connect(str(tmp_path / "warehouse.duckdb")) as connection:
            assert connection.execute("select revenue from main.sales_orders").fetchone() == (value,)
            names = {row[0] for row in connection.execute("show tables").fetchall()}
            assert "sales_orders" in names
            assert all(not name.startswith("rv_") for name in names)
