from __future__ import annotations

import json
from pathlib import Path

import pytest

from dbt_metricflow_service.platform_catalog import CATALOG_SCHEMA, MANIFEST_SCHEMA
from dbt_metricflow_service.platform_runs import build_platform_command, validate_artifacts


def test_run_builds_all_models_in_new_schema(tmp_path: Path) -> None:
    first = build_platform_command(tmp_path / "project", tmp_path / "profiles", "postgres", "run_a")
    assert first[1] == "build"
    assert "--select" not in first and "--exclude" not in first
    assert "--target" in first and first[first.index("--target") + 1] == "postgres"


def test_ready_requires_sha_digests_tests_relations_and_query_probe(tmp_path: Path) -> None:
    artifacts = tmp_path / "target"
    artifacts.mkdir()
    (artifacts / "manifest.json").write_text(json.dumps({
        "metadata": {"adapter_type": "postgres", "dbt_schema_version": MANIFEST_SCHEMA},
        "nodes": {"model.sample.a": {
            "resource_type": "model", "relation_name": '"db"."run_a"."a"', "schema": "run_a",
            "config": {"materialized": "table"}
        }},
    }), encoding="utf-8")
    (artifacts / "semantic_manifest.json").write_text(
        json.dumps({"semantic_models": [], "metrics": []}), encoding="utf-8"
    )
    (artifacts / "run_results.json").write_text(json.dumps({
        "metadata": {"dbt_schema_version": "v6"}, "results": [{"status": "success", "unique_id": "model.sample.a"}]
    }), encoding="utf-8")
    (artifacts / "catalog.json").write_text(json.dumps({
        "metadata": {"dbt_schema_version": CATALOG_SCHEMA}, "nodes": {"model.sample.a": {"columns": {}}},
    }), encoding="utf-8")

    result = validate_artifacts(artifacts, "run_a", query_probe_passed=True)
    assert result["queryCapability"] is True
    assert result["relationsVerified"] is True

    # StarRocks 使用已有渲染器时，仍以真实查询探针决定能否发布。
    manifest = json.loads((artifacts / "manifest.json").read_text(encoding="utf-8"))
    manifest["metadata"]["adapter_type"] = "starrocks"
    (artifacts / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    assert validate_artifacts(artifacts, "run_a", query_probe_passed=True)["queryCapability"] is True

    with pytest.raises(ValueError):
        validate_artifacts(artifacts, "run_b", query_probe_passed=True)
    with pytest.raises(ValueError):
        validate_artifacts(artifacts, "run_a", query_probe_passed=False)
    manifest = json.loads((artifacts / "manifest.json").read_text(encoding="utf-8"))
    manifest["nodes"]["model.sample.a"]["schema"] = "run_a_reporting"
    (artifacts / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError):
        validate_artifacts(artifacts, "run_a", query_probe_passed=True)


def test_profile_binding_mismatch_is_rejected() -> None:
    from dbt_metricflow_service.platform_bindings import ProjectBinding
    from dbt_metricflow_service.platform_models import PlatformRunRequest
    from dbt_metricflow_service.platform_runs import validate_request_binding

    request = PlatformRunRequest(
        projectId="sample", commitSha="a" * 40, projectDigest="b" * 64,
        profileBindingId="wrong", configVersion="1", idempotencyKey="key",
    )
    with pytest.raises(ValueError):
        validate_request_binding(request, ProjectBinding("sample", "remote", ".", "postgres"))
