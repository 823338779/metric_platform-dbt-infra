from __future__ import annotations

import json
from pathlib import Path

import pytest

from dbt_metricflow_service.platform_catalog import CATALOG_SCHEMA, MANIFEST_SCHEMA
from dbt_metricflow_service.platform_runs import build_platform_command, validate_artifacts


def empty_artifacts(target: Path) -> None:
    target.mkdir(parents=True)
    payloads = {
        "manifest.json": {
            "metadata": {"adapter_type": "postgres", "dbt_schema_version": MANIFEST_SCHEMA},
            "nodes": {}, "sources": {}, "semantic_models": {}, "metrics": {},
        },
        "semantic_manifest.json": {"semantic_models": [], "metrics": []},
        "run_results.json": {"results": []},
        "catalog.json": {"metadata": {"dbt_schema_version": CATALOG_SCHEMA}, "nodes": {}, "sources": {}},
    }
    for name, payload in payloads.items():
        (target / name).write_text(json.dumps(payload), encoding="utf-8")


def test_empty_release_validates_without_claiming_query_capability(tmp_path: Path) -> None:
    empty_artifacts(tmp_path / "target")
    validation = validate_artifacts(tmp_path / "target", "run_empty", query_probe_passed=False)
    assert validation["queryCapability"] is False
    assert validation["representativeQueryPassed"] is False
    assert validation["allTestsPassed"] is True
    assert validation["relationsVerified"] is True
    assert len(validation["manifestDigest"]) == 64


@pytest.mark.parametrize("missing", ["nodes", "sources", "semantic_models", "metrics"])
def test_incomplete_manifest_cannot_be_published_as_empty(tmp_path: Path, missing: str) -> None:
    empty_artifacts(tmp_path / "target")
    path = tmp_path / "target" / "manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    del manifest[missing]
    path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError):
        validate_artifacts(tmp_path / "target", "run_empty", query_probe_passed=False)


def test_empty_build_reaches_ready_without_metricflow_probe(tmp_path: Path, monkeypatch) -> None:
    import asyncio

    from dbt_metricflow_service.platform_bindings import ProjectBinding
    from dbt_metricflow_service.platform_models import PlatformRunRequest
    from dbt_metricflow_service.platform_runs import PlatformRunCoordinator
    from dbt_metricflow_service.platform_store import PlatformJobStore, RunState

    project = tmp_path / "project"
    empty_artifacts(project / "target")
    store = PlatformJobStore(tmp_path / "jobs.sqlite")
    run_id = store.reserve_run("empty", "fingerprint")
    run_dir = tmp_path / "artifacts" / str(run_id)
    run_dir.mkdir(parents=True)
    coordinator = PlatformRunCoordinator(store, {}, tmp_path / "artifacts", tmp_path / "profiles")
    monkeypatch.setattr("dbt_metricflow_service.platform_runs.resolve_revision", lambda *args: project)
    monkeypatch.setattr(coordinator, "_execute", lambda *args: None)

    def unexpected_probe(*args):
        raise AssertionError("Empty projects cannot execute a metric query")

    monkeypatch.setattr("dbt_metricflow_service.platform_runs.invoke_programmatic", unexpected_probe)
    request = PlatformRunRequest(
        projectId="sample", commitSha="a" * 40, projectDigest="b" * 64,
        profileBindingId="postgres", configVersion="1", idempotencyKey="empty",
    )
    asyncio.run(coordinator._build(
        run_id, request, ProjectBinding("sample", "remote", ".", "postgres"), run_dir
    ))
    assert store.find_run(run_id).state is RunState.READY
    assert coordinator.get(run_id)["queryCapability"] is False


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


def test_ready_requires_own_run_prefix(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_text(json.dumps({
        "metadata": {"adapter_type": "starrocks", "dbt_schema_version": MANIFEST_SCHEMA},
        "nodes": {"model.sample.a": {"resource_type": "model", "schema": "dbt_ecom",
                                      "alias": "rv_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_orders",
                                      "relation_name": "`dbt_ecom`.`rv_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_orders`",
                                      "config": {"materialized": "table"}}},
    }), encoding="utf-8")
    (target / "semantic_manifest.json").write_text(
        json.dumps({"semantic_models": [], "metrics": []}), encoding="utf-8"
    )
    (target / "run_results.json").write_text(json.dumps({
        "results": [{"status": "success", "unique_id": "model.sample.a"}]
    }), encoding="utf-8")
    (target / "catalog.json").write_text(json.dumps({
        "metadata": {"dbt_schema_version": CATALOG_SCHEMA}, "nodes": {"model.sample.a": {"columns": {}}},
    }), encoding="utf-8")

    validate_artifacts(
        target, "dbt_ecom", query_probe_passed=True,
        table_prefix="rv_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_",
    )
    with pytest.raises(ValueError):
        validate_artifacts(
            target, "dbt_ecom", query_probe_passed=True,
            table_prefix="rv_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb_",
        )
