"""执行 profile、物理命名空间和外部 secret 解析的边界。"""

import json
from pathlib import Path

import pytest
import yaml

from dbt_metricflow_service.models.configuration import ExecutionCredential, ExecutionInstance
from dbt_metricflow_service.platform.execution_profile import (
    materialized_profile,
    validate_execution_manifest,
    validate_execution_target,
)
from dbt_metricflow_service.settings import Settings
from tests.test_execution_snapshot_storage import snapshot


def settings():
    return Settings(
        profiles_dir=Path("profiles"), command_timeout_seconds=30, max_output_bytes=1024,
        execution_instances={"warehouse": ExecutionInstance(
            type="starrocks", hosts=["localhost", "127.0.0.1"], port=9030,
        )},
        execution_credentials={"warehouse-test": ExecutionCredential(
            user_env="DBT_ENV_SECRET_ENV_TEST_USER", password_env="DBT_ENV_SECRET_ENV_TEST_PASSWORD",
        )},
    )


@pytest.fixture
def secrets(monkeypatch):
    monkeypatch.setenv("DBT_ENV_SECRET_ENV_TEST_USER", "test-user")
    monkeypatch.setenv("DBT_ENV_SECRET_ENV_TEST_PASSWORD", "sensitive-test-password")


def test_starrocks_catalog_schema_mapping(tmp_path, secrets):
    body = snapshot()
    with materialized_profile(body, tmp_path, settings()) as directory:
        text = (directory / "profiles.yml").read_text(encoding="utf-8")
        output = yaml.safe_load(text)["execution"]["outputs"]["execution"]
        assert output["catalog"] == body.catalog
        assert output["schema"] == body.schema_name
        # dbt source 默认继承 target.database；填 schema 会被 StarRocks 当成外部 catalog。
        assert output["database"] is None
        assert "sensitive-test-password" not in text
        assert "test-user" not in text
    assert not directory.exists()


def test_profile_isolated_per_execution(tmp_path, secrets):
    with materialized_profile(snapshot(), tmp_path, settings()) as first:
        original = (first / "profiles.yml").read_bytes()
        with materialized_profile(snapshot(), tmp_path, settings()) as second:
            assert first != second
            assert (first / "profiles.yml").read_bytes() == original


def test_unknown_secret_ref_is_rejected(tmp_path, secrets):
    with pytest.raises(ValueError, match="CREDENTIAL_UNAVAILABLE"):
        with materialized_profile(snapshot(credentialRef="PATH"), tmp_path, settings()):
            pytest.fail("unknown secret was accepted")


def test_aliases_share_target_but_unknown_instance_is_rejected():
    from dbt_metricflow_service.models.execution import physical_target_key

    body = snapshot()
    alias = body.model_copy(update={"connection": body.connection.model_copy(update={"host": "127.0.0.1"})})
    validate_execution_target(body, settings())
    validate_execution_target(alias, settings())
    assert physical_target_key(body) == physical_target_key(alias)
    with pytest.raises(ValueError, match="EXECUTION_TARGET_UNVERIFIED"):
        validate_execution_target(body.model_copy(update={
            "connection": body.connection.model_copy(update={"host": "unexpected.test"})}), settings())


def test_manifest_rejects_other_catalog(tmp_path):
    body = snapshot()
    manifest = {"nodes": {"model.test": {"resource_type": "model", "config": {"materialized": "table"},
        "database": body.schema_name, "schema": body.schema_name, "alias": "orders",
        "relation_name": f"`other_catalog`.`{body.schema_name}`.`orders`"}}}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError):
        validate_execution_manifest(tmp_path, body)
    manifest["nodes"]["model.test"]["relation_name"] = f"`{body.catalog}`.`{body.schema_name}`.`orders`"
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    validate_execution_manifest(tmp_path, body)


def test_starrocks_manifest_uses_adapter_catalog_semantics(tmp_path):
    body = snapshot()
    node = {"resource_type": "model", "config": {"materialized": "table"},
            "database": None, "schema": body.schema_name, "alias": "orders",
            "relation_name": f"`{body.schema_name}`.`orders`"}
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"nodes": {"model.test": node}}), encoding="utf-8")
    validate_execution_manifest(tmp_path, body)
    node["config"]["catalog"] = "other_catalog"
    path.write_text(json.dumps({"nodes": {"model.test": node}}), encoding="utf-8")
    with pytest.raises(ValueError, match="catalog"):
        validate_execution_manifest(tmp_path, body)
