from __future__ import annotations

from pathlib import Path

import pytest

from dbt_metricflow_service.platform.namespace import (
    prepare_fixed_schema_project,
    validate_fixed_schema_manifest,
)


def test_project_copy_preserves_model_names(tmp_path: Path) -> None:
    (tmp_path / "dbt_project.yml").write_text("name: shop\n", encoding="utf-8")

    prepare_fixed_schema_project(tmp_path, "dbt_ecom")

    assert not (tmp_path / "macros" / "generate_alias_name.sql").exists()


def test_project_rejects_existing_alias_macro(tmp_path: Path) -> None:
    macros = tmp_path / "macros"
    macros.mkdir()
    (macros / "custom.sql").write_text("{% macro generate_alias_name(x, y) %}bad{% endmacro %}", encoding="utf-8")

    with pytest.raises(ValueError):
        prepare_fixed_schema_project(tmp_path, "dbt_ecom")
    assert not (macros / "generate_alias_name.sql").exists()


def test_parse_accepts_original_names_only_in_fixed_schema(tmp_path: Path) -> None:
    import json

    target = tmp_path / "target"
    target.mkdir()
    manifest = {"nodes": {
        "model.shop.orders": {"resource_type": "model", "schema": "dbt_ecom",
                              "alias": "orders", "relation_name": "`dbt_ecom`.`orders`",
                              "config": {"materialized": "table"}}
    }}
    (target / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    validate_fixed_schema_manifest(target, "dbt_ecom")

    manifest["nodes"]["model.shop.orders"]["relation_name"] = (
        "`ecommerce_raw`.`orders`"
    )
    (target / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError):
        validate_fixed_schema_manifest(target, "dbt_ecom")

    manifest["nodes"]["model.shop.orders"].update(
        alias="OrderItems",
        relation_name="`dbt_ecom`.`OrderItems`",
    )
    (target / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    validate_fixed_schema_manifest(target, "dbt_ecom")


@pytest.mark.parametrize("hook", ["pre-hook", "post-hook", "pre_hook", "post_hook"])
def test_parse_rejects_model_hooks(tmp_path: Path, hook: str) -> None:
    import json

    manifest = {"nodes": {"model.shop.orders": {
        "resource_type": "model", "schema": "dbt_ecom",
        "alias": "orders",
        "relation_name": "`dbt_ecom`.`orders`",
        "config": {"materialized": "table", hook: ["drop table old_orders"]},
    }}}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError):
        validate_fixed_schema_manifest(tmp_path, "dbt_ecom")


def test_parse_rejects_project_run_hook(tmp_path: Path) -> None:
    import json

    manifest = {"nodes": {"operation.shop.start": {"resource_type": "operation"}}}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError):
        validate_fixed_schema_manifest(tmp_path, "dbt_ecom")
