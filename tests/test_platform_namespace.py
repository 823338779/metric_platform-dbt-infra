from __future__ import annotations

from pathlib import Path
from uuid import UUID

import pytest

from dbt_metricflow_service.platform.namespace import (
    prepare_versioned_project,
    run_prefix,
    validate_versioned_manifest,
)

RUN_ID = UUID("12345678-1234-5678-9abc-def012345678")


def test_run_prefix_is_deterministic() -> None:
    assert run_prefix(RUN_ID) == "rv_12345678123456789abcdef012345678_"


def test_project_copy_gets_run_specific_alias_macro(tmp_path: Path) -> None:
    (tmp_path / "dbt_project.yml").write_text("name: shop\n", encoding="utf-8")

    prefix = prepare_versioned_project(tmp_path, RUN_ID, "dbt_ecom")

    macro = (tmp_path / "macros" / "generate_alias_name.sql").read_text(encoding="utf-8")
    assert prefix in macro
    assert "generate_alias_name" in macro
    assert "custom_alias_name" in macro


def test_project_rejects_existing_alias_macro(tmp_path: Path) -> None:
    macros = tmp_path / "macros"
    macros.mkdir()
    (macros / "custom.sql").write_text("{% macro generate_alias_name(x, y) %}bad{% endmacro %}", encoding="utf-8")

    with pytest.raises(ValueError):
        prepare_versioned_project(tmp_path, RUN_ID, "dbt_ecom")
    assert not (macros / "generate_alias_name.sql").exists()


def test_parse_rejects_unscoped_relation_before_build(tmp_path: Path) -> None:
    import json

    target = tmp_path / "target"
    target.mkdir()
    manifest = {"nodes": {
        "model.shop.orders": {"resource_type": "model", "schema": "dbt_ecom",
                              "alias": "orders", "relation_name": "`dbt_ecom`.`orders`",
                              "config": {"materialized": "table"}}
    }}
    (target / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError):
        validate_versioned_manifest(target, "dbt_ecom", run_prefix(RUN_ID))

    manifest["nodes"]["model.shop.orders"].update(
        alias="rv_12345678123456789abcdef012345678_orders",
        relation_name="`dbt_ecom`.`rv_12345678123456789abcdef012345678_orders`",
    )
    (target / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    validate_versioned_manifest(target, "dbt_ecom", run_prefix(RUN_ID))

    manifest["nodes"]["model.shop.orders"]["relation_name"] = (
        "`ecommerce_raw`.`rv_12345678123456789abcdef012345678_orders`"
    )
    (target / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError):
        validate_versioned_manifest(target, "dbt_ecom", run_prefix(RUN_ID))

    manifest["nodes"]["model.shop.orders"].update(
        alias="rv_12345678123456789abcdef012345678_OrderItems",
        relation_name="`dbt_ecom`.`rv_12345678123456789abcdef012345678_OrderItems`",
    )
    (target / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    validate_versioned_manifest(target, "dbt_ecom", run_prefix(RUN_ID))


@pytest.mark.parametrize("hook", ["pre-hook", "post-hook", "pre_hook", "post_hook"])
def test_parse_rejects_model_hooks(tmp_path: Path, hook: str) -> None:
    import json

    manifest = {"nodes": {"model.shop.orders": {
        "resource_type": "model", "schema": "dbt_ecom",
        "alias": "rv_12345678123456789abcdef012345678_orders",
        "relation_name": "`dbt_ecom`.`rv_12345678123456789abcdef012345678_orders`",
        "config": {"materialized": "table", hook: ["drop table old_orders"]},
    }}}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError):
        validate_versioned_manifest(tmp_path, "dbt_ecom", run_prefix(RUN_ID))


def test_parse_rejects_project_run_hook(tmp_path: Path) -> None:
    import json

    manifest = {"nodes": {"operation.shop.start": {"resource_type": "operation"}}}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError):
        validate_versioned_manifest(tmp_path, "dbt_ecom", run_prefix(RUN_ID))
