from __future__ import annotations

import json
from pathlib import Path
from typing import Any

MANIFEST_SCHEMA = "https://schemas.getdbt.com/dbt/manifest/v12.json"
CATALOG_SCHEMA = "https://schemas.getdbt.com/dbt/catalog/v1.json"
SEMANTIC_SCHEMA = "dbt-metricflow/0.15.0"


def _read(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("dbt 原生产物格式无效")
    return data


def catalog_from_artifacts(target: Path, *, query_capability: bool = True) -> dict[str, Any]:
    """只按已知 dbt 产物版本派生发布目录；原生定义完整保留。"""

    manifest = _read(target / "manifest.json")
    semantic = _read(target / "semantic_manifest.json")
    catalog = _read(target / "catalog.json")
    manifest_version = manifest.get("metadata", {}).get("dbt_schema_version")
    catalog_version = catalog.get("metadata", {}).get("dbt_schema_version")
    if manifest_version != MANIFEST_SCHEMA or catalog_version != CATALOG_SCHEMA:
        raise ValueError("dbt 产物版本不兼容")
    if not isinstance(semantic.get("semantic_models"), list) or not isinstance(semantic.get("metrics"), list):
        raise ValueError("MetricFlow 原生产物格式无效")
    resources: list[dict[str, Any]] = []
    dependencies: list[dict[str, str]] = []

    def add_resource(native_id: str, kind: str, definition: dict[str, Any], **extra: Any) -> None:
        resource = {
            "resourceId": native_id, "nativeId": native_id, "kind": kind,
            "name": definition.get("name", native_id), "description": definition.get("description"),
            "sourcePath": (
                definition["original_file_path"].replace("\\", "/")
                if isinstance(definition.get("original_file_path"), str) else None
            ), "definition": definition,
            "queryable": query_capability and kind in {"METRIC", "DIMENSION", "SEMANTIC_MODEL", "TABLE", "VIEW"},
            **extra,
        }
        resources.append(resource)
        for upstream in definition.get("depends_on", {}).get("nodes", []):
            dependencies.append({
                "upstreamResourceId": upstream, "downstreamResourceId": native_id, "kind": "DBT_DEPENDS_ON"
            })

    # 物理 table/view 由 manifest materialization 与 catalog 列共同核实。
    catalog_nodes = catalog.get("nodes", {})
    for native_id, definition in manifest.get("nodes", {}).items():
        if definition.get("resource_type") != "model":
            continue
        materialized = definition.get("config", {}).get("materialized")
        if materialized not in {"table", "view"}:
            continue
        relation = definition.get("relation_name")
        details = catalog_nodes.get(native_id)
        if not isinstance(relation, str) or not relation or not isinstance(details, dict):
            raise ValueError("dbt 物理关系或列信息缺失")
        add_resource(
            native_id, materialized.upper(), definition,
            relation=relation, columns=details.get("columns", {}),
        )

    # StarRocks 原始表属于 dbt source，使用原生产物中的关系与列供平台查看和预览。
    catalog_sources = catalog.get("sources", {})
    for native_id, definition in manifest.get("sources", {}).items():
        relation = definition.get("relation_name")
        details = catalog_sources.get(native_id)
        if not isinstance(relation, str) or not relation or not isinstance(details, dict):
            raise ValueError("dbt source 物理关系或列信息缺失")
        add_resource(native_id, "TABLE", definition, relation=relation, columns=details.get("columns", {}))

    # 语义模型和指标直接取 manifest 原生定义；维度保留所属语义模型路径。
    for native_id, definition in manifest.get("semantic_models", {}).items():
        add_resource(native_id, "SEMANTIC_MODEL", definition)
        for dimension in definition.get("dimensions", []):
            name = dimension.get("name")
            if not isinstance(name, str) or not name:
                raise ValueError("dbt 维度定义无名称")
            dimension_id = f"{native_id}.dimension.{name}"
            add_resource(dimension_id, "DIMENSION", {
                **dimension, "original_file_path": definition.get("original_file_path"),
            }, semanticModelId=native_id)
            dependencies.append({
                "upstreamResourceId": native_id, "downstreamResourceId": dimension_id,
                "kind": "SEMANTIC_DIMENSION",
            })
    for native_id, definition in manifest.get("metrics", {}).items():
        add_resource(native_id, "METRIC", definition)

    return {
        "resources": resources, "dependencies": dependencies,
        "artifactSchemas": {
            "manifest": manifest_version, "semanticManifest": SEMANTIC_SCHEMA, "catalog": catalog_version,
        },
    }
