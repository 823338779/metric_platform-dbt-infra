from __future__ import annotations

from pathlib import Path
from typing import Final

from dbt_metricflow_service import constants as strings

from ..models.artifacts import CatalogRelation, ResourceKind
from ..models.catalog import ArtifactSchemas, NativeCatalog, NativeResource
from ..models.native import NativeManifest, NativeNode, NativePhysicalCatalog, SemanticManifest

ARTIFACT_VERSION_INCOMPATIBLE_MESSAGE: Final[str] = 'dbt 产物版本不兼容'
PHYSICAL_DETAILS_MISSING_MESSAGE: Final[str] = 'dbt 物理关系或列信息缺失'
SOURCE_PHYSICAL_DETAILS_MISSING_MESSAGE: Final[str] = 'dbt source 物理关系或列信息缺失'
DIMENSION_NAME_MISSING_MESSAGE: Final[str] = 'dbt 维度定义无名称'

MANIFEST_SCHEMA = "https://schemas.getdbt.com/dbt/manifest/v12.json"
CATALOG_SCHEMA = "https://schemas.getdbt.com/dbt/catalog/v1.json"
SEMANTIC_SCHEMA = "dbt-metricflow/0.15.0"


def catalog_from_artifacts(target: Path, *, query_capability: bool = True) -> NativeCatalog:
    """只按已知 dbt 产物版本派生发布目录；原生定义完整保留。"""
    manifest = NativeManifest.model_validate_json((target / strings.MANIFEST_FILE).read_bytes())
    SemanticManifest.model_validate_json((target / strings.SEMANTIC_MANIFEST_FILE).read_bytes())
    catalog = NativePhysicalCatalog.model_validate_json((target / strings.CATALOG_FILE).read_bytes())
    if (manifest.metadata.dbt_schema_version != MANIFEST_SCHEMA
            or catalog.metadata.dbt_schema_version != CATALOG_SCHEMA):
        raise ValueError(ARTIFACT_VERSION_INCOMPATIBLE_MESSAGE)
    resources: list[NativeResource] = []
    dependencies: list[CatalogRelation] = []

    def add_resource(resource: NativeResource) -> None:
        resources.append(resource)
        for upstream in resource.definition.depends_on.nodes:
            dependencies.append(CatalogRelation(
                upstream_resource_id=upstream, downstream_resource_id=resource.native_id, kind=strings.DBT_DEPENDS_ON
            ))

    def resource(native_id: str, kind: ResourceKind, definition: NativeNode) -> NativeResource:
        return NativeResource(
            resource_id=native_id, native_id=native_id, kind=kind,
            name=definition.name or native_id, description=definition.description,
            source_path=definition.original_file_path.replace("\\", "/") if definition.original_file_path else None,
            definition=definition, queryable=query_capability,
        )

    for native_id, definition in manifest.nodes.items():
        if definition.resource_type != strings.DBT_MODEL_RESOURCE or definition.config.materialized not in {
            strings.DBT_TABLE_MATERIALIZATION,
            strings.DBT_VIEW_MATERIALIZATION,
        }:
            continue
        details = catalog.nodes.get(native_id)
        if not definition.relation_name or details is None:
            raise ValueError(PHYSICAL_DETAILS_MISSING_MESSAGE)
        item = resource(native_id, ResourceKind(definition.config.materialized.upper()), definition)
        item.relation, item.columns = definition.relation_name, details.columns
        add_resource(item)

    for native_id, definition in manifest.sources.items():
        details = catalog.sources.get(native_id)
        if not definition.relation_name or details is None:
            raise ValueError(SOURCE_PHYSICAL_DETAILS_MISSING_MESSAGE)
        item = resource(native_id, ResourceKind.TABLE, definition)
        item.relation, item.columns = definition.relation_name, details.columns
        add_resource(item)

    for native_id, definition in manifest.semantic_models.items():
        add_resource(resource(native_id, ResourceKind.SEMANTIC_MODEL, definition))
        for dimension in definition.dimensions:
            if not dimension.name:
                raise ValueError(DIMENSION_NAME_MISSING_MESSAGE)
            dimension_id = f"{native_id}.dimension.{dimension.name}"
            dimension = dimension.model_copy(deep=True)
            dimension.original_file_path = definition.original_file_path
            item = resource(dimension_id, ResourceKind.DIMENSION, dimension)
            item.semantic_model_id = native_id
            add_resource(item)
            dependencies.append(CatalogRelation(
                upstream_resource_id=native_id, downstream_resource_id=dimension_id, kind=strings.SEMANTIC_DIMENSION
            ))
    for native_id, definition in manifest.metrics.items():
        add_resource(resource(native_id, ResourceKind.METRIC, definition))
    return NativeCatalog(resources=resources, dependencies=dependencies, artifact_schemas=ArtifactSchemas(
        manifest=MANIFEST_SCHEMA, semantic_manifest=SEMANTIC_SCHEMA, catalog=CATALOG_SCHEMA,
    ))
