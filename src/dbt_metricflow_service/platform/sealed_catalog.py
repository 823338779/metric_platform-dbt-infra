"""发布前生成展示目录；未知关键语义必须失败，不能留给平台解释。"""
from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Final
from uuid import UUID

from dbt_metricflow_service import constants as strings

from ..models.artifacts import (
    PUBLISHED_CATALOG_FILE,
    BindingMode,
    CatalogRelation,
    DatasetAttributes,
    DatasetResource,
    DimensionAttributes,
    DimensionResource,
    MetricAttributes,
    MetricResource,
    MetricTimeConfig,
    PhysicalAttributes,
    PhysicalResource,
    PublishedCatalog,
    RelationBinding,
    RelationName,
    ResourceKind,
)
from ..models.catalog import NativeCatalog
from ..models.native import NativeConversion, NativeCumulative, NativeNode, NativeTypeParams, SemanticManifest
from ..models.payloads import JsonValue

SEMANTIC_COLLECTION_MISMATCH_MESSAGE: Final[str] = '原生产物语义集合不一致'
SEMANTIC_DEFINITION_MISMATCH_MESSAGE: Final[str] = '原生产物语义定义不一致'
CATALOG_IDENTITY_MISMATCH_MESSAGE: Final[str] = '目录身份重复或不一致'
DUPLICATE_PHYSICAL_BINDING_MESSAGE: Final[str] = '物理绑定重复'
EPHEMERAL_DEPENDENCY_CYCLE_MESSAGE: Final[str] = 'ephemeral 依赖存在环'
DANGLING_CATALOG_REFERENCE_MESSAGE: Final[str] = '目录存在悬空引用'
METRIC_REFERENCE_CYCLE_MESSAGE: Final[str] = '指标引用存在环'
METRIC_TYPE_UNSUPPORTED_MESSAGE: Final[str] = '不支持的指标类型'
METRIC_REFERENCE_MISSING_MESSAGE: Final[str] = '指标引用不存在'
METRIC_DATASET_MISSING_MESSAGE: Final[str] = '指标数据集不存在'
MEASURE_OWNERSHIP_INVALID_MESSAGE: Final[str] = '指标度量归属不存在或不唯一'
METRIC_DATASET_OWNERSHIP_MISSING_MESSAGE: Final[str] = '指标缺少数据集归属'
DIMENSION_DATASET_MISSING_MESSAGE: Final[str] = '维度缺少所属数据集'
SEMANTIC_PHYSICAL_SOURCE_INVALID_MESSAGE: Final[str] = '语义模型缺少唯一物理来源'
SEMANTIC_BINDING_MISMATCH_MESSAGE: Final[str] = '语义模型物理关系与发布绑定不一致'
VERIFIED_PHYSICAL_BINDING_MISSING_MESSAGE: Final[str] = '物理资源缺少经过验证的绑定'
CATALOG_DEPENDENCY_CYCLE_MESSAGE: Final[str] = '目录依赖存在环'

METRIC_TYPES = frozenset(
    (
        strings.METRIC_SIMPLE_TYPE,
        strings.METRIC_DERIVED_TYPE,
        strings.METRIC_RATIO_TYPE,
        strings.METRIC_CUMULATIVE_TYPE,
        strings.METRIC_CONVERSION_TYPE,
    )
)
DEPENDENCY = strings.DBT_DEPENDS_ON
MEMBERSHIP = strings.SEMANTIC_DIMENSION
METRIC_INPUT = strings.METRIC_INPUT
METRIC_DATASET = strings.METRIC_DATASET
MODEL_DATASET = strings.MODEL_DATASET
SEMANTIC_FILE = strings.SEMANTIC_MANIFEST_FILE
UTF8 = strings.UTF8


def _canonical(value: JsonValue) -> JsonValue:
    # 两种原生产物中的空默认值不影响语义比较；数组保持定义顺序。
    if isinstance(value, dict):
        return {key: _canonical(item) for key, item in value.items() if item not in (None, [], {})}
    if isinstance(value, list):
        return [_canonical(item) for item in value]
    return value


def _semantic_fields(node: NativeNode, kind: ResourceKind) -> list[JsonValue]:
    if kind == ResourceKind.SEMANTIC_MODEL:
        return [
            [item.native_details() for item in node.dimensions],
            [item.native_details() for item in node.entities],
            [item.native_details() for item in node.measures],
            node.defaults.native_details() if node.defaults else None,
        ]
    return [node.type, node.type_params.native_details() if node.type_params else None,
            node.filter, node.time_granularity]


def _validate_semantics(catalog: NativeCatalog, semantic: SemanticManifest) -> None:
    for kind, definitions in ((ResourceKind.SEMANTIC_MODEL, semantic.semantic_models),
                              (ResourceKind.METRIC, semantic.metrics)):
        indexed = {item.name: item for item in definitions}
        candidates = [item for item in catalog.resources if item.kind == kind]
        expected = {item.name: item.definition for item in candidates}
        if len(indexed) != len(definitions) or len(expected) != len(candidates) or indexed.keys() != expected.keys():
            raise ValueError(SEMANTIC_COLLECTION_MISMATCH_MESSAGE)
        for name, definition in expected.items():
            if any(_canonical(left) != _canonical(right) for left, right in
                   zip(_semantic_fields(definition, kind), _semantic_fields(indexed[name], kind), strict=True)):
                raise ValueError(SEMANTIC_DEFINITION_MISMATCH_MESSAGE)


def build_published_catalog(
    *, project_id: str, release_id: UUID, native_catalog: NativeCatalog,
    semantic_manifest: SemanticManifest, relation_bindings: list[RelationBinding],
) -> PublishedCatalog:
    resources = native_catalog.resources
    indexed = {item.resource_id: item for item in resources}
    if len(indexed) != len(resources) or any(item.native_id != key for key, item in indexed.items()):
        raise ValueError(CATALOG_IDENTITY_MISMATCH_MESSAGE)
    _validate_semantics(native_catalog, semantic_manifest)
    binding_by_id = {item.native_id: item for item in relation_bindings}
    if len(binding_by_id) != len(relation_bindings):
        raise ValueError(DUPLICATE_PHYSICAL_BINDING_MESSAGE)
    metric_ids = {item.name: key for key, item in indexed.items() if item.kind == ResourceKind.METRIC}
    dataset_ids = {item.name: key for key, item in indexed.items() if item.kind == ResourceKind.SEMANTIC_MODEL}
    relations: set[tuple[str, str, str]] = set()
    ephemeral = native_catalog.ephemeral_dependencies

    def edge(upstream: str, downstream: str, kind: str, visited: frozenset[str] = frozenset()) -> None:
        if upstream in ephemeral:
            if upstream in visited:
                raise ValueError(EPHEMERAL_DEPENDENCY_CYCLE_MESSAGE)
            for parent in ephemeral[upstream]:
                edge(parent, downstream, kind, visited | {upstream})
            return
        if upstream not in indexed or downstream not in indexed:
            raise ValueError(DANGLING_CATALOG_REFERENCE_MESSAGE)
        relations.add((upstream, downstream, kind))

    for item in native_catalog.dependencies:
        edge(item.upstream_resource_id, item.downstream_resource_id, item.kind)
    for key, item in indexed.items():
        for upstream in item.definition.depends_on.nodes:
            edge(upstream, key, DEPENDENCY)

    computed: dict[str, MetricAttributes] = {}
    visiting: set[str] = set()

    def metric_attributes(key: str) -> MetricAttributes:
        if key in visiting:
            raise ValueError(METRIC_REFERENCE_CYCLE_MESSAGE)
        if key in computed:
            return computed[key]
        visiting.add(key)
        definition = indexed[key].definition
        metric_type = definition.type
        if metric_type not in METRIC_TYPES:
            raise ValueError(METRIC_TYPE_UNSUPPORTED_MESSAGE)
        params = definition.type_params or NativeTypeParams()
        cumulative = params.cumulative_type_params or NativeCumulative()
        conversion = params.conversion_type_params or NativeConversion()
        inputs = list(params.metrics or [])
        inputs.extend(item for item in (params.numerator, params.denominator, cumulative.metric,
                                       conversion.base_metric, conversion.conversion_metric) if item)
        input_ids: list[str] = []
        datasets: set[str] = set()
        for entry in inputs:
            name = entry if isinstance(entry, str) else entry.name
            if name not in metric_ids:
                raise ValueError(METRIC_REFERENCE_MISSING_MESSAGE)
            parent = metric_ids[name]
            input_ids.append(parent)
            datasets.update(metric_attributes(parent).dataset_ids)
            edge(parent, key, METRIC_INPUT)
        aggregation = params.metric_aggregation_params
        if aggregation and aggregation.semantic_model:
            name = aggregation.semantic_model
            if name not in dataset_ids:
                raise ValueError(METRIC_DATASET_MISSING_MESSAGE)
            datasets.add(dataset_ids[name])
        measures = list(params.input_measures)
        measures.extend(
            item for item in (params.measure, conversion.base_measure, conversion.conversion_measure) if item
        )
        for measure in measures:
            name = measure if isinstance(measure, str) else measure.name
            owners = [dataset for dataset in dataset_ids.values()
                      if any(item.name == name for item in indexed[dataset].definition.measures)]
            if len(owners) != 1:
                raise ValueError(MEASURE_OWNERSHIP_INVALID_MESSAGE)
            datasets.update(owners)
        if not datasets:
            raise ValueError(METRIC_DATASET_OWNERSHIP_MISSING_MESSAGE)
        for dataset in datasets:
            edge(dataset, key, METRIC_DATASET)
        result = MetricAttributes(
            metric_type=metric_type, definition_summary=params.expr or f"{metric_type}: {definition.name}",
            input_metric_ids=sorted(set(input_ids)), dataset_ids=sorted(datasets), filters=definition.filter,
            time_config=MetricTimeConfig(time_granularity=definition.time_granularity, window=params.window,
                                        cumulative=cumulative, conversion=conversion),
        )
        visiting.remove(key)
        computed[key] = result
        return result

    output = []
    for key, item in sorted(indexed.items()):
        definition, kind = item.definition, item.kind
        if kind == ResourceKind.METRIC:
            resource_type = MetricResource
            attributes = metric_attributes(key)
            capabilities = [strings.QUERY] if item.queryable else []
        elif kind == ResourceKind.DIMENSION:
            dataset = item.semantic_model_id
            if dataset not in dataset_ids.values():
                raise ValueError(DIMENSION_DATASET_MISSING_MESSAGE)
            edge(dataset, key, MEMBERSHIP)
            resource_type = DimensionResource
            capabilities = [strings.DIMENSION_VALUES] if item.queryable else []
            attributes = DimensionAttributes(dataset_id=dataset, dimension_type=definition.type,
                    expression=definition.expr,
                    time_granularity=definition.type_params.time_granularity if definition.type_params else None)
        elif kind == ResourceKind.SEMANTIC_MODEL:
            models = [node for node in definition.depends_on.nodes if node in binding_by_id]
            if len(models) != 1:
                raise ValueError(SEMANTIC_PHYSICAL_SOURCE_INVALID_MESSAGE)
            semantic = next(node for node in semantic_manifest.semantic_models if node.name == item.name)
            relation = semantic.node_relation
            binding = binding_by_id[models[0]].relation
            if relation and (relation.database, relation.schema_name, relation.alias) != (
                binding.database, binding.schema_name, binding.identifier,
            ):
                raise ValueError(SEMANTIC_BINDING_MISMATCH_MESSAGE)
            edge(models[0], key, MODEL_DATASET)
            resource_type = DatasetResource
            capabilities = []
            attributes = DatasetAttributes(
                model_resource_id=models[0], dimension_ids=sorted(node for node, value in indexed.items()
                    if value.kind == ResourceKind.DIMENSION and value.semantic_model_id == key),
                entities=definition.entities, measures=definition.measures,
                default_time_dimension=definition.defaults.agg_time_dimension if definition.defaults else None)
        else:
            binding = binding_by_id.get(key)
            if binding is None:
                raise ValueError(VERIFIED_PHYSICAL_BINDING_MISSING_MESSAGE)
            resource_type = PhysicalResource
            capabilities = [strings.PREVIEW] if item.queryable else []
            attributes = PhysicalAttributes(origin=binding.mode, relation=binding.relation, columns=item.columns)
        output.append(
            resource_type(
                resource_id=key,
                native_id=item.native_id,
                kind=kind,
                name=item.name,
                display_name=definition.label or item.name,
                description=item.description
                if strings.DESCRIPTION_FIELD in item.model_fields_set
                else definition.description,
                source_path=item.source_path,
                native_details=definition.native_details(),
                capabilities=capabilities,
                attributes=attributes,
            )
        )

    incoming = {key: set() for key in indexed}
    for upstream, downstream, _ in relations:
        incoming[downstream].add(upstream)
    while incoming:
        ready = {key for key, parents in incoming.items() if not parents}
        if not ready:
            raise ValueError(CATALOG_DEPENDENCY_CYCLE_MESSAGE)
        incoming = {key: parents - ready for key, parents in incoming.items() if key not in ready}
    return PublishedCatalog(project_id=project_id, release_id=release_id, resources=output,
        relation_bindings=relation_bindings,
        relations=[CatalogRelation(upstream_resource_id=up, downstream_resource_id=down, kind=kind)
                   for up, down, kind in sorted(relations)])


def write_publication_catalog(
    target: Path, *, project_id: str, release_id: UUID, run_id: UUID, native_catalog: NativeCatalog,
) -> PublishedCatalog:
    """全构建的实际关系形成直接绑定，完整目录文件必须在 capture 之前写入。"""
    bindings = []
    for item in native_catalog.resources:
        if item.kind not in (ResourceKind.TABLE, ResourceKind.VIEW):
            continue
        definition = item.definition
        external = definition.resource_type == strings.DBT_SOURCE_RESOURCE
        bindings.append(RelationBinding(
            native_id=item.native_id,
            relation=RelationName(database=definition.database, schema=definition.schema_name,
                                  identifier=definition.identifier if external else definition.alias),
            creator_run_id=None if external else run_id, mode=BindingMode.EXTERNAL if external else BindingMode.BUILT,
            definition_digest=hashlib.sha256(
                json.dumps(definition.native_details(), sort_keys=True).encode(UTF8)
            ).hexdigest(),
            verified_at=datetime.now(UTC),
        ))
    catalog = build_published_catalog(project_id=project_id, release_id=release_id, native_catalog=native_catalog,
        semantic_manifest=SemanticManifest.model_validate_json((target / SEMANTIC_FILE).read_bytes()),
        relation_bindings=bindings)
    (target / PUBLISHED_CATALOG_FILE).write_text(catalog.model_dump_json(by_alias=True), encoding=UTF8)
    return catalog
